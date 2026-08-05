import os
import math
import shutil
import subprocess
import struct
from cuemsutils.helpers import new_uuid

from enum import Enum, auto
from peewee import *


from cuemsutils.tools.StringSanitizer import StringSanitizer
from cuemsutils.tools.CopyMoveVersioned import CopyMoveVersioned
from cuemsutils.tools.CTimecode import CTimecode
from cuemsutils.log import logged, Logger
from cuemsutils.helpers import new_datetime

from cuemseditor.CuemsDBModel import Project, Media, ProjectMedia
from cuemseditor.CuemsErrors import *


FFPROBE_TIMEOUT = 30  # seconds; local-disk probes finish in <1s, NFS/USB headroom


def probe_duration(file_path):
    """Probe a media file's duration with ffprobe and return a ``CTimecode``.

    Runs ``ffprobe -show_entries format=duration`` (plain float seconds, NOT
    ``-sexagesimal``) and constructs ``CTimecode(start_seconds=...)`` which
    canonicalises correctly at every framerate including the whole-second carry
    boundary. This replaces the previous sexagesimal-string reformatting hack,
    which silently corrupted any duration whose millisecond fraction ended in a
    zero (stored short by up to ~0.9 s) and mangled the carry case into a
    minutes-long overshoot.

    Args:
        file_path: Absolute path to the media file.

    Returns:
        ``CTimecode`` at the default ms framerate (``str()`` → ``HH:MM:SS.mmm``).

    Raises:
        NotTimeCodeError: on any failure — ffprobe timeout, non-zero exit,
            empty / ``N/A`` / non-numeric / non-finite / negative output.
    """
    cmd = ['ffprobe', '-v', 'error',
           '-show_entries', 'format=duration',
           '-of', 'default=noprint_wrappers=1:nokey=1', file_path]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=FFPROBE_TIMEOUT)
    except subprocess.TimeoutExpired as e:
        raise NotTimeCodeError(
            f'ffprobe timed out after {FFPROBE_TIMEOUT}s for {file_path}') from e
    if result.returncode != 0:
        stderr = result.stderr.decode('utf8', errors='replace').strip()
        raise NotTimeCodeError(
            f'ffprobe failed (rc={result.returncode}) for {file_path}: {stderr}')
    out = result.stdout.decode('utf8').strip()
    if not out or out == 'N/A':
        raise NotTimeCodeError(f'ffprobe returned no duration ({out!r}) for {file_path}')
    try:
        seconds = float(out)
    except ValueError as e:
        raise NotTimeCodeError(f'ffprobe output not a number: {out!r}') from e
    # float() accepts 'nan'/'inf' silently; those blow up inside CTimecode
    # (ValueError / OverflowError) rather than raising our contract exception.
    if not math.isfinite(seconds):
        raise NotTimeCodeError(f'ffprobe returned non-finite duration {out!r} for {file_path}')
    if seconds < 0:
        raise NotTimeCodeError(f'ffprobe returned negative duration {seconds} for {file_path}')
    return CTimecode(start_seconds=seconds)


class MediaType(Enum):
    """Media file category.

    Used as the ``media_type`` column value in the ``Media`` ORM model (stored
    as ``MOVIE``, ``AUDIO``, or ``IMAGE`` string).
    """

    MOVIE = auto()
    AUDIO = auto()
    IMAGE = auto()


class CuemsDBMedia(StringSanitizer):
    """Media file CRUD, thumbnail / waveform generation, and video pre-indexing.

    On upload (via :meth:`new`) the following sidecar files are created:

    * **Movie** — JPEG thumbnail (ffmpeg) + ``.idx`` video index (cuems-videoindexer).
    * **Audio** — PNG waveform image (audiowaveform) + JSON waveform data.
    * **Image** — JPEG thumbnail (ffmpeg).

    All mutating operations are wrapped in Peewee atomic transactions with
    rollback; filesystem changes are reversed on failure where possible.

    User-supplied name strings are sanitised via the inherited
    ``StringSanitizer`` methods.

    Example:
        >>> db_media = CuemsDBMedia(settings_dict, database)
        >>> dest_name = db_media.new("/tmp/cuems/upload.tmp123456", "my-audio.wav")
        >>> media_list = db_media.list()
        >>> meta = db_media.load_meta(uuid_str)
        >>> db_media.delete(uuid_str)       # soft-delete → trash
        >>> db_media.restore(uuid_str)      # restore from trash
        >>> db_media.delete_from_trash(uuid_str)  # permanent delete
    """

    def __init__(self, settings_dict, db_connection):
        """Initialise paths from *settings_dict*.

        Args:
            settings_dict: Mapping consumed from ``settings.xml``.  Required
                keys: ``library_path``, ``tmp_path``, ``media_folder_name``,
                ``trash_folder_name``, ``thumbnail_folder_name``,
                ``waveform_folder_name``, ``thumbnail_extension``,
                ``waveform_extension``, ``thumbnail_size``.
            db_connection: The shared Peewee ``SqliteDatabase`` instance owned
                by ``CuemsDBManager``.
        """
        self.db = db_connection
        try:
            self.library_path = settings_dict['library_path']
            self.tmp_path = settings_dict['tmp_path']
            self.media_path = os.path.join(self.library_path, settings_dict['media_folder_name'])
            self.trash_path = os.path.join(self.library_path, settings_dict['trash_folder_name'], settings_dict['media_folder_name'])
            self.thumbnail_path = os.path.join(self.media_path, settings_dict['thumbnail_folder_name'])
            self.waveform_path = os.path.join(self.media_path, settings_dict['waveform_folder_name'])
            self.thumbnail_trash_path = os.path.join(self.trash_path, settings_dict['thumbnail_folder_name'])
            self.waveform_trash_path = os.path.join(self.trash_path, settings_dict['waveform_folder_name'])
            self.thumbnail_extension = settings_dict['thumbnail_extension']
            self.waveform_extension = settings_dict['waveform_extension']
            self.thumbnail_size = settings_dict['thumbnail_size']
        except KeyError as e:
            Logger.error(f'can not read settings {e}')

    def new(self, tmp_file_path, filename):
        """Intake a newly uploaded file: move it into the media library and create sidecars.

        Steps performed atomically:

        1. ``CopyMoveVersioned.move`` from ``tmp_file_path`` to
           ``<media_path>/<filename>`` (versioned if *filename* exists).
        2. Detect ``MediaType`` from the file extension.
        3. Extract duration via ``ffprobe`` (audio/video).
        4. Generate thumbnail (ffmpeg), waveform (audiowaveform for audio), and
           video index (cuems-videoindexer for movies).
        5. Insert the ``Media`` DB row.

        On any failure the transaction is rolled back and any partially created
        sidecar files are removed.

        Args:
            tmp_file_path: Absolute path to the file in the tmp upload directory.
            filename: Sanitised target filename (no directory component).

        Returns:
            The actual destination filename (may differ from *filename* if
            ``CopyMoveVersioned`` added a suffix to avoid collision).

        Raises:
            Exception: Re-raises after rollback and cleanup on any failure.
        """
        with self.db.atomic() as transaction:
            trash_state = False
            try:
                dest_filename = None
                dest_filename = CopyMoveVersioned.move(tmp_file_path, self.media_path, filename)

                try:
                    _type = self.get_type(dest_filename)
                except Exception as e:
                    Logger.warning(f'could not get media type; error : {e}')
                    _type = None
                    raise e

                try:
                    if _type in (MediaType.MOVIE, MediaType.AUDIO):
                        media_duration = self.get_duration(dest_filename)
                    else:
                        media_duration = None
                except Exception as e:
                    Logger.warning(f'could not get media duration; error : {e}')
                    media_duration = None

                try:
                    if _type is MediaType.MOVIE:
                        Logger.debug(f'creating thumbnail for movie: {dest_filename}')
                        dest_thumbnail_filename = None
                        dest_thumbnail_filename = self.create_video_thumbnail(dest_filename, media_duration)
                        self.create_video_index(dest_filename)
                    elif _type is MediaType.AUDIO:
                        Logger.debug(f'creating thumbnail for audio: {dest_filename}')
                        dest_thumbnail_filename = None
                        dest_waveform_filename = None
                        if media_duration is not None:
                            dest_thumbnail_filename = self.create_audio_thubnail(dest_filename, media_duration)
                        else:
                            Logger.warning(f'no duration for {dest_filename}; skipping audio waveform thumbnail')
                        dest_waveform_filename = self.create_audio_waveform(dest_filename)
                    elif _type is MediaType.IMAGE:
                        Logger.debug(f'creating thumbnail for image: {dest_filename}')
                        dest_thumbnail_filename = None
                        dest_thumbnail_filename = self.create_video_thumbnail(dest_filename, None)
                except Exception as e:
                    Logger.error(f'could not generate {_type} thumbnail or waveform; error : {e}')
                    media_thumbnail_binary_data = None

                media_uuid = new_uuid()
                Media.create(uuid=str(media_uuid), name=dest_filename, unix_name=dest_filename, created=new_datetime(), modified=new_datetime(), duration=media_duration, media_type=_type.name, in_trash=False)
                Logger.debug(f'new media created: {media_uuid} {dest_filename}')
                return dest_filename
            except Exception as e:
                Logger.error("error: {} {} trying to move new file, rolling back database insert".format(type(e), e))
                transaction.rollback()
                if dest_filename is None and dest_thumbnail_filename is None:  # if move or copy where not successful we don't need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                    if _type is MediaType.AUDIO:
                        if dest_waveform_filename is None:
                            raise e
                    else:
                        raise e
                if os.path.exists(self.get_file_path(dest_filename)):
                    os.remove(self.get_file_path(dest_filename))

                if os.path.exists(self.get_thumbnail_path(dest_filename)):
                    os.remove(self.get_thumbnail_path(dest_filename))

                if os.path.exists(self.get_waveform_path(dest_filename)):
                    os.remove(self.get_waveform_path(dest_filename))

                raise e

    def list(self):
        """Return all live (non-trashed) media files with project usage counts.

        Returns:
            List of single-key dicts ``{uuid_str: {name, unix_name, created,
            modified, duration, type, in_projects, in_projects_list,
            in_trash_projects}}``, ordered by ``created`` descending.
            ``duration`` is the ``CTimecode`` string (or ``None``); the frontend
            copies it into cue ``Media.duration`` on save.  ``in_projects_list``
            is a list of ``{uuid, name, in_trash}`` dicts for each referencing
            project.
        """
        media_list = list()

        medias = (Media
                  .select(Media,
                          fn.COUNT(Case(Project.in_trash, (('0', 1),), None)).alias('in_project_count'),
                          fn.COUNT(Case(Project.in_trash, (('1', 1),), None)).alias('in_project_trash_count'))
                  .join(ProjectMedia, JOIN.LEFT_OUTER)
                  .join(Project, JOIN.LEFT_OUTER, on=(Project.uuid == ProjectMedia.project))
                  .where((Media.in_trash == False))
                  .group_by(Media).order_by(Media.created.desc()))

        medias = prefetch(
            medias,
            ProjectMedia,
            Project)

        for media in medias:
            project_list = []  # Reset list for each media item
            for project in media.projects():
                project_dict = {'uuid': str(project.uuid), 'name': project.name, 'in_trash': project.in_trash}
                project_list.append(project_dict)

            media_dict = {str(media.uuid): {'name': media.name, 'unix_name': media.unix_name, 'created': media.created, 'modified': media.modified, 'duration': media.duration, 'type': media.media_type, "in_projects": media.in_project_count, "in_projects_list": project_list, "in_trash_projects": media.in_project_trash_count}}
            media_list.append(media_dict)

        return media_list

    def list_trash(self):
        """Return all trashed media files with project usage counts.

        Returns:
            Same shape as :meth:`list` but for media where
            ``in_trash == True``.  ``in_projects_list`` is not included.
        """
        media_list = list()

        medias = (Media
                  .select(Media.uuid, Media.name, Media.unix_name, Media.created, Media.modified, Media.duration, Media.media_type,
                          fn.COUNT(Case(Project.in_trash, (('0', 1),), None)).alias('in_project_count'),
                          fn.COUNT(Case(Project.in_trash, (('1', 1),), None)).alias('in_project_trash_count'))
                  .join(ProjectMedia, JOIN.LEFT_OUTER)
                  .join(Project, JOIN.LEFT_OUTER, on=(Project.uuid == ProjectMedia.project))
                  .where(Media.in_trash == True)
                  .group_by(Media.uuid).order_by(Media.created.desc()))
        for media in medias:
            media_dict = {str(media.uuid): {'name': media.name, 'unix_name': media.unix_name, 'created': media.created, 'modified': media.modified, 'duration': media.duration, 'type': media.media_type, "in_projects": media.in_project_count, "in_trash_projects": media.in_project_trash_count}}
            media_list.append(media_dict)

        return media_list

    def save(self, uuid, data):
        """Update the ``name`` and ``description`` fields of a live media record.

        Args:
            uuid: Media UUID string.
            data: Dict with structure ``{uuid: {name, description}}``.

        Returns:
            ``'updated'`` on success.

        Raises:
            NonExistentItemError: If the media record does not exist or is in
                the trash.
        """
        try:
            media = Media.get((Media.uuid == uuid) & (Media.in_trash == False))
            with self.db.atomic() as transaction:
                try:
                    media.update(name=StringSanitizer.sanitize_name(data['uuid']['name']), description=StringSanitizer.sanitize_text_size(data['uuid']['description']), modified=new_datetime()).execute()
                    return 'updated'
                except Exception as e:
                    Logger.error("error: {} {} trying to update  media data, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def load_meta(self, uuid):
        """Return full metadata for a media file, including project usage.

        Args:
            uuid: Media UUID string (live or trashed).

        Returns:
            Single-key dict ``{uuid: {name, unix_name, description, created,
            modified, duration, type, in_trash, in_projects, in_trash_projects}}``.
            ``in_projects`` is a list of live project UUID strings;
            ``in_trash_projects`` is a list of trashed project UUID strings.

        Raises:
            NonExistentItemError: If no media record with *uuid* exists.
        """
        try:
            media = Media.get(Media.uuid == uuid)
            file_meta = dict()
            project_list = list()
            project_trash_list = list()
            media_projects_query = media.projects()
            for project in media_projects_query:
                if project.in_trash == False:
                    project_list.append(str(project.uuid))
                else:
                    project_trash_list.append(str(project.uuid))

            file_meta[uuid] = {'name': media.name, 'unix_name': media.unix_name, 'description': media.description, 'created': media.created, 'modified': media.modified, 'duration': media.duration, 'type': media.media_type, 'in_trash': media.in_trash, 'in_projects': project_list, 'in_trash_projects': project_trash_list}
            return file_meta

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def load_thumbnail(self, uuid):
        """Read and return the thumbnail file as a length-prefixed binary blob.

        The returned bytes start with a 36-byte UUID header (see
        :meth:`add_binary_header`) so the frontend can correlate the binary
        WebSocket frame to the originating request.

        Args:
            uuid: Media UUID string.

        Returns:
            ``bytes`` — 36-byte UUID header + raw thumbnail file contents.

        Raises:
            NonExistentItemError: If the media record or its thumbnail file
                cannot be found.
        """
        try:
            media_filename = Media.get(Media.uuid == uuid).unix_name
            thumbnail_file_path = self.get_thumbnail_path(media_filename)
            try:
                with open(thumbnail_file_path, 'rb') as file:
                    media_thumbnail_binary_data = file.read()
                return self.add_binary_header(media_thumbnail_binary_data, uuid, 1)

            except Exception as e:
                raise NonExistentItemError("item with uuid: {} error reading thumbnail ; {}, {}".format(uuid, type(e), e))

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def load_waveform(self, uuid):
        """Read and return the waveform data file as a length-prefixed binary blob.

        The returned bytes start with a 36-byte UUID header (see
        :meth:`add_binary_header`) so the frontend can correlate the binary
        WebSocket frame to the originating request.

        Args:
            uuid: Media UUID string.

        Returns:
            ``bytes`` — 36-byte UUID header + raw waveform file contents.

        Raises:
            NonExistentItemError: If the media record or its waveform file
                cannot be found.
        """
        try:
            media_filename = Media.get(Media.uuid == uuid).unix_name
            waveform_file_path = self.get_waveform_path(media_filename)
            try:
                with open(waveform_file_path, 'rb') as file:
                    media_waveform_binary_data = file.read()
                return self.add_binary_header(media_waveform_binary_data, uuid, 2)

            except Exception as e:
                raise NonExistentItemError("item with uuid: {} error reading  waveform ; {}, {}".format(uuid, type(e), e))

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def delete(self, uuid):
        """Soft-delete a media file by moving it and its sidecars to the trash.

        Moves the media file, thumbnail, and (for audio) waveform to the
        corresponding trash sub-directories via ``CopyMoveVersioned.move``.
        Sets ``Media.in_trash = True`` atomically.  Reverses filesystem moves
        on transaction rollback.

        Args:
            uuid: UUID of the live media file to trash.

        Raises:
            NonExistentItemError: If no live media file with *uuid* exists.
        """
        try:
            trash_state = False
            media = Media.get((Media.uuid == uuid) & (Media.in_trash == trash_state))

            with self.db.atomic() as transaction:
                try:
                    dest_filename = None
                    dest_thumbnail_filename = None
                    file_path = self.get_file_path(media.unix_name)
                    file_thumbnail_path = self.get_thumbnail_path(media.unix_name)

                    try:
                        if os.path.exists(file_thumbnail_path):
                            dest_thumbnail_filename = CopyMoveVersioned.move(file_thumbnail_path, self.thumbnail_trash_path)
                    except Exception as e:
                        Logger.error("error: {} {}; trying to move thumbnail to trash".format(type(e), e))
                        raise e

                    if self.is_audio(media):
                        dest_waveform_filename = None
                        file_waveform_path = self.get_waveform_path(media.unix_name)
                        try:
                            if os.path.exists(file_waveform_path):
                                dest_waveform_filename = CopyMoveVersioned.move(file_waveform_path, self.waveform_trash_path)
                        except Exception as e:
                            Logger.error("error: {} {}; trying to move waveform to trash".format(type(e), e))
                            raise e

                    dest_filename = CopyMoveVersioned.move(file_path, self.trash_path)
                    media.in_trash = True
                    media.save()
                    Logger.debug('updating instance in db: {}'.format(media))
                except Exception as e:
                    Logger.error("error: {} {}; trying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None and dest_thumbnail_filename is None:
                        if self.is_audio(media):
                            if dest_waveform_filename is None:
                                raise e
                        else:
                            raise e

                    Logger.debug("moving files back to media folder")
                    if os.path.exists(self.get_file_path(dest_filename, trash_state=True)):
                        shutil.move(self.get_file_path(dest_filename, trash_state=True), self.get_file_path(media.unix_name))

                    if os.path.exists(self.get_thumbnail_path(dest_thumbnail_filename, trash_state=True)):
                        shutil.move(self.get_thumbnail_path(dest_thumbnail_filename, trash_state=True), self.get_thumbnail_path(media.unix_name))

                    if self.is_audio(media):
                        if os.path.exists(self.get_waveform_path(dest_waveform_filename, trash_state=True)):
                            shutil.move(self.get_waveform_path(dest_waveform_filename, trash_state=True), self.get_waveform_path(media.unix_name))

                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def restore(self, uuid):
        """Restore a trashed media file and its sidecars back to the media library.

        Moves files from the trash sub-directories back to the live media
        directories via ``CopyMoveVersioned.move``.  Sets
        ``Media.in_trash = False`` atomically.  Reverses filesystem moves
        on transaction rollback.

        Args:
            uuid: UUID of a trashed media file to restore.

        Raises:
            NonExistentItemError: If no trashed media file with *uuid* exists.
        """
        try:
            trash_state = True
            media_trash = Media.get((Media.uuid == uuid) & (Media.in_trash == trash_state))

            with self.db.atomic() as transaction:
                try:
                    dest_filename = None
                    dest_thumbnail_filename = None
                    file_path = self.get_file_path(media_trash.unix_name, trash_state=True)
                    file_thumbnail_path = self.get_thumbnail_path(media_trash.unix_name, trash_state=True)

                    try:
                        if os.path.exists(file_thumbnail_path):
                            dest_thumbnail_filename = CopyMoveVersioned.move(file_thumbnail_path, self.thumbnail_path)
                    except Exception as e:
                        Logger.error("error: {} {}; trying to move thumbnail from trash".format(type(e), e))
                        raise e

                    if self.is_audio(media_trash):
                        dest_waveform_filename = None
                        file_waveform_path = self.get_waveform_path(media_trash.unix_name, trash_state=True)
                        try:
                            if os.path.exists(file_waveform_path):
                                dest_waveform_filename = CopyMoveVersioned.move(file_waveform_path, self.waveform_path)
                        except Exception as e:
                            Logger.error("error: {} {}; trying to waveform from trash".format(type(e), e))
                            raise e

                    dest_filename = CopyMoveVersioned.move(file_path, self.media_path)
                    media_trash.in_trash = False
                    media_trash.save()
                    Logger.debug('updating instance in db: {}'.format(media_trash))
                except Exception as e:
                    Logger.error("error: {} {}; trying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None and dest_thumbnail_filename is None:  # if move or copy where not successful we don't need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        if self.is_audio(media_trash):
                            if dest_waveform_filename is None:
                                raise e
                        else:
                            raise e

                    if os.path.exists(self.get_file_path(dest_filename)):
                        shutil.move(self.get_file_path(dest_filename), self.get_file_path(media_trash.unix_name, trash_state=True))

                    if os.path.exists(self.get_thumbnail_path(dest_thumbnail_filename)):
                        shutil.move(self.get_thumbnail_path(dest_thumbnail_filename), self.get_thumbnail_path(media_trash.unix_name, trash_state=True))

                    if self.is_audio(media_trash):
                        if os.path.exists(self.get_waveform_path(dest_waveform_filename)):
                            shutil.move(self.get_waveform_path(dest_waveform_filename), self.get_waveform_path(media_trash.unix_name, trash_state=True))

                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def delete_from_trash(self, uuid):
        """Permanently delete a trashed media file and its sidecars.

        Removes the ``Media`` DB row (and its ``ProjectMedia`` join rows via
        cascade) and deletes the media file, thumbnail, and waveform from the
        trash directories.

        Args:
            uuid: UUID of a trashed media file.

        Raises:
            NonExistentItemError: If no trashed media file with *uuid* exists.
        """
        try:
            trash_state = True
            media = Media.get((Media.uuid == uuid) & (Media.in_trash == trash_state))

            with self.db.atomic() as transaction:
                try:
                    file_path = self.get_file_path(media.unix_name, trash_state=True)
                    file_thumbnail_path = self.get_thumbnail_path(media.unix_name, trash_state=True)

                    if os.path.exists(file_thumbnail_path):
                        os.remove(file_thumbnail_path)

                    if self.is_audio(media):
                        file_waveform_path = self.get_waveform_path(media.unix_name, trash_state=True)
                        if os.path.exists(file_waveform_path):
                            os.remove(file_waveform_path)

                    media.delete_instance(recursive=True)
                    os.remove(file_path)
                    Logger.debug('modifing instance in table: {}'.format(media))
                except Exception as e:
                    Logger.error("error: {} {}; trying to delete file from trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def get_type(self, filename):
        """Detect the ``MediaType`` of *filename* from its extension.

        Extension matching is case-insensitive.

        Args:
            filename: Filename string (no directory component needed).

        Returns:
            ``MediaType.MOVIE``, ``MediaType.AUDIO``, or ``MediaType.IMAGE``;
            ``None`` if the extension is not in any known list.
        """
        movie_list = ('.mov', '.avi', '.mkv', '.mpg', '.mp4', '.webm', '.m4v', '.flv', '.wmv', '.ogv', '.3gp')
        audio_list = ('.aif', '.aiff', '.wav', '.mp3', '.m4a', '.ogg', '.oga', '.flac', '.aac', '.wma', '.opus', '.weba')
        image_list = ('.png', '.jpg', '.jpeg', '.tga', '.webp', '.gif', '.bmp', '.heic', '.heif', '.tiff', '.tif', '.ico')
        name_root, file_extension = os.path.splitext(filename)
        file_extension_lower = file_extension.lower() if file_extension else ''
        _type = None

        if file_extension_lower in movie_list:
            _type = MediaType.MOVIE
        elif file_extension_lower in audio_list:
            _type = MediaType.AUDIO
        elif file_extension_lower in image_list:
            _type = MediaType.IMAGE

        return _type

    def is_audio(self, db_record):
        """Return ``True`` if *db_record* has ``media_type == 'AUDIO'``.

        Args:
            db_record: A ``Media`` ORM instance.

        Returns:
            ``bool``
        """
        if db_record.media_type == MediaType.AUDIO.name:
            return True
        else:
            return False

    def get_duration(self, filename):
        """Extract a media file's duration and return a ``CTimecode``.

        Thin wrapper around the module-level :func:`probe_duration`, resolving
        *filename* against ``media_path``. Kept as a method so callers with a
        ``CuemsDBMedia`` instance need not know the media directory layout.

        Args:
            filename: Filename of the media file inside ``<media_path>``.

        Returns:
            ``CTimecode`` instance representing the media duration.

        Raises:
            NotTimeCodeError: If the duration cannot be probed (see
                :func:`probe_duration`).
        """
        return probe_duration(os.path.join(self.media_path, filename))

    @logged
    def create_video_thumbnail(self, filename, duration):
        """Generate a JPEG thumbnail for a video or image file using ``ffmpeg``.

        Seeks to 200 ms when *duration* is provided to avoid all-black frames
        at the start of some video files.  Writes to
        ``<thumbnail_path>/<stem>_<ext>.jpg``.

        Args:
            filename: Filename of the source file inside ``<media_path>``.
            duration: ``CTimecode`` duration or ``None`` (images have no
                duration; the first frame is used unconditionally).

        Returns:
            Absolute path to the created thumbnail file, or ``None`` if
            ``ffmpeg`` did not produce an output file.
        """
        # ffmpeg -y -hide_banner -loglevel warning -i input.mov -vf "scale=240:-1" -frames:v 1 -update true out.png
        file_path = self.get_file_path(filename)
        thumbnail_file_path = self.get_thumbnail_path(filename)
        try:
            if duration is None:
                result = subprocess.run(['ffmpeg', '-y', '-hide_banner', '-loglevel', 'warning', '-i', file_path, '-vf', f'scale={str(self.thumbnail_size[0])}:-1', '-frames:v', '1', '-update', 'true', thumbnail_file_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
            else:
                time_option = "-ss"
                timecode = f'200ms'
                result = subprocess.run(['ffmpeg', time_option, timecode, '-y', '-hide_banner', '-loglevel', 'warning', '-i', file_path, '-vf', f'scale={str(self.thumbnail_size[0])}:-1', '-vframes', '1', '-update', 'true', thumbnail_file_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
        except subprocess.TimeoutExpired:
            Logger.warning(f'ffmpeg thumbnail timed out for {filename}')
            return None
        if result.returncode != 0:
            Logger.warning(f'ffmpeg thumbnail rc={result.returncode} for {filename}: {result.stdout.decode("utf8")}')
        if os.path.exists(thumbnail_file_path):
            Logger.debug(f'thumbnail file created for {filename}, output: {result.stdout.decode("utf8")}')
            return thumbnail_file_path
        else:
            Logger.debug(f'thumbnail file not created for {filename}, output: {result.stdout.decode("utf8")}')

    @logged
    def create_audio_thubnail(self, filename, duration):
        """Generate a waveform overview image for an audio file using ``audiowaveform``.

        Produces a PNG-format thumbnail at ``thumbnail_size`` pixels using
        8-bit sample data.  The ``-e`` (end time) argument is
        ``duration.milliseconds_rounded / 1000`` seconds.

        Args:
            filename: Filename of the source audio file inside ``<media_path>``.
            duration: ``CTimecode`` duration of the audio file.

        Returns:
            Absolute path to the created thumbnail file, or ``None`` if
            ``audiowaveform`` did not produce an output file.
        """
        # audiowaveform -i sample.wav -o sample.dat -b 8
        file_path = self.get_file_path(filename)
        thumbnail_file_path = self.get_thumbnail_path(filename)
        # TODO: support 24-bit data
        try:
            result = subprocess.run(['audiowaveform', '-i', file_path, '-o', thumbnail_file_path, '-e', str(duration.milliseconds_rounded / 1000), '-w', str(self.thumbnail_size[0]), '-h', str(self.thumbnail_size[1]), '--no-axis-labels', '--amplitude-scale', '0.9'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
        except subprocess.TimeoutExpired:
            Logger.warning(f'audiowaveform thumbnail timed out for {filename}')
            return None
        if result.returncode != 0:
            Logger.warning(f'audiowaveform thumbnail rc={result.returncode} for {filename}: {result.stdout.decode("utf8")}')
        if os.path.exists(thumbnail_file_path):
            return thumbnail_file_path
        else:
            Logger.debug(f'thumbnail file not created for {filename}, output: {result.stdout.decode("utf8")}')

    def create_audio_waveform(self, filename):
        """Generate an 8-bit audiowaveform JSON data file for an audio file.

        Writes to ``<waveform_path>/<stem>_<ext>.json``.

        Args:
            filename: Filename of the source audio file inside ``<media_path>``.

        Returns:
            Absolute path to the created waveform file, or ``None`` if
            ``audiowaveform`` did not produce an output file.
        """
        # audiowaveform -i sample.wav -o sample.dat -b 8
        file_path = self.get_file_path(filename)
        waveform_file_path = self.get_waveform_path(filename)
        try:
            result = subprocess.run(['audiowaveform', '-i', file_path, '-o', waveform_file_path, '-b', '8'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
        except subprocess.TimeoutExpired:
            Logger.warning(f'audiowaveform data timed out for {filename}')
            return None
        if result.returncode != 0:
            Logger.warning(f'audiowaveform data rc={result.returncode} for {filename}: {result.stdout.decode("utf8")}')
        if os.path.exists(waveform_file_path):
            Logger.debug(f'waveform file created for {filename}, output: {result.stdout.decode("utf8")}')
            return waveform_file_path
        else:
            Logger.debug(f'waveform file not created for {filename}, output: {result.stdout.decode("utf8")}')

    def create_video_index(self, filename):
        """Run ``cuems-videoindexer`` to pre-build the frame index sidecar (``.idx``) for a video file.

        The ``.idx`` file is created in ``<media_path>/indexes/`` so that
        ``videocomposer`` can call ``loadCachedIndex`` on first arm instead of
        building the index at playback time.

        Args:
            filename: Filename of the video file inside ``<media_path>``.
        """
        file_path = self.get_file_path(filename)
        try:
            result = subprocess.run(
                ['cuems-videoindexer', file_path],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                timeout=300
            )
            idx_dir = os.path.join(self.media_path, 'indexes')
            idx_path = os.path.join(idx_dir, os.path.basename(file_path) + '.idx')
            if os.path.exists(idx_path):
                Logger.debug(f'video index created for {filename}')
            else:
                Logger.warning(f'video index not created for {filename}: {result.stdout.decode("utf8")}')
        except Exception as e:
            Logger.warning(f'video index failed for {filename}: {e}')

    def get_file_path(self, filename, trash_state=False):
        """Return the absolute path for a media file in the live or trash directory.

        Args:
            filename: Media ``unix_name``.
            trash_state: ``True`` to resolve inside the trash directory.
                Defaults to ``False``.

        Returns:
            Absolute path string.
        """
        if trash_state is False:
            return os.path.join(self.media_path, filename)
        else:
            return os.path.join(self.trash_path, filename)

    def get_thumbnail_filename(self, filename):
        """Derive the thumbnail filename for *filename*.

        Convention: ``<stem>_<ext_without_dot><thumbnail_extension>``
        e.g. ``concert.mp4`` → ``concert_mp4.jpg``.

        Args:
            filename: Source media filename.

        Returns:
            Thumbnail filename string (no directory component).
        """
        name_root, file_extension = os.path.splitext(filename)
        thumbnail_file_name = f'{name_root}_{file_extension[1:]}{self.thumbnail_extension}'
        return thumbnail_file_name

    def get_thumbnail_path(self, filename, trash_state=False):
        """Return the absolute path for a thumbnail file.

        Args:
            filename: Source media ``unix_name``.
            trash_state: ``True`` to resolve inside the trash thumbnail
                directory.  Defaults to ``False``.

        Returns:
            Absolute path string.
        """
        thumbnail_file_name = self.get_thumbnail_filename(filename)
        if trash_state is False:
            thumbnail_file_path = os.path.join(self.thumbnail_path, thumbnail_file_name)
        else:
            thumbnail_file_path = os.path.join(self.thumbnail_trash_path, thumbnail_file_name)
        return thumbnail_file_path

    def get_waveform_filename(self, filename):
        """Derive the waveform filename for *filename*.

        Convention: ``<stem>_<ext_without_dot><waveform_extension>``
        e.g. ``concert.wav`` → ``concert_wav.json``.

        Args:
            filename: Source media filename.

        Returns:
            Waveform filename string (no directory component).
        """
        name_root, file_extension = os.path.splitext(filename)
        waveform_file_name = f'{name_root}_{file_extension[1:]}{self.waveform_extension}'
        return waveform_file_name

    def get_waveform_path(self, filename, trash_state=False):
        """Return the absolute path for a waveform file.

        Args:
            filename: Source media ``unix_name``.
            trash_state: ``True`` to resolve inside the trash waveform
                directory.  Defaults to ``False``.

        Returns:
            Absolute path string.
        """
        waveform_file_name = self.get_waveform_filename(filename)
        if trash_state is False:
            waveform_file_path = os.path.join(self.waveform_path, waveform_file_name)
        else:
            waveform_file_path = os.path.join(self.waveform_trash_path, waveform_file_name)
        return waveform_file_path

    def add_binary_header(self, binary_data, uuid_string, type_number):
        """Prepend a 36-byte UUID header to *binary_data*.

        The frontend identifies the media item from this header when it
        receives the binary WebSocket frame without an out-of-band correlation
        identifier.

        Args:
            binary_data: Raw bytes of the thumbnail or waveform file.
            uuid_string: 36-character UUID string to embed.
            type_number: Integer payload type tag (1 = thumbnail, 2 = waveform).

        Returns:
            ``bytes`` — 36-byte UUID header followed by *binary_data*.
        """
        # 36 bytes; first 36 positions, char = uuid
        return struct.pack('<36s', str.encode(uuid_string)) + binary_data

    def check_if_media_existed_in_projects(self, filename):
        """Query ``ProjectMedia`` rows referencing *filename* regardless of media FK.

        Used by ``CuemsUpload`` after a successful upload to detect re-uploads
        of files that projects already reference by filename.

        Args:
            filename: ``unix_name`` to look up in ``ProjectMedia.media_filename``.

        Returns:
            Peewee ``ModelSelect`` of matching ``ProjectMedia`` rows, or
            ``False`` if none exist.
        """
        projectmedia_mproject_refs = ProjectMedia.select(ProjectMedia.project_id, ProjectMedia.media_filename).where(ProjectMedia.media_filename == filename)

        if projectmedia_mproject_refs:
            return projectmedia_mproject_refs
        else:
            return False
