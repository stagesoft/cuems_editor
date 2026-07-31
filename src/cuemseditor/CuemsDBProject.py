import os
import traceback
import shutil
from peewee import DoesNotExist, IntegrityError, prefetch

from cuemsutils.tools.StringSanitizer import StringSanitizer
from cuemsutils.tools.CopyMoveVersioned import CopyMoveVersioned
from cuemsutils.tools.CTimecode import CTimecode
from cuemsutils.xml.Parsers import CuemsParser
from cuemsutils.xml.XmlReaderWriter import XmlReaderWriter
from cuemsutils.helpers import new_datetime, new_uuid
from cuemsutils.log import logged, Logger


from cuemseditor.CuemsErrors import *
from cuemseditor.CuemsDBModel import Project, Media, ProjectMedia


class DurationFixStats:
    """Result of a :func:`fix_media_durations_in_contents` walk.

    Attributes:
        media_refs: Number of cue ``Media`` blocks visited.
        replacements: Number of ``duration`` values actually changed.
        orphans: List of ``file_name`` values referencing a media file with no
            ``Media`` row (duration left untouched — a leftover
            ``00:00:00.000`` there matches the valid timecode shape, so it is
            invisible to a pattern scan; report it explicitly instead).
    """

    def __init__(self):
        self.media_refs = 0
        self.replacements = 0
        self.orphans = []


def db_duration_resolver(file_name):
    """Resolve a media ``unix_name`` to its stored duration string (or ``None``).

    Raises ``KeyError`` when no ``Media`` row exists for *file_name*, so the
    walk can distinguish an orphaned reference from a present-but-NULL duration.
    """
    try:
        media = Media.get(Media.unix_name == file_name)
    except DoesNotExist:
        raise KeyError(file_name)
    return media.duration


def fix_media_durations_in_contents(contents, resolver):
    """Overwrite each cue ``Media['duration']`` from *resolver*, in place.

    Shared by the WebSocket save path (:meth:`CuemsDBProject._fix_media_durations`)
    and the standalone duration-repair script, so both trust the same DB source
    of truth and walk cue trees identically.

    Args:
        contents: ``CueList['contents']`` list (recursively walked).
        resolver: callable ``file_name -> duration_str_or_None``; must raise
            ``KeyError`` for a media file absent from the DB.

    Returns:
        :class:`DurationFixStats`.
    """
    stats = DurationFixStats()
    _walk_media_durations(contents, resolver, stats)
    return stats


def _walk_media_durations(contents, resolver, stats):
    if not contents:
        return
    for item in contents:
        # Handle nested CueLists
        if 'CueList' in item:
            _walk_media_durations(item['CueList'].get('contents', []), resolver, stats)

        # Check for AudioCue or VideoCue wrappers
        if 'AudioCue' in item:
            cue_data = item['AudioCue']
        elif 'VideoCue' in item:
            cue_data = item['VideoCue']
        else:
            cue_data = item  # flat structure

        media = cue_data.get('Media') if isinstance(cue_data, dict) else None
        if media and isinstance(media, dict):
            file_name = media.get('file_name')
            if file_name:
                stats.media_refs += 1
                try:
                    duration = resolver(file_name)
                except KeyError:
                    stats.orphans.append(file_name)  # media not in DB; keep original
                    continue
                if duration:
                    new_value = str(duration)
                    if media.get('duration') != new_value:
                        media['duration'] = new_value
                        stats.replacements += 1


def _fade_duration_ms(duration):
    """Best-effort milliseconds for a FadeCue ``duration`` JSON value.

    *duration* is the raw JSON shape the frontend/XML round-trip produces:
    ``{'CTimecode': '<timecode string>'}`` (dict possibly empty or with a
    ``None`` value) or ``None``.  Parsing delegates to the real ``CTimecode``
    on purpose: its millisecond semantics are literal and non-obvious
    (``'.3'`` is 3 ms, not 300) and some shapes raise (``'00:00:03'`` →
    ``IndexError``), so a hand-rolled parser here would silently diverge from
    what the engine computes when it loads the same string.  Any parse
    failure returns ``None``.
    """
    if isinstance(duration, dict):
        duration = duration.get('CTimecode')
    if not isinstance(duration, str) or not duration.strip():
        return None
    try:
        return CTimecode(duration).milliseconds_rounded
    except Exception:
        return None


def _walk_fade_durations(contents, offenders):
    if not contents:
        return
    for item in contents:
        if not isinstance(item, dict):
            continue
        if 'CueList' in item:
            _walk_fade_durations(item['CueList'].get('contents', []), offenders)
        if 'FadeCue' in item:
            cue_data = item['FadeCue']
            if not isinstance(cue_data, dict):
                continue
            ms = _fade_duration_ms(cue_data.get('duration'))
            if ms is None or ms <= 0:
                reason = (
                    'missing or unparseable'
                    if ms is None else 'must be greater than zero'
                )
                offenders.append(
                    (cue_data.get('name'), cue_data.get('id'), reason)
                )


def validate_fade_durations_in_contents(contents):
    """Reject any FadeCue whose duration is missing, unparseable, or <= 0.

    Save-time gate: ``CuemsParser``/``GenericParser`` assigns via
    ``dict.__setitem__``, bypassing ``FadeCue.set_duration``'s own
    positive-and-non-zero rule, so a zero coming from a client would reach
    the XML and later become a silent no-op at reveal (gradient-motiond
    drops ``dur <= 0`` over fire-and-forget UDP).  Collects ALL offenders and
    raises a single ``ValueError``; the WS layer forwards the text to the
    client.
    """
    offenders = []
    _walk_fade_durations(contents, offenders)
    if offenders:
        detail = '; '.join(
            f"'{name or 'unnamed'}' (id {cue_id or 'unknown'}: {reason})"
            for name, cue_id, reason in offenders
        )
        raise ValueError(
            f"FadeCue duration must be greater than zero: {detail}"
        )


class CuemsDBProject(StringSanitizer):
    """Project CRUD, XML script I/O, and filesystem directory management.

    Each CueMS project lives at
    ``<projects_path>/<unix_name>/cue_script.xml``.  All mutating operations
    are wrapped in Peewee atomic transactions with rollback; filesystem
    changes are reversed on failure where possible.

    User-supplied name strings are sanitised via the inherited
    ``StringSanitizer`` methods before being used as directory names or
    stored in the database.

    Example:
        >>> db_project = CuemsDBProject(settings_dict, database)
        >>> uuid = db_project.new({"CuemsScript": {"name": "My Show", ...}}, "my-show")
        >>> project_data = db_project.load(uuid)
        >>> db_project.update(uuid, project_data)
        >>> db_project.delete(uuid)          # soft-delete → trash
        >>> db_project.restore(uuid)         # restore from trash
        >>> db_project.delete_from_trash(uuid)  # permanent delete
    """

    def __init__(self, settings_dict, db_connection):
        """Initialise paths from *settings_dict*.

        Args:
            settings_dict: Mapping consumed from ``settings.xml``.  Required
                keys: ``tmp_path``, ``library_path``, ``script_file_name``,
                ``script_schema_name``, ``project_folder_name``,
                ``trash_folder_name``, ``media_folder_name``.
            db_connection: The shared Peewee ``SqliteDatabase`` instance owned
                by ``CuemsDBManager``.

        Raises:
            KeyError: If any required key is missing from *settings_dict*.
        """
        self.db = db_connection
        self.settings_dict = settings_dict
        try:
            self.tmp_path = self.settings_dict['tmp_path']
            self.library_path = settings_dict['library_path']
            self.script_file_name = settings_dict['script_file_name']
            self.script_schema_name = settings_dict['script_schema_name']
            self.projects_path = os.path.join(self.library_path, settings_dict['project_folder_name'])
            self.trash_path = os.path.join(self.library_path, settings_dict['trash_folder_name'], settings_dict['project_folder_name'])
            self.media_path = os.path.join(self.library_path, settings_dict['media_folder_name'])
        except KeyError as e:
            Logger.error(f'can not read settings {e}')
            raise e

    def get_project_unix_name(self, uuid):
        """Return the filesystem directory name for a live (non-trashed) project.

        Args:
            uuid: Project UUID string.

        Returns:
            ``unix_name`` value from the ``Project`` DB record.

        Raises:
            NonExistentItemError: If no live project with *uuid* exists.
        """
        try:
            project = Project.get((Project.uuid == uuid) & (Project.in_trash == False))
            return project.unix_name
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def load(self, uuid, include_trash=False):
        """Load a project's ``CuemsScript`` dict from its XML file on disk.

        Args:
            uuid: Project UUID string.
            include_trash: When ``True``, also searches trashed projects.
                Defaults to ``False``.

        Returns:
            ``dict`` produced by ``XmlReaderWriter.read()`` — the parsed
            ``CuemsScript`` structure.

        Raises:
            NonExistentItemError: If no project with *uuid* exists (or the
                project is in the trash and *include_trash* is ``False``).
        """
        try:
            if not include_trash:
                project = Project.get((Project.uuid == uuid) & (Project.in_trash == False))
            else:
                project = Project.get(Project.uuid == uuid)
            return self.load_xml(project.unix_name)
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def list(self):
        """Return a list of all live (non-trashed) projects, newest first.

        Returns:
            List of single-key dicts ``{uuid_str: {name, unix_name,
            description, created, modified}}``, ordered by ``created``
            descending.

        Example:
            >>> projects = db_project.list()
            >>> for entry in projects:
            ...     uuid, meta = next(iter(entry.items()))
            ...     print(uuid, meta['name'])
        """
        project_list = list()
        projects = Project.select().where(Project.in_trash == False).order_by(Project.created.desc())
        for project in projects:
            project_dict = {str(project.uuid): {'name': project.name, 'unix_name': project.unix_name, 'description': project.description, 'created': project.created, 'modified': project.modified}}
            project_list.append(project_dict)

        return project_list

    def list_trash(self):
        """Return a list of all trashed projects, newest first.

        Returns:
            Same shape as :meth:`list` but for projects where
            ``in_trash == True``.
        """
        project_trash_list = list()
        projects_trash = Project.select().where(Project.in_trash == True).order_by(Project.created.desc())
        for project in projects_trash:
            project_dict = {str(project.uuid): {'name': project.name, 'unix_name': project.unix_name, 'description': project.description, 'created': project.created, 'modified': project.modified}}
            project_trash_list.append(project_dict)

        return project_trash_list

    def update(self, uuid, data):
        """Save an edited project: update DB metadata and rewrite the XML file.

        Runs inside a Peewee atomic transaction.  The XML is validated
        against ``script.xsd`` by ``XmlReaderWriter.write_from_object`` —
        an invalid cue tree raises before any persistent change is made.

        A pre-save pass via :meth:`_fix_media_durations` corrects zero
        durations sent by the frontend.

        Args:
            uuid: Project UUID string; must match ``data['CuemsScript']['id']``.
            data: ``CuemsScript`` dict as received from the frontend over
                WebSocket.

        Raises:
            NonExistentItemError: If the project does not exist or is in the
                trash.
            Exception: Re-raises any error after rolling back the transaction.
        """
        try:
            project = Project.get((Project.uuid == uuid) & (Project.in_trash == False))
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

        try:
            del data['CuemsScript']['unix_name']
        except KeyError:
            pass

        # SAFETY NET: older frontends send Media.duration as '00:00:00.000';
        # overwrite from the DB (source of truth) before saving.
        # As of the media-duration fix, the frontend copies the real duration
        # from the file_list payload (CuemsDBMedia.list() now carries a
        # 'duration' key) — remove this net only once ALL deployed frontends
        # AND editors are >= those versions.
        self._fix_media_durations(data)

        self._clean_dangling_targets(data)

        # Reject zero/unparseable FadeCue durations BEFORE parsing: the parser
        # bypasses FadeCue.set_duration, and a saved zero later becomes a
        # silent no-op at reveal (gradient-motiond drops dur <= 0).
        validate_fade_durations_in_contents(
            (data.get('CuemsScript', {}).get('CueList') or {}).get('contents') or []
        )

        with self.db.atomic() as transaction:
            try:
                project.name = StringSanitizer.sanitize_name(data['CuemsScript']['name'])
                now = new_datetime()
                data['CuemsScript']['modified'] = now
                project.modified = now
                project.description = StringSanitizer.sanitize_text_size(data['CuemsScript']['description'])
                project.save()
                project_object = CuemsParser(data).parse()
                self.update_media_relations(project, project_object)
                self.save_xml(project.unix_name, project_object)
            except Exception as e:
                Logger.error("error: {} {} trying to update  project, rolling back database update".format(type(e), e))
                transaction.rollback()
                raise e

    # SAFETY NET (see update()): overwrite frontend-sent Media durations from
    # the DB. Delegates to the module-level fix_media_durations_in_contents so
    # the repair script and this path share one walk.
    def _fix_media_durations(self, data):
        """Overwrite each cue's ``Media.duration`` from the database.

        The DB is the source of truth for media duration. This corrects the
        legacy frontend behaviour of sending ``00:00:00.000``. Orphaned media
        references (no ``Media`` row) are logged, not modified.
        """
        try:
            cuelist = data.get('CuemsScript', {}).get('CueList', {})
            contents = cuelist.get('contents', [])
            stats = fix_media_durations_in_contents(contents, db_duration_resolver)
            if stats.orphans:
                Logger.warning(
                    f"media duration fix: {len(stats.orphans)} cue(s) reference "
                    f"media not in DB (duration left as-is): {stats.orphans}")
        except Exception as e:
            Logger.warning(f"Could not fix media durations: {e}")

    CUE_TYPES = ['AudioCue', 'VideoCue', 'DmxCue', 'ActionCue', 'CueList']

    def _clean_dangling_targets(self, data):
        """Clear target and action_target references that point to non-existing cues.

        When a cue is deleted in the frontend, any ActionCue (or regular cue)
        still referencing it by UUID will have a dangling reference. This method
        collects all cue UUIDs and nullifies any reference that cannot be resolved.
        """
        try:
            cuelist = data.get('CuemsScript', {}).get('CueList', {})
            contents = cuelist.get('contents', [])
            all_ids = set()
            self._collect_cue_ids(contents, all_ids)
            self._nullify_dangling_refs(contents, all_ids)
        except Exception as e:
            Logger.warning(f"Could not clean dangling targets: {e}")

    def _collect_cue_ids(self, contents, ids):
        """Recursively collect all cue UUIDs from the project contents."""
        if not contents:
            return
        for item in contents:
            for cue_type in self.CUE_TYPES:
                if cue_type in item:
                    cue_data = item[cue_type]
                    cue_id = cue_data.get('id')
                    if cue_id:
                        ids.add(cue_id)
                    if cue_type == 'CueList':
                        self._collect_cue_ids(cue_data.get('contents', []), ids)

    def _nullify_dangling_refs(self, contents, valid_ids):
        """Recursively clear target/action_target refs that point to non-existing cues."""
        if not contents:
            return
        for item in contents:
            for cue_type in self.CUE_TYPES:
                if cue_type in item:
                    cue_data = item[cue_type]
                    if cue_type == 'CueList':
                        self._nullify_dangling_refs(cue_data.get('contents', []), valid_ids)
                        continue
                    target = cue_data.get('target')
                    if target and target not in valid_ids:
                        Logger.warning(f"{cue_type} {cue_data.get('id')} has dangling target {target}, clearing")
                        cue_data['target'] = None
                    if cue_type == 'ActionCue':
                        action_target = cue_data.get('action_target')
                        if action_target and action_target not in valid_ids:
                            Logger.warning(f"ActionCue {cue_data.get('id')} has dangling action_target {action_target}, clearing")
                            cue_data['action_target'] = None

    def new(self, data, unix_name):
        """Create a new project: allocate a UUID, write the XML, insert the DB row.

        The project directory ``<projects_path>/<unix_name>/`` and its
        ``cue_script.xml`` are created inside an atomic transaction.  The
        directory is removed on rollback.

        Args:
            data: ``CuemsScript`` dict from the frontend; ``id``, ``created``,
                and ``modified`` are assigned here and written back into
                *data* before parsing.
            unix_name: Raw directory name candidate supplied by the frontend;
                sanitised via ``StringSanitizer.sanitize_dir_permit_increment``
                before use.

        Returns:
            New project UUID string.

        Raises:
            IntegrityError: If ``name`` or ``unix_name`` already exists.
            KeyError: If ``data['CuemsScript']`` is missing required fields.
            Exception: Re-raises after rollback and directory cleanup.
        """
        try:
            unix_name = StringSanitizer.sanitize_dir_permit_increment(unix_name)
        except Exception as e:
            raise e

        try:
            project_uuid = str(new_uuid())
            data['CuemsScript']['id'] = project_uuid
            now = new_datetime()
            data['CuemsScript']['created'] = now
            data['CuemsScript']['modified'] = now
        except KeyError as e:
            Logger.error("error: Missing {} ;trying to make new  project, rolling back database insert".format(e))
            raise e
        except Exception as e:
            Logger.error("error: {} {} ;trying to read  project data".format(type(e), e))
            raise e

        # Same save-time gate as update() — see validate_fade_durations_in_contents.
        validate_fade_durations_in_contents(
            (data.get('CuemsScript', {}).get('CueList') or {}).get('contents') or []
        )

        with self.db.atomic() as transaction:
            try:
                project = Project.create(uuid=project_uuid, unix_name=unix_name, name=StringSanitizer.sanitize_name(data['CuemsScript']['name']), description=StringSanitizer.sanitize_text_size(data['CuemsScript']['description']), created=now, modified=now)
                os.mkdir(os.path.join(self.projects_path, unix_name))
                Logger.debug('data is now: {}'.format(data))
                project_object = CuemsParser(data).parse()
                Logger.debug(f'project_object is now: {type(project_object)},{project_object}')
                self.add_media_relations(project, project_object)
                self.save_xml(unix_name, project_object)
                return project_uuid
            except IntegrityError as e:
                transaction.rollback()
                Logger.error("error: {} {} ;name or unix_name already exists, rolling back database insert".format(type(e), e))
                raise e
            except Exception as e:
                transaction.rollback()
                Logger.error("error: {} {} ;trying to make new  project, rolling back database insert".format(type(e), e))

                if os.path.exists(os.path.join(self.projects_path, unix_name)):
                    shutil.rmtree(os.path.join(self.projects_path, unix_name))

                raise e

    def _is_name_available(self, unix_name, display_name):
        """Check that both unix_name and display_name are free in the DB
        (including trashed records, since unique constraints span all rows).
        """
        return not Project.select().where(
            (Project.unix_name == unix_name) | (Project.name == display_name)
        ).exists()

    def duplicate(self, uuid):
        """Duplicate a project: copy the directory and create a new DB record.

        The copy is named ``<original_name> - Copy``; a numeric suffix
        ``(N)`` is appended if the name is taken by another live or trashed
        record.  The duplicated ``cue_script.xml`` has its ``CuemsScript.id``
        and ``name`` updated to match the new DB record.

        Args:
            uuid: UUID of the project to duplicate; must be a live project.

        Returns:
            New project UUID string.

        Raises:
            NonExistentItemError: If the source project does not exist.
            Exception: Re-raises after rollback and copy cleanup.
        """
        try:
            project = Project.get((Project.uuid == uuid) & (Project.in_trash == False))
            with self.db.atomic() as transaction:
                try:
                    new_unix_name = None
                    project_path = os.path.join(self.projects_path, project.unix_name)
                    base_unix = project.unix_name
                    base_display = project.name + ' - Copy'

                    # Find a name pair unique on both filesystem AND DB
                    # (trashed projects keep their DB records and unique constraints)
                    candidate_unix = base_unix
                    candidate_display = base_display
                    i = 0
                    while (os.path.exists(os.path.join(self.projects_path, candidate_unix))
                           or not self._is_name_available(candidate_unix, candidate_display)):
                        i += 1
                        candidate_unix = f"{base_unix}-{i:03d}"
                        candidate_display = f"{base_display} ({i})"

                    shutil.copytree(project_path, os.path.join(self.projects_path, candidate_unix))
                    new_unix_name = candidate_unix

                    project.unix_name = new_unix_name
                    new_project_uuid = str(new_uuid())
                    project.uuid = new_project_uuid
                    project.name = candidate_display
                    project.modified = new_datetime()
                    project.save(force_insert=True)

                    dup_project = Project.get(Project.uuid == new_project_uuid)
                    data = self.load_xml(dup_project.unix_name)
                    data['CuemsScript']['id'] = new_project_uuid
                    data['CuemsScript']['name'] = project.name
                    data['CuemsScript']['modified'] = project.modified
                    # NO fade-duration validation here on purpose: the source is
                    # load_xml of an existing project — legacy scripts must stay
                    # duplicable; the engine's reveal guard covers them.
                    project_object = CuemsParser(data).parse()
                    self.add_media_relations(dup_project, project_object)
                    self.save_xml(new_unix_name, project_object)
                    return new_project_uuid
                except Exception as e:
                    Logger.error("error: {} {}; trying to duplicate  project, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    if new_unix_name is None:  # if move or copy where not successful with don't need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.projects_path, new_unix_name)):
                        shutil.rmtree(os.path.join(self.projects_path, new_unix_name))
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def export(self, uuid):
        tmp_project_path = None
        output_filename = None
        try:
            try:
                project = Project.get(Project.uuid==uuid)
            except DoesNotExist:
                raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

            unix_name = project.unix_name
            project_path = os.path.join(self.projects_path, unix_name, self.script_file_name)
            project_medias = ProjectMedia.select().where(ProjectMedia.project == project)
            tmp_project_path = os.path.join(self.tmp_path, unix_name)
            if not os.path.exists(tmp_project_path):
                os.makedirs(tmp_project_path)
            Logger.debug('exporting project {} to {}'.format(unix_name, tmp_project_path))
            shutil.copy(project_path, tmp_project_path)

            project_medias = prefetch(project_medias, Media)
            if project_medias:
                Logger.debug('project {} has media relations, exporting them'.format(unix_name))
                tmp_media_path = os.path.join(tmp_project_path, 'media')
                os.makedirs(tmp_media_path)
                for media in project_medias:
                    media_path = os.path.join(self.media_path, media.media.unix_name)
                    try:
                        shutil.copy(media_path, tmp_media_path)
                        Logger.debug('copying media {} to {}'.format(media.media.unix_name, tmp_media_path))
                    except Exception as e:
                        Logger.error("error: {} {}; copying media to project export dir".format(type(e), e))
                        raise e
            else:
                Logger.debug('project {} has no media relations, skipping media export'.format(unix_name))

            shutil.make_archive(tmp_project_path, 'zip', self.tmp_path, unix_name)
            output_filename = unix_name + '.zip'
            server_export_path = os.path.join(self.settings_dict['html_root_path'], self.settings_dict['export_folder_name'])
            try:
                dest_filename = CopyMoveVersioned.move(os.path.join(self.tmp_path, output_filename), server_export_path, output_filename)
                return os.path.join(self.settings_dict['export_folder_name'], dest_filename)
            except Exception as e:
                Logger.error("error: {} {}; moving exported project to exports folder".format(type(e), e))
                raise e
        except Exception as e:
            Logger.error("error: {} {}; exporting project".format(type(e), e))
            raise e
        finally:
            if tmp_project_path is not None and os.path.exists(tmp_project_path):
                shutil.rmtree(tmp_project_path)
                Logger.debug('cleaning tmp project export folder: {}'.format(tmp_project_path))
            if output_filename is not None and os.path.exists(os.path.join(self.tmp_path, output_filename)):
                os.remove(os.path.join(self.tmp_path, output_filename))
                Logger.debug('cleaning tmp project export file: {}'.format(os.path.join(self.tmp_path, output_filename)))

    def delete(self, uuid):
        """Soft-delete a project by moving it to the trash directory.

        The project directory is moved via ``CopyMoveVersioned.move`` to
        ``<trash_path>/``.  The DB row's ``in_trash`` flag is set to ``True``
        atomically.  On any failure the directory is moved back and the
        transaction is rolled back.

        Args:
            uuid: UUID of the live project to trash.

        Raises:
            NonExistentItemError: If no live project with *uuid* exists.
            Exception: Re-raises after rollback and filesystem cleanup.
        """
        try:
            project = Project.get((Project.uuid == uuid) & (Project.in_trash == False))
            with self.db.atomic() as transaction:
                try:
                    dest_filename = None
                    file_path = os.path.join(self.projects_path, project.unix_name)
                    dest_filename = CopyMoveVersioned.move(file_path, self.trash_path, project.unix_name)
                    project.in_trash = True
                    project.save()
                    Logger.debug('updating instance in db: {}'.format(project))
                except Exception as e:
                    Logger.error("error: {} {}; trying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not successful with don't need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.trash_path, dest_filename)):
                        shutil.move(os.path.join(self.trash_path, dest_filename), os.path.join(self.projects_path, project.unix_name))
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def restore(self, uuid):
        """Restore a trashed project back to the active projects directory.

        Moves the directory from ``<trash_path>/`` to ``<projects_path>/``
        via ``CopyMoveVersioned.move``, which handles name collisions by
        appending a numeric suffix.  The DB record's ``unix_name`` is updated
        to the actual destination name and ``in_trash`` is set to ``False``.

        Args:
            uuid: UUID of the trashed project to restore.

        Raises:
            NonExistentItemError: If no trashed project with *uuid* exists.
            Exception: Re-raises after rollback and filesystem cleanup.
        """
        try:
            project_trash = Project.get((Project.uuid == uuid) & (Project.in_trash == True))

            with self.db.atomic() as transaction:
                try:
                    dest_filename = None
                    project_path = os.path.join(self.trash_path, project_trash.unix_name)
                    dest_filename = CopyMoveVersioned.move(project_path, self.projects_path, project_trash.unix_name)
                    project_trash.unix_name = dest_filename
                    project_trash.in_trash = False
                    project_trash.save()
                    Logger.debug('updating instance in db: {}'.format(project_trash))
                except Exception as e:
                    Logger.error("error: {} {}; trying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not successful with don't need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.projects_path, dest_filename)):
                        shutil.move(os.path.join(self.projects_path, dest_filename), os.path.join(self.trash_path, project_path.unix_name))
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def delete_from_trash(self, uuid):
        """Permanently delete a trashed project: remove the DB record and directory.

        Calls ``project.delete_instance(recursive=True)`` to cascade-delete
        ``ProjectMedia`` join rows, then removes the project directory tree
        with ``shutil.rmtree``.

        Args:
            uuid: UUID of a project that is currently in the trash.

        Raises:
            NonExistentItemError: If no trashed project with *uuid* exists.
            Exception: Re-raises after rolling back the transaction (directory
                may already be gone at that point).
        """
        try:
            project = Project.get((Project.uuid == uuid) & (Project.in_trash == True))

            with self.db.atomic() as transaction:
                try:
                    project_path = os.path.join(self.trash_path, project.unix_name)
                    project.delete_instance(recursive=True)
                    shutil.rmtree(project_path)  # non empty dir, must use rmtree
                    Logger.debug('deleting project from trash: {}'.format(project))
                except Exception as e:
                    Logger.error("error: {} {}; trying to delete project to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def add_media_relations(self, project, project_object):
        """Create ``ProjectMedia`` join rows for all media referenced by *project_object*.

        Args:
            project: ``Project`` ORM instance.
            project_object: ``CuemsScript`` object whose ``get_media_filenames()``
                returns the set of ``unix_name`` strings to link.
        """
        media_filenames_list = project_object.get_media_filenames()
        for media_name in media_filenames_list:
            media = Media.get(Media.unix_name == media_name)
            ProjectMedia.create(project=project, media=media, media_filename=media_name)

    def update_media_relations(self, project, project_object):
        """Diff and sync ``ProjectMedia`` join rows after a project save.

        Computes the symmetric difference between the old and new media sets,
        then removes stale ``ProjectMedia`` rows and creates new ones.

        Args:
            project: ``Project`` ORM instance being saved.
            project_object: ``CuemsScript`` object reflecting the new cue
                tree after the save.
        """
        Logger.debug('updating media relations for project: {}'.format(project.unix_name))
        old_media_query = project.medias()
        old_media_dict = dict()
        Logger.debug('query done')
        for media in old_media_query:
            old_media_dict[media.unix_name] = str(media.uuid)
        old_media_list = list(old_media_dict.keys())
        Logger.debug('old media list: {}'.format(old_media_list))
        media_list = project_object.get_media_filenames()
        Logger.debug('media list: {}'.format(media_list))

        remove_set = set(old_media_list).difference(media_list)
        add_set = set(media_list).difference(old_media_list)

        Logger.debug('media remove list: {}'.format(remove_set))
        Logger.debug('media add list: {}'.format(add_set))

        if remove_set:
            for media_unix_name in remove_set:
                ProjectMedia.delete().where((ProjectMedia.project == project) & (ProjectMedia.media == old_media_dict[media_unix_name])).execute()

        if add_set:
            for media_unix_name in add_set:
                media = Media.select(Media.uuid).where(Media.unix_name == media_unix_name).get()
                ProjectMedia.create(project=project, media=media, media_filename=media_unix_name)

    def update_projects_existed_media(self, project_uuid, media_filename):
        """Re-link a re-uploaded media file to the projects that referenced it by filename.

        Called after ``CuemsUpload`` detects that *media_filename* was already
        referenced by one or more projects.  Reads the XML, finds matching
        cues, and reconciles the stored media UUID with what is now in the DB.

        Args:
            project_uuid: UUID of the project to update.
            media_filename: ``unix_name`` of the re-uploaded media file.
        """
        project_object = CuemsParser(self.load(project_uuid, include_trash=True)).parse()
        media_dict = project_object.get_media()
        matching_media_dict = dict()
        for cue_uuid, media_object in media_dict.items():
            for media_uiid, project_media_filename in media_object.items():
                if media_filename == project_media_filename:
                    matching_media_dict[cue_uuid] = media_object

        if matching_media_dict:
            Logger.debug('found cues with media filename: {} in project: {}'.format(media_filename, project_uuid))
            first_media_object = next(iter(matching_media_dict.values()))
            old_media_uuid = next(iter(first_media_object.keys()))
            # TODO: manage if  all media with same filename have the same uuid  SHOULD BE TRUE
            for cue_uuid, media in matching_media_dict.items():
                for media_uuid, media_filename in media.items():
                    if media_uuid != old_media_uuid:
                        Logger.warning('found different media uuid for same media filename: {} in project {},  cue {}, using first found: {}'.format(project_uuid, cue_uuid, media_filename))
            try:
                self.update_existed_media_uuid(media_filename, old_media_uuid)
            except IntegrityError:
                Logger.warning('error updating media uuid for media filename: {} from project: {}. Media uuid inconsistency detected'.format(media_filename, project_uuid))
            self.deletele_mising_media_references(media_filename)
            project = Project.get(Project.uuid == project_uuid)
            self.update_media_relations(project, project_object)
        else:
            Logger.warning('no cues found for media filename: {}'.format(media_filename))

    def update_existed_media_uuid(self, media_filename, old_media_uuid):
        """Overwrite the UUID of a ``Media`` row to match the value recorded in project XML.

        Used when a re-uploaded file produces a new UUID in the DB that differs
        from the UUID stored inside existing project XML files.

        Args:
            media_filename: ``unix_name`` identifying the ``Media`` row.
            old_media_uuid: The UUID string to assign (taken from the XML).

        Raises:
            NonExistentItemError: If no ``Media`` row has *media_filename*.
        """
        Logger.debug('updating media uuid for media filename: {} with old uuid: {}'.format(media_filename, old_media_uuid))
        try:
            media = Media.get(Media.unix_name == media_filename)
            with self.db.atomic() as transaction:
                try:
                    Media.update(uuid=old_media_uuid).where(Media.unix_name == media_filename).execute()
                except Exception as e:
                    Logger.error("error: {} {}; trying to update media uuid, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with unix_name: {} does not exist".format(media_filename))

    def deletele_mising_media_references(self, media_filename):
        """Delete ``ProjectMedia`` rows whose ``media_id`` FK is NULL for *media_filename*.

        Cleans up dangling join rows left when a ``Media`` record was replaced
        by a re-upload.

        Args:
            media_filename: ``unix_name`` to filter on.
        """
        Logger.debug('deleting missing media references for media filename: {}'.format(media_filename))
        missing_media_project_refs = ProjectMedia.delete().where(ProjectMedia.media_filename == media_filename and ProjectMedia.media_id.is_null()).execute()

    def save_xml(self, unix_name, project_object):
        """Write *project_object* to ``<projects_path>/<unix_name>/cue_script.xml``.

        Validates against ``script.xsd`` before writing; raises if the
        ``CuemsScript`` object fails schema validation.

        Args:
            unix_name: Project directory name.
            project_object: ``CuemsScript`` instance to serialize.
        """
        writer = XmlReaderWriter(schema_name=self.script_schema_name, xmlfile=(os.path.join(self.projects_path, unix_name, self.script_file_name)))
        writer.write_from_object(project_object)

    def load_xml(self, unix_name):
        """Read and parse ``<projects_path>/<unix_name>/cue_script.xml``.

        Args:
            unix_name: Project directory name.

        Returns:
            ``CuemsScript`` dict produced by ``XmlReaderWriter.read()``.
        """
        reader = XmlReaderWriter(schema_name=self.script_schema_name, xmlfile=(os.path.join(self.projects_path, unix_name, self.script_file_name)))
        return reader.read()
