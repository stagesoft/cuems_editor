import os
import shutil
import subprocess
import re
import struct
import uuid as uuid_module
from enum import Enum, auto
from peewee import *

from .CuemsUtils import StringSanitizer, CopyMoveVersioned, CuemsLibraryMaintenance, date_now_iso_utc
from .CuemsDBModel import Project, Media, ProjectMedia
from .CuemsErrors import *
from ..CTimecode import CTimecode
from ..log import *


SCRIPT_FILE_NAME = 'script.xml'
PROJECT_FOLDER_NAME = 'projects'
MEDIA_FOLDER_NAME = 'media'
TRASH_FOLDER_NAME = 'trash'
THUMBNAIL_FOLDER_NAME = 'thumbnail'
WAVEFORM_FOLDER_NAME = 'waveform'
THUMBNAIL_EXTENSION = '.png'
WAVEFORM_EXTENSION = '.dat'
THUMBNAIL_W = 240
THUMBNAIL_H = 240

class MediaType(Enum):
    MOVIE = auto()
    AUDIO = auto()
    IMAGE = auto()


class CuemsDBMedia(StringSanitizer):

    def __init__(self, library_path, tmp_upload_path, db_connection):
        self.library_path = library_path
        self.tmp_upload_path = tmp_upload_path
        self.db = db_connection
        self.media_path = os.path.join(self.library_path, MEDIA_FOLDER_NAME)
        self.trash_path = os.path.join(self.library_path, TRASH_FOLDER_NAME, MEDIA_FOLDER_NAME)
        self.thumbnail_path = os.path.join(self.media_path, THUMBNAIL_FOLDER_NAME)
        self.waveform_path = os.path.join(self.media_path, WAVEFORM_FOLDER_NAME)
        self.thumbnail_trash_path = os.path.join(self.trash_path, THUMBNAIL_FOLDER_NAME)
        self.waveform_trash_path = os.path.join(self.trash_path, WAVEFORM_FOLDER_NAME)

    def new(self, tmp_file_path, filename):
        with self.db.atomic() as transaction:
            trash_state = False
            try:
                dest_filename = None
                dest_filename = CopyMoveVersioned.move(tmp_file_path, self.media_path, filename)
                
                try:
                    _type = self.get_type(dest_filename)
                except Exception as e:
                    logger.warning(f'could not get media type; error : {e}')
                    _type = None
                    raise e

                try:
                    if _type in (MediaType.MOVIE, MediaType.AUDIO):
                        media_duration = self.get_duration(dest_filename)
                    else:
                        media_duration = None
                except Exception as e:
                    logger.warning(f'could not get media duration; error : {e}')
                    media_duration = None

                try:
                    if _type is MediaType.MOVIE:
                        dest_thumbnail_filename = None
                        dest_thumbnail_filename = self.create_video_thubnail(dest_filename, media_duration)
                    elif _type is MediaType.AUDIO:
                        dest_thumbnail_filename = None
                        dest_waveform_filename = None
                        dest_thumbnail_filename = self.create_audio_thubnail(dest_filename, media_duration)
                        dest_waveform_filename = self.create_audio_waveform(dest_filename)
                    elif _type is MediaType.IMAGE:
                        dest_thumbnail_filename = None
                        dest_thumbnail_filename = self.create_video_thubnail(dest_filename, None)
                except Exception as e:
                    logger.exception(f'could not generate {_type} thumbnail or waveform; error : {e}')
                    media_thumbnail_binary_data = None

                    
                Media.create(uuid=uuid_module.uuid1(), name=dest_filename, unix_name=dest_filename, created=date_now_iso_utc(), modified=date_now_iso_utc(), duration=media_duration, media_type=_type.name, in_trash=False)
            except Exception as e:
                logger.error("error: {} {} triying to move new file, rolling back database insert".format(type(e), e))
                transaction.rollback()
                if dest_filename is None and dest_thumbnail_filename is None:  # if move or copy where not sucessfull we dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
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
        media_list = list()

        medias = (Media
         .select(Media.uuid, Media.name, Media.unix_name, Media.created, Media.modified, Media.media_type,
         fn.COUNT(Case(Project.in_trash, (('0', 1),), None)).alias('in_project_count'),
         fn.COUNT(Case(Project.in_trash, (('1', 1),), None)).alias('in_project_trash_count'))
         .join(ProjectMedia, JOIN.LEFT_OUTER)  # Joins tweet -> favorite.
         .join(Project, JOIN.LEFT_OUTER, on=(Project.uuid==ProjectMedia.project))  # Joins user -> tweet.
         .where(Media.in_trash==False)
         .group_by(Media.uuid))
        for media in medias:
            media_dict = {str(media.uuid): {'name': media.name, 'unix_name': media.unix_name, 'created': media.created, 'modified': media.modified,  'type': media.media_type, "in_projects": media.in_project_count, "in_trash_projects" : media.in_project_trash_count} }
            media_list.append(media_dict)

        return media_list

    def list_trash(self):
        media_list = list()

        medias = (Media
         .select(Media.uuid, Media.name, Media.unix_name, Media.created, Media.modified, Media.media_type,
         fn.COUNT(Case(Project.in_trash, (('0', 1),), None)).alias('in_project_count'),
         fn.COUNT(Case(Project.in_trash, (('1', 1),), None)).alias('in_project_trash_count'))
         .join(ProjectMedia, JOIN.LEFT_OUTER)  # Joins tweet -> favorite.
         .join(Project, JOIN.LEFT_OUTER, on=(Project.uuid==ProjectMedia.project))  # Joins user -> tweet.
         .where(Media.in_trash==True)
         .group_by(Media.uuid))
        for media in medias:
            media_dict = {str(media.uuid): {'name': media.name, 'unix_name': media.unix_name, 'created': media.created, 'modified': media.modified, 'type': media.media_type, "in_projects": media.in_project_count, "in_trash_projects" : media.in_project_trash_count} }
            media_list.append(media_dict)

        return media_list

    def save(self, uuid, data):   #TODO: check uuid format
        try:
            media = Media.get((Media.uuid==uuid) & (Media.in_trash == False))
            with self.db.atomic() as transaction:
                try:
                    media.update(name=StringSanitizer.sanitize_name(data['uuid']['name']), description=StringSanitizer.sanitize_text_size(data['uuid']['description']), modified=date_now_iso_utc()).execute()
                    return 'updated'
                except Exception as e:
                    logger.error("error: {} {} triying to update  media data, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    raise e
            
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def load_meta(self, uuid):
        try:
            media = Media.get(Media.uuid==uuid)
            file_meta = dict()
            project_list = list()
            project_trash_list = list()
            media_projects_query = media.projects()
            for project in media_projects_query:
                if project.in_trash == False :
                    project_list.append(str(project.uuid))
                else:
                    project_trash_list.append(str(project.uuid))

            file_meta[uuid] = { 'name': media.name, 'unix_name': media.unix_name, 'description': media.description, 'created': media.created, 'modified': media.modified,  'duration': media.duration, 'type': media.media_type, 'in_trash': media.in_trash, 'in_projects' : project_list, 'in_trash_projects' : project_trash_list }
            return file_meta
            
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def load_thumbnail(self, uuid):
        try:
            media_filename = Media.get(Media.uuid==uuid).unix_name
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
        try:
            media_filename = Media.get(Media.uuid==uuid).unix_name
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
        try:
            trash_state = False
            media = Media.get((Media.uuid==uuid) & (Media.in_trash == trash_state))
        
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
                        logger.error("error: {} {}; triying to move thumbnail to trash".format(type(e), e))
                        raise e

                    if self.is_audio(media):
                        dest_waveform_filename = None
                        file_waveform_path = self.get_waveform_path(media.unix_name)
                        try:
                            if os.path.exists(file_waveform_path):
                                dest_waveform_filename = CopyMoveVersioned.move(file_waveform_path, self.waveform_trash_path)
                        except Exception as e:
                            logger.error("error: {} {}; triying to move waveform to trash".format(type(e), e))
                            raise e
                   
                    dest_filename = CopyMoveVersioned.move(file_path, self.trash_path)
                    media.in_trash = True
                    media.save()
                    logger.debug('modifing instance in table: {}'.format(media))
                except Exception as e:
                    logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    # if move or copy where not sucessfull we don't need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                    if dest_filename is None & dest_thumbnail_filename is None:
                        if self.is_audio(media):
                            if dest_waveform_filename is None:
                                raise e
                        else:
                            raise e

                    # check if any file has been moved to trash folder and return it to media folder's
                    logger.debug("moving files back to media folder")
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
        try:
            trash_state = True
            media_trash = Media.get((Media.uuid==uuid) & (Media.in_trash == trash_state))
        
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
                        logger.error("error: {} {}; triying to move thumbnail from trash".format(type(e), e))
                        raise e

                    if self.is_audio(media_trash):
                        dest_waveform_filename = None
                        file_waveform_path = self.get_waveform_path(media_trash.unix_name, trash_state=True)
                        try:
                            if os.path.exists(file_waveform_path):
                                dest_waveform_filename = CopyMoveVersioned.move(file_waveform_path, self.waveform_path)
                        except Exception as e:
                            logger.error("error: {} {}; triying to waveform from trash".format(type(e), e))
                            raise e

                    
                    dest_filename = CopyMoveVersioned.move(file_path, self.media_path)
                    media_trash.in_trash = False
                    media_trash.save()
                    logger.debug('deleting instance from table: {}'.format(media_trash))
                except Exception as e:
                    logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None and dest_thumbnail_filename is None:  # if move or copy where not sucessfull we dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        if self.is_audio(media_trash):
                            if dest_waveform_filename is None:
                                raise e
                        else:
                            raise e

                    if os.path.exists(self.get_file_path(dest_filename)):
                        shutil.move( self.get_file_path(dest_filename), self.get_file_path(media_trash.unix_name, trash_state=True))

                    if os.path.exists(self.get_thumbnail_path(dest_thumbnail_filename)):
                        shutil.move(self.get_thumbnail_path(dest_thumbnail_filename), self.get_thumbnail_path(media_trash.unix_name, trash_state=True))

                    if self.is_audio(media_trash):
                        if os.path.exists(self.get_waveform_path(dest_waveform_filename)):
                            shutil.move(self.get_waveform_path(dest_waveform_filename), self.get_waveform_path(media_trash.unix_name, trash_state=True))

                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def delete_from_trash(self, uuid):
        try:
            trash_state=True
            media = Media.get((Media.uuid==uuid) & (Media.in_trash == trash_state))

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
                    logger.debug('modifing instance in table: {}'.format(media))
                except Exception as e:
                    logger.error("error: {} {}; triying to delete file from trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def get_type(self, filename):
        movie_list = ('.mov', '.avi', '.mkv', '.mpg', '.mp4')
        audio_list = ('.aif', '.aiff', '.wav', '.mp3')
        image_list = ('.png', '.jpg', '.tga')
        name_root, file_extension = os.path.splitext(filename)
        _type = None

        if file_extension in movie_list:
            _type = MediaType.MOVIE
        elif file_extension in audio_list:
            _type = MediaType.AUDIO
        elif file_extension in image_list:
            _type = MediaType.IMAGE

        return _type

    def is_audio(self, db_record):
        if db_record.media_type == MediaType.AUDIO.name:
            return True
        else:
            return False


    def get_duration(self, filename):
        # ffprobe -sexagesimal -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 audio.wav
        timecode_pattern = r'^([\d]{1,2}:[\d]{2}:[\d]{2})(\.[\d]{6})'
        file_path = os.path.join(self.media_path, filename)
        result = subprocess.run(['ffprobe', '-sexagesimal', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', file_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        result_string = result.stdout.decode('utf8').strip()
        duration_match = re.match(timecode_pattern, result_string)
        if duration_match:
            # TODO: remove this ugly hack and let CTimecode handle extra digits
            millis = duration_match.group(2)
            millis = round(float(millis), 3)
            if str(millis)[0:1] == '1':
                millis = '0.9'
            else:
                millis = str(millis)[1:]
            duration = CTimecode(f'{duration_match.group(1)}{millis}')
            # when CTimecode works let this handle extra digits
            #duration = CTimecode(duration_match.group())
            return duration
        else:
            raise NotTimeCodeError('ffprobe output does not match timecode format')

    def create_video_thubnail(self, filename, duration):
        # ffmpeg -y -hide_banner -loglevel warning -i input.mov -vf "scale=240:-1" -vframes 1 out.png
        file_path = self.get_file_path(filename)
        thumbnail_file_path = self.get_thumbnail_path(filename)
        if duration is None:
            result = subprocess.run(['ffmpeg', '-y', '-hide_banner', '-loglevel', 'warning', '-i', file_path, '-vf', f'scale={str(THUMBNAIL_W)}:-1', '-vframes', '1', thumbnail_file_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        else:
            time_option = "-ss"
            timecode = f'200ms'
            result = subprocess.run(['ffmpeg', time_option, timecode, '-y', '-hide_banner', '-loglevel', 'warning', '-i', file_path, '-vf', f'scale={str(THUMBNAIL_W)}:-1', '-vframes', '1', thumbnail_file_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        
        if os.path.exists(thumbnail_file_path):
            return thumbnail_file_path   



    def create_audio_thubnail(self, filename, duration):
        # audiowaveform -i sample.wav -o sample.dat -b 8
        file_path = self.get_file_path(filename)
        thumbnail_file_path = self.get_thumbnail_path(filename)
        #TODO: support 24-bit data
        result = subprocess.run(['audiowaveform', '-i', file_path, '-o', thumbnail_file_path, '-e', str(duration.milliseconds/1000), '-w', str(THUMBNAIL_W), '-h', str(THUMBNAIL_H), '--no-axis-labels', '--amplitude-scale', '0.9'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        
        if os.path.exists(thumbnail_file_path):
            return thumbnail_file_path

    def create_audio_waveform(self, filename):
        # audiowaveform -i sample.wav -o sample.dat -b 8
        file_path = self.get_file_path(filename)
        waveform_file_path = self.get_waveform_path(filename)
        result = subprocess.run(['audiowaveform', '-i', file_path, '-o', waveform_file_path, '-b', '8'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        if os.path.exists(waveform_file_path):
            return waveform_file_path

    def get_file_path(self, filename, trash_state=False):
        if trash_state is False:
            return os.path.join(self.media_path, filename)
        else:
            return os.path.join(self.trash_path, filename)

    def get_thumbnail_filename(self, filename):
        name_root, file_extension = os.path.splitext(filename)
        thumbnail_file_name = f'{name_root}_{file_extension[1:]}{THUMBNAIL_EXTENSION}'
        return thumbnail_file_name

    def get_thumbnail_path(self, filename, trash_state=False):
        thumbnail_file_name = self.get_thumbnail_filename(filename)
        if trash_state is False:
            thumbnail_file_path = os.path.join(self.thumbnail_path, thumbnail_file_name)
        else:
            thumbnail_file_path = os.path.join(self.thumbnail_trash_path, thumbnail_file_name)
        return thumbnail_file_path

    def get_waveform_filename(self, filename):
        name_root, file_extension = os.path.splitext(filename)
        waveform_file_name = f'{name_root}_{file_extension[1:]}{WAVEFORM_EXTENSION}'
        return waveform_file_name

    def get_waveform_path(self, filename, trash_state=False):
        waveform_file_name = self.get_waveform_filename(filename)
        if trash_state is False:
            waveform_file_path = os.path.join(self.waveform_path, waveform_file_name)
        else:
            waveform_file_path = os.path.join(self.waveform_trash_path, waveform_file_name)
        return waveform_file_path

    def add_binary_header(self, binary_data, uuid_string, type_number):
        # 36 bytes; first 36 positions, char = uuid 

        return struct.pack('<36s', str.encode(uuid_string)) + binary_data