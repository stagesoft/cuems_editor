from peewee import *
import time
import uuid as uuid_module
import os
import shutil
import json
import datetime
import traceback


from .CuemsDBModel import * # imports Media (DB model)
from .CuemsUtils import StringSanitizer, CopyMoveVersioned, CuemsLibraryMaintenance, date_now_iso_utc
from .CuemsErrors import *
from ..log import *
from ..DictParser import CuemsParser # do not import Media (File class) TODO: change this? name conflict Media (DB model) and Media ( Cue class)
from ..XmlBuilder import XmlBuilder
from ..XmlReaderWriter import XmlReader, XmlWriter


pewee_logger = logging.getLogger('peewee')

pewee_logger.setLevel(logging.INFO)



SCRIPT_SCHEMA_FILE_PATH = '/etc/cuems/script.xsd' #TODO: get all this constants from config?

SCRIPT_FILE_NAME = 'script.xml'
PROJECT_FOLDER_NAME = 'projects'
MEDIA_FOLDER_NAME = 'media'
TRASH_FOLDER_NAME = 'trash'




class CuemsDBManager():
    
    def __init__(self,settings_dict):

        try:
            self.library_path = settings_dict['library_path']
            self.db_name = settings_dict['database_name']
        except KeyError as e:
            logger.error(f'can not read settings {e}')
            raise e

        self.xsd_path = SCRIPT_SCHEMA_FILE_PATH
        self.db_path = os.path.join(self.library_path, self.db_name)
        self.models = [Project, Media,  ProjectMedia]
        database.init(self.db_path)
        database.connect()
        logger.debug(f'database connected {database}, {self.db_name}')
        for model in self.models:
            if database.table_exists(model._meta.table): # pylint: disable=maybe-no-member
                continue
            else:
                logger.warning(f'table "{model._meta.table_name	}" does not exist, creating') # pylint: disable=maybe-no-member
        # safe=True uses IF NOT EXIST on table create
        database.create_tables( self.models, safe=True) 
        self.project = CuemsDBProject(self.library_path, self.xsd_path, database)
        self.media = CuemsDBMedia(self.library_path, database)

class CuemsDBMedia(StringSanitizer):

    def __init__(self, library_path, db_connection):
        self.library_path = library_path
        self.db = db_connection
        self.media_path = os.path.join(self.library_path, MEDIA_FOLDER_NAME)
        self.trash_path = os.path.join(self.library_path, TRASH_FOLDER_NAME, MEDIA_FOLDER_NAME)
    
    def new(self, tmp_file_path, filename):
        with self.db.atomic() as transaction:
            try:
                dest_filename = None
                dest_filename = CopyMoveVersioned.move(tmp_file_path, self.media_path, filename)
                Media.create(uuid=uuid_module.uuid1(), name=dest_filename, unix_name=dest_filename, created=date_now_iso_utc(), modified=date_now_iso_utc(), in_trash=False)
            except Exception as e:
                logger.error("error: {} {} triying to move new file, rolling back database insert".format(type(e), e))
                transaction.rollback()
                if dest_filename is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                if os.path.exists(os.path.join(self.media_path, dest_filename)):
                    os.remove(os.path.join(self.media_path, dest_filename))
                raise e

    def list(self):
        media_list = list()

        medias = (Media
         .select(Media.uuid, Media.name, Media.unix_name, Media.created, Media.modified, 
         fn.COUNT(Case(Project.in_trash, (('0', 1),), None)).alias('in_project_count'),
         fn.COUNT(Case(Project.in_trash, (('1', 1),), None)).alias('in_project_trash_count'))
         .join(ProjectMedia, JOIN.LEFT_OUTER)  # Joins tweet -> favorite.
         .join(Project, JOIN.LEFT_OUTER, on=(Project.uuid==ProjectMedia.project))  # Joins user -> tweet.
         .where(Media.in_trash==False)
         .group_by(Media.uuid))
        for media in medias:
            media_dict = {str(media.uuid): {'name': media.name, 'unix_name': media.unix_name, 'created': media.created, 'modified': media.modified, "in_projects": media.in_project_count, "in_trash_projects" : media.in_project_trash_count} }
            media_list.append(media_dict)

        return media_list

    def list_trash(self):
        media_list = list()

        medias = (Media
         .select(Media.uuid, Media.name, Media.unix_name, Media.created, Media.modified,
         fn.COUNT(Case(Project.in_trash, (('0', 1),), None)).alias('in_project_count'),
         fn.COUNT(Case(Project.in_trash, (('1', 1),), None)).alias('in_project_trash_count'))
         .join(ProjectMedia, JOIN.LEFT_OUTER)  # Joins tweet -> favorite.
         .join(Project, JOIN.LEFT_OUTER, on=(Project.uuid==ProjectMedia.project))  # Joins user -> tweet.
         .where(Media.in_trash==True)
         .group_by(Media.uuid))
        for media in medias:
            media_dict = {str(media.uuid): {'name': media.name, 'unix_name': media.unix_name, 'created': media.created, 'modified': media.modified, "in_projects": media.in_project_count, "in_trash_projects" : media.in_project_trash_count} }
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
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    def load_meta(self, uuid):
        try:
            media = Media.get(Media.uuid==uuid)
            file_meta = dict()
            project_dict = dict()
            project_trash_dict = dict()
            media_projects_query = media.projects()
            for project in media_projects_query:
                if project.in_trash == False :
                    project_dict[str(project.uuid)] = project.unix_name
                else:
                    project_trash_dict[str(project.uuid)] = project.unix_name

            file_meta[uuid] = { 'name': media.name, 'unix_name': media.unix_name, 'description': media.description, 'created': media.created, 'modified': media.modified, 'in_trash': media.in_trash, 'in_projects' : project_dict, 'in_trash_projects' : project_trash_dict }
            return file_meta
            
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

        
    def delete(self, uuid):
        try:
            media = Media.get((Media.uuid==uuid) & (Media.in_trash == False))
        

            with self.db.atomic() as transaction:
                try:
                    dest_filename = None
                    file_path = os.path.join(self.media_path, media.unix_name)
                    dest_filename = CopyMoveVersioned.move(file_path, self.trash_path, media.unix_name)
                    media.in_trash = True
                    media.save()
                    logger.debug('deleting instance from table: {}'.format(media))
                except Exception as e:
                    logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.trash_path, dest_filename)):
                        shutil.move( os.path.join(self.trash_path, dest_filename), os.path.join(self.media_path, media.unix_name))
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    def restore(self, uuid):
        try:
            media_trash = Media.get((Media.uuid==uuid) & (Media.in_trash == True))
        
            with self.db.atomic() as transaction:
                try:
                    dest_filename = None
                    file_path = os.path.join(self.trash_path, media_trash.unix_name)
                    dest_filename = CopyMoveVersioned.move(file_path, self.media_path, media_trash.unix_name)
                    media_trash.in_trash = False
                    media_trash.save()
                    logger.debug('deleting instance from table: {}'.format(media_trash))
                except Exception as e:
                    logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.media_path, dest_filename)):
                        shutil.move( os.path.join(self.media_path, dest_filename), os.path.join(self.trash_path, media_trash.unix_name))
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    def delete_from_trash(self, uuid):
        try:
            media = Media.get((Media.uuid==uuid) & (Media.in_trash == True))

            with self.db.atomic() as transaction:
                try:
                    file_path = os.path.join(self.trash_path, media.unix_name)
                    media.delete_instance(recursive=True)
                    os.remove(file_path)
                    logger.debug('deleting media from trash: {}'.format(media))
                except Exception as e:
                    logger.error("error: {} {}; triying to delete file from trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))


class CuemsDBProject(StringSanitizer):

    def __init__(self, library_path, xsd_path, db_connection):
        self.library_path = library_path
        self.xsd_path = xsd_path
        self.db = db_connection
        self.projects_path = os.path.join(self.library_path, PROJECT_FOLDER_NAME)
        self.trash_path = os.path.join(self.library_path, TRASH_FOLDER_NAME, PROJECT_FOLDER_NAME)
    
    
    
    
    def load(self, uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            return self.load_xml(project.unix_name)
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    def list(self):
        project_list = list()
        projects = Project.select().where(Project.in_trash == False)
        for project in projects:
            project_dict = {str(project.uuid): {'name': project.name, 'unix_name': project.unix_name, 'created': project.created, 'modified': project.modified} }
            project_list.append(project_dict)

        return project_list
    
    def list_trash(self):
        project_trash_list = list()
        projects_trash = Project.select().where(Project.in_trash == True)
        for project in projects_trash:
            project_dict = {str(project.uuid): {'name': project.name, 'unix_name': project.unix_name, 'created': project.created, 'modified': project.modified} }
            project_trash_list.append(project_dict)

        return project_trash_list

    def update(self, uuid, data):   #TODO: check uuid format
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            with self.db.atomic() as transaction:
                try:
                    project.name=StringSanitizer.sanitize_name(data['CuemsScript']['name'])
                    now = date_now_iso_utc()
                    data['CuemsScript']['modified'] = now
                    project.modified=now
                    project.description=StringSanitizer.sanitize_text_size(data['CuemsScript']['description'])
                    project.save()
                    project_object = CuemsParser(data).parse()
                    self.update_media_relations(project, project_object, data)
                    self.save_xml(project.unix_name, project_object)
                except Exception as e:
                    logger.error(traceback.format_exc()) # TODO: clean, only for debug
                    logger.error("error: {} {} triying to update  project, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    raise e
            
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    def new(self, data):
        try:
            unix_name = StringSanitizer.sanitize_dir_permit_increment(data['CuemsScript']['unix_name'])
        except KeyError as e:
            raise e
        
        project_uuid = str(uuid_module.uuid1())
        data['CuemsScript']['uuid']= project_uuid
        now = date_now_iso_utc()
        data['CuemsScript']['created'] = now
        data['CuemsScript']['modified'] = now
        with self.db.atomic() as transaction:
            try:
                project = Project.create(uuid=project_uuid, unix_name=unix_name, name=StringSanitizer.sanitize_name(data['CuemsScript']['name']), description=StringSanitizer.sanitize_text_size(data['CuemsScript']['description']), created=now, modified=now)
                os.mkdir(os.path.join(self.projects_path, unix_name))
                project_object = CuemsParser(data).parse()
                self.add_media_relations(project, project_object, data)
                self.save_xml(unix_name, project_object)
                return project_uuid
            except IntegrityError as e:
                transaction.rollback()
                logger.error("error: {} {} ;triying to make new  project, rolling back database insert".format(type(e), e))
                raise e
            except Exception as e:
                transaction.rollback()
                logger.error(traceback.format_exc()) # TODO: clean, only for debug
                logger.error("error: {} {} ;triying to make new  project, rolling back database insert".format(type(e), e))
                
                if os.path.exists(os.path.join(self.projects_path, unix_name)):
                    shutil.rmtree(os.path.join(self.projects_path, unix_name) )
                             
                raise e

    def duplicate(self, uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            with self.db.atomic() as transaction:
                try:
                    new_unix_name = None
                    project_path = os.path.join(self.projects_path, project.unix_name)
                    new_unix_name = CopyMoveVersioned.copy_dir(project_path, self.projects_path, project.unix_name)
                    project.unix_name = new_unix_name
                    new_uuid = str(uuid_module.uuid1())
                    project.uuid = new_uuid
                    project.name = project.name + ' - Copy'
                    project.modified=date_now_iso_utc()
                    project.save(force_insert=True)

                    dup_project= Project.get(Project.uuid==new_uuid)
                    data = self.load_xml(dup_project.unix_name)
                    project_object = CuemsParser(data).parse()
                    self.add_media_relations(dup_project, project_object, data)
                    return new_uuid
                except Exception as e:
                    logger.error("error: {} {}; triying to duplicate  project, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    if new_unix_name is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.projects_path, new_unix_name)):
                        shutil.rmtree(os.path.join(self.projects_path, new_unix_name))
                    raise e
            
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    def delete(self, uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            with self.db.atomic() as transaction:
                try:
                    dest_filename = None
                    file_path = os.path.join(self.projects_path, project.unix_name)
                    dest_filename = CopyMoveVersioned.move(file_path, self.trash_path, project.unix_name)
                    project.in_trash = True
                    project.save()
                    logger.debug('deleting instance from table: {}'.format(project))
                except Exception as e:
                    logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.trash_path, dest_filename)):
                        shutil.move( os.path.join(self.trash_path, dest_filename), os.path.join(self.projects_path, project.unix_name))
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))
    
    def restore(self, uuid):
        try:
            project_trash = Project.get((Project.uuid==uuid) & (Project.in_trash == True))
        
            with self.db.atomic() as transaction:
                try:
                    dest_filename = None
                    project_path = os.path.join(self.trash_path, project_trash.unix_name)
                    dest_filename = CopyMoveVersioned.move(project_path, self.projects_path, project_trash.unix_name)
                    project_trash.in_trash = False
                    project_trash.save()
                    logger.debug('deleting instance from table: {}'.format(project_trash))
                except Exception as e:
                    logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.projects_path, dest_filename)):
                        shutil.move( os.path.join(self.projects_path, dest_filename), os.path.join(self.trash_path, project_path.unix_name))
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    def delete_from_trash(self, uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == True))

            with self.db.atomic() as transaction:
                try:
                    project_path = os.path.join(self.trash_path, project.unix_name)
                    project.delete_instance(recursive=True)
                    shutil.rmtree(project_path)  #non empty dir, must use rmtree
                    logger.debug('deleting project from trash: {}'.format(project))
                except Exception as e:
                    logger.error("error: {} {}; triying to delete project to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    def add_media_relations(self, project, project_object, data):
        media_dict = project_object.get_media()
        for media_name, value in media_dict.items():
            media = Media.get(Media.unix_name==media_name)
            ProjectMedia.create( project=project, media=media)    
    
    def update_media_relations(self, project, project_object, data):
        old_media_query = project.medias()
        old_media_dict = dict()
        for media in old_media_query:
            old_media_dict[media.unix_name] = str(media.uuid)
        old_media_list=list(old_media_dict.keys())

        
        
        media_dict = project_object.get_media()
        media_list =list(media_dict.keys())
        
        remove_set = set(old_media_list).difference(media_list)
        add_set = set(media_list).difference(old_media_list)

        logging.debug('media remove list: {}'.format(remove_set))
        logging.debug('media add list: {}'.format(add_set))

        if remove_set:
            for media_unix_name in remove_set:
                ProjectMedia.delete().where((ProjectMedia.project == project)&(ProjectMedia.media == old_media_dict[media_unix_name] )).execute() 

        if add_set:
            for media_unix_name in add_set:
                media = Media.select(Media.uuid).where(Media.unix_name==media_unix_name).get()
                ProjectMedia.create( project=project, media=media)

    
    def save_xml(self, unix_name, project_object):

        writer = XmlWriter(schema = self.xsd_path, xmlfile = (os.path.join(self.projects_path, unix_name, SCRIPT_FILE_NAME)))
        writer.write_from_object(project_object)


    def load_xml(self, unix_name):
        reader = XmlReader(schema = self.xsd_path, xmlfile = (os.path.join(self.projects_path, unix_name, SCRIPT_FILE_NAME)))
        return reader.read()

    def check_uuid_uniqueness(self, project_object):
        uuid_list = []
        uuid_list.append(project_object.uuid)

        uuid_list.append(project_object.cuelist.uuid)

            

        



