import os
import traceback
import shutil
from peewee import DoesNotExist, IntegrityError

from cuemsutils.StringSanitizer import StringSanitizer
from cuemsutils.CopyMoveVersioned import CopyMoveVersioned
from cuemsutils.xml.Parsers import CuemsParser
from cuemsutils.xml.XmlReaderWriter import XmlReaderWriter
from cuemsutils.helpers import new_datetime, new_uuid
from cuemsutils.log import logged, Logger


from CuemsErrors import *
from CuemsDBModel import Project, Media, ProjectMedia

SCRIPT_FILE_NAME = 'script.xml'
PROJECT_FOLDER_NAME = 'projects'
TRASH_FOLDER_NAME = 'trash'


class CuemsDBProject(StringSanitizer):

    def __init__(self, library_path, xsd_path, db_connection):
        self.library_path = library_path
        self.xsd_path = xsd_path
        self.db = db_connection
        self.projects_path = os.path.join(self.library_path, PROJECT_FOLDER_NAME)
        self.trash_path = os.path.join(self.library_path, TRASH_FOLDER_NAME, PROJECT_FOLDER_NAME)
    
    
    def get_project_unix_name(self, uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            return project.unix_name
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))
        
    def load(self, uuid, include_trash=False):
        try:
            if not include_trash:
                project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            else:
                project = Project.get(Project.uuid==uuid)
            return self.load_xml(project.unix_name)
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

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
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

        try:
            del data['CuemsScript']['unix_name']
        except KeyError:
            pass

        with self.db.atomic() as transaction:
            try:
                project.name=StringSanitizer.sanitize_name(data['CuemsScript']['name'])
                now = new_datetime()
                data['CuemsScript']['modified'] = now
                project.modified=now
                project.description=StringSanitizer.sanitize_text_size(data['CuemsScript']['description'])
                project.save()
                project_object = CuemsParser(data).parse()
                self.update_media_relations(project, project_object)
                self.save_xml(project.unix_name, project_object)
            except Exception as e:
                Logger.error("error: {} {} triying to update  project, rolling back database update".format(type(e), e))
                transaction.rollback()
                raise e
            
        

    def new(self, data, unix_name):

        try:
            unix_name = StringSanitizer.sanitize_dir_permit_increment(unix_name)
        except Exception as e:
            raise e
        
        try:
            project_uuid = str(new_uuid())
            data['CuemsScript']['id']= project_uuid
            now = new_datetime()
            data['CuemsScript']['created'] = now
            data['CuemsScript']['modified'] = now
        except KeyError as e:
            Logger.error("error: Missing {} ;triying to make new  project, rolling back database insert".format(e))
            raise e
        except Exception as e:
            Logger.error("error: {} {} ;triying to read  project data".format(type(e), e))
            raise e

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
                Logger.error("error: {} {} ;name or unix_name allready exists, rolling back database insert".format(type(e), e))
                raise e
            except Exception as e:
                transaction.rollback()
                Logger.error("error: {} {} ;triying to make new  project, rolling back database insert".format(type(e), e))
                
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
                    new_uuid = str(new_uuid())
                    project.uuid = new_uuid
                    project.name = project.name + ' - Copy'
                    project.modified=new_datetime()
                    project.save(force_insert=True)

                    dup_project= Project.get(Project.uuid==new_uuid)
                    data = self.load_xml(dup_project.unix_name)
                    project_object = CuemsParser(data).parse()
                    self.add_media_relations(dup_project, project_object)
                    return new_uuid
                except Exception as e:
                    Logger.error("error: {} {}; triying to duplicate  project, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    if new_unix_name is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.projects_path, new_unix_name)):
                        shutil.rmtree(os.path.join(self.projects_path, new_unix_name))
                    raise e
            
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

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
                    Logger.debug('updating instance in db: {}'.format(project))
                except Exception as e:
                    Logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.trash_path, dest_filename)):
                        shutil.move( os.path.join(self.trash_path, dest_filename), os.path.join(self.projects_path, project.unix_name))
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))
    
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
                    Logger.debug('updating instance in db: {}'.format(project_trash))
                except Exception as e:
                    Logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.projects_path, dest_filename)):
                        shutil.move( os.path.join(self.projects_path, dest_filename), os.path.join(self.trash_path, project_path.unix_name))
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def delete_from_trash(self, uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == True))

            with self.db.atomic() as transaction:
                try:
                    project_path = os.path.join(self.trash_path, project.unix_name)
                    project.delete_instance(recursive=True)
                    shutil.rmtree(project_path)  #non empty dir, must use rmtree
                    Logger.debug('deleting project from trash: {}'.format(project))
                except Exception as e:
                    Logger.error("error: {} {}; triying to delete project to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def add_media_relations(self, project, project_object):
        media_filenames_list = project_object.get_media_filenames()
        for media_name in media_filenames_list:
            media = Media.get(Media.unix_name==media_name)
            ProjectMedia.create( project=project, media=media, media_filename=media_name)    
    
    def update_media_relations(self, project, project_object):
        Logger.debug('updating media relations for project: {}'.format(project.unix_name))
        old_media_query = project.medias()
        old_media_dict = dict()
        Logger.debug('query done')
        for media in old_media_query:
            old_media_dict[media.unix_name] = str(media.uuid)
        old_media_list=list(old_media_dict.keys())
        Logger.debug('old media list: {}'.format(old_media_list))
        media_list = project_object.get_media_filenames()
        Logger.debug('media list: {}'.format(media_list))
         
        remove_set = set(old_media_list).difference(media_list)
        add_set = set(media_list).difference(old_media_list)

        Logger.debug('media remove list: {}'.format(remove_set))
        Logger.debug('media add list: {}'.format(add_set))

        if remove_set:
            for media_unix_name in remove_set:
                ProjectMedia.delete().where((ProjectMedia.project == project)&(ProjectMedia.media == old_media_dict[media_unix_name] )).execute() 

        if add_set:
            for media_unix_name in add_set:
                media = Media.select(Media.uuid).where(Media.unix_name==media_unix_name).get()
                ProjectMedia.create( project=project, media=media, media_filename=media_unix_name)  

    
    def save_xml(self, unix_name, project_object):

        writer = XmlReaderWriter(schema_name = self.xsd_path, xmlfile = (os.path.join(self.projects_path, unix_name, SCRIPT_FILE_NAME)))
        writer.write_from_object(project_object)


    def load_xml(self, unix_name):
        reader = XmlReaderWriter(schema_name = self.xsd_path, xmlfile = (os.path.join(self.projects_path, unix_name, SCRIPT_FILE_NAME)))
        return reader.read()

            

        



