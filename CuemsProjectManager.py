from peewee import *
from datetime import datetime
import time
import uuid as uuid_module
import os
import shutil
import json

from ..log import *
from .CuemsUtils import StringSanitizer, CopyMoveVersioned, CuemsLibraryMaintenance, LIBRARY_PATH
from .CuemsErrors import *
from .. import DictParser
from .. import CuemsParser
from ..XmlBuilder import XmlBuilder
from .. import XmlReader, XmlWriter


pewee_logger = logging.getLogger('peewee')

pewee_logger.setLevel(logging.DEBUG)
pewee_logger.addHandler(handler)


db = SqliteDatabase(os.path.join(LIBRARY_PATH, 'project-manager.db'), pragmas={'foreign_keys': 1}) # TODO: get filename from settings ?


def now_formated():
    return datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

class BaseModel(Model):
    class Meta:
        database = db

class Project(BaseModel):
    uuid = UUIDField(index = True, unique = True, primary_key = True)
    name = CharField( null = True )
    unix_name = CharField(unique = True)
    created = DateTimeField()
    modified = DateTimeField()
    in_trash = BooleanField(default=False)

    @staticmethod
    def all_fields():
        return [Project.uuid, Project.name, Project.unix_name, Project.created, Project.modified, Project.in_trash]


    def medias(self):
        return (Media
                .select( *Media.all_fields(), fn.COUNT(ProjectMedia.id).alias('count'))
                .join(ProjectMedia, on=ProjectMedia.media)
                .where(ProjectMedia.project == self)
                .order_by(Media.created)
                .group_by(Media.uuid))




class Media(BaseModel):
    uuid = UUIDField(index = True, unique = True, primary_key = True)
    name = CharField( null = True )
    unix_name = CharField(unique = True)
    created = DateTimeField()
    modified = DateTimeField()
    in_trash = BooleanField(default=False)

    @staticmethod
    def all_fields():
        return [Media.uuid, Media.name, Media.unix_name, Media.created, Media.modified, Media.in_trash]

    def projects(self):
        return (Project
                .select( *Project.all_fields(), fn.COUNT(ProjectMedia.id).alias('count'))
                .join(ProjectMedia, on=ProjectMedia.project)
                .where(ProjectMedia.media == self)
                .order_by(Project.created)
                .group_by(Project.uuid))

    def orphan(self):
        return (Media
                .select()
                .join(ProjectMedia, JOIN.LEFT_OUTER)
                .where(ProjectMedia.media == None)
                .order_by(Media.created))



class ProjectMedia(BaseModel):
    id = PrimaryKeyField()
    project = ForeignKeyField(Project, backref='project_medias')
    media = ForeignKeyField(Media, backref='media_projects')

    def missing_refs(self):
        return (ProjectMedia
                .select()
                .join(Media, JOIN.LEFT_OUTER, on=(Media.uuid==ProjectMedia.media))
                .join(Project, JOIN.LEFT_OUTER, on=(Project.uuid==ProjectMedia.project))
                .where(
                    (Media.uuid == None) |
                    (Project.uuid == None))
                .order_by(Media.created))




class CuemsMedia(StringSanitizer):
    
    media_path = os.path.join(LIBRARY_PATH, 'media')     #TODO: get upload folder path from settings?
    trash_path = os.path.join(LIBRARY_PATH, 'trash', 'media')
    
    
    @staticmethod
    def new(tmp_file_path, filename):
        
        with db.atomic() as transaction:
            try:
                dest_filename = None
                dest_filename = CopyMoveVersioned.move(tmp_file_path, CuemsMedia.media_path, filename)
                Media.create(uuid=uuid_module.uuid1(), unix_name=dest_filename, created=now_formated(), modified=now_formated(), in_trash=False)
            except Exception as e:
                logger.error("error: {} {} triying to move new file, rolling back database insert".format(type(e), e))
                transaction.rollback()
                if dest_filename is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                if os.path.exists(os.path.join(CuemsMedia.media_path, dest_filename)):
                    os.remove(os.path.join(CuemsMedia.media_path, dest_filename))
                raise e

    @staticmethod
    def list():
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

    @staticmethod
    def list_trash():
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

    @staticmethod
    def save(uuid, data):   #TODO: check uuid format
        try:
            media = Media.get((Media.uuid==uuid) & (Media.in_trash == False))
            with db.atomic() as transaction:
                try:
                    media.update(name=data['uuid']['name'], modified=now_formated()).execute()
                    return 'updated'
                except Exception as e:
                    logger.error("error: {} {} triying to update  media data, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    raise e
            
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    @staticmethod
    def load_meta(uuid):
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

            file_meta[uuid] = { 'name': media.name, 'unix_name': media.unix_name, 'created': media.created, 'modified': media.modified, 'in_trash': media.in_trash, 'in_projects' : project_dict, 'in_trash_projects' : project_trash_dict }
            return file_meta
            
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

        
    @staticmethod
    def delete(uuid):
        try:
            media = Media.get((Media.uuid==uuid) & (Media.in_trash == False))
        

            with db.atomic() as transaction:
                try:
                    dest_filename = None
                    file_path = os.path.join(CuemsMedia.media_path, media.unix_name)
                    dest_filename = CopyMoveVersioned.move(file_path, CuemsMedia.trash_path, media.unix_name)
                    media.in_trash = True
                    media.save()
                    logger.debug('deleting instance from table: {}'.format(media))
                except Exception as e:
                    logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(CuemsMedia.trash_path, dest_filename)):
                        shutil.move( os.path.join(CuemsMedia.trash_path, dest_filename), os.path.join(CuemsMedia.media_path, media.unix_name))
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    @staticmethod
    def restore(uuid):
        try:
            media_trash = Media.get((Media.uuid==uuid) & (Media.in_trash == True))
        
            with db.atomic() as transaction:
                try:
                    dest_filename = None
                    file_path = os.path.join(CuemsMedia.trash_path, media_trash.unix_name)
                    dest_filename = CopyMoveVersioned.move(file_path, CuemsMedia.media_path, media_trash.unix_name)
                    media_trash.in_trash = False
                    media_trash.save()
                    logger.debug('deleting instance from table: {}'.format(media_trash))
                except Exception as e:
                    logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(CuemsMedia.media_path, dest_filename)):
                        shutil.move( os.path.join(CuemsMedia.media_path, dest_filename), os.path.join(CuemsMedia.trash_path, media_trash.unix_name))
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    @staticmethod
    def delete_from_trash(uuid):
        try:
            media = Media.get((Media.uuid==uuid) & (Media.in_trash == True))

            with db.atomic() as transaction:
                try:
                    file_path = os.path.join(CuemsMedia.trash_path, media.unix_name)
                    media.delete_instance(recursive=True)
                    os.remove(file_path)
                    logger.debug('deleting media from trash: {}'.format(media))
                except Exception as e:
                    logger.error("error: {} {}; triying to delete file from trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))


class CuemsProject(StringSanitizer):
    
    projects_path = os.path.join(LIBRARY_PATH, 'projects')
    trash_path = os.path.join(LIBRARY_PATH, 'trash', 'projects')



    @staticmethod
    def load(uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            print(project)
            print(project.in_trash)
            return CuemsProject.load_xml(project.unix_name)
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    @staticmethod
    def list():
        project_list = list()
        projects = Project.select().where(Project.in_trash == False)
        for project in projects:
            project_dict = {str(project.uuid): {'name': project.name, 'unix_name': project.unix_name, 'created': project.created, 'modified': project.modified} }
            project_list.append(project_dict)

        return project_list
    
    @staticmethod
    def list_trash():
        project_trash_list = list()
        projects_trash = Project.select().where(Project.in_trash == True)
        for project in projects_trash:
            project_dict = {str(project.uuid): {'name': project.name, 'unix_name': project.unix_name, 'created': project.created, 'modified': project.modified} }
            project_trash_list.append(project_dict)

        return project_trash_list

    @staticmethod
    def update(uuid, data):   #TODO: check uuid format
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            with db.atomic() as transaction:
                try:
                    project.name=data['CuemsScript']['name']
                    now = now_formated()
                    data['CuemsScript']['modified'] = now
                    project.modified=now
                    project.save()
                    project_object = CuemsParser(data).parse()
                    CuemsProject.update_media_relations(project, project_object, data)
                    CuemsProject.save_xml(project.unix_name, project_object)
                except Exception as e:
                    logger.error("error: {} {} triying to update  project, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    raise e
            
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    @staticmethod
    def new(data):
        try:
            unix_name = StringSanitizer.sanitize_dir_permit_increment(data['CuemsScript']['unix_name'])
        except KeyError:
            unix_name = StringSanitizer.sanitize_dir_name(data['CuemsScript']['name'])
        
        project_uuid = str(uuid_module.uuid1())
        data['CuemsScript']['uuid']= project_uuid
        now = now_formated()
        data['CuemsScript']['created'] = now
        data['CuemsScript']['modified'] = now
        with db.atomic() as transaction:
            try:
                project = Project.create(uuid=project_uuid, unix_name=unix_name, name=data['CuemsScript']['name'], created=now, modified=now)
                os.mkdir(os.path.join(CuemsProject.projects_path, unix_name))
                project_object = CuemsParser(data).parse()
                CuemsProject.add_media_relations(project, project_object, data)
                CuemsProject.save_xml(unix_name, project_object)
                return project_uuid
            except Exception as e:
                logger.error("error: {} {} ;triying to make new  project, rolling back database insert".format(type(e), e))
                transaction.rollback()
                if os.path.exists(os.path.join(CuemsProject.projects_path, unix_name)):
                    shutil.rmtree(os.path.join(CuemsProject.projects_path, unix_name) )                
                raise e

    @staticmethod
    def duplicate(uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            with db.atomic() as transaction:
                try:
                    new_unix_name = None
                    project_path = os.path.join(CuemsProject.projects_path, project.unix_name)
                    new_unix_name = CopyMoveVersioned.copy_dir(project_path, CuemsProject.projects_path, project.unix_name)
                    project.unix_name = new_unix_name
                    new_uuid = str(uuid_module.uuid1())
                    project.uuid = new_uuid
                    project.name = project.name + ' - Copy'
                    project.modified=now_formated()
                    project.save(force_insert=True)

                    dup_project= Project.get(Project.uuid==new_uuid)
                    data = CuemsProject.load_xml(dup_project.unix_name)
                    project_object = CuemsParser(data).parse()
                    CuemsProject.add_media_relations(dup_project, project_object, data)
                    return new_uuid
                except Exception as e:
                    logger.error("error: {} {}; triying to duplicate  project, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    if new_unix_name is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(CuemsProject.projects_path, new_unix_name)):
                        shutil.rmtree(os.path.join(CuemsProject.projects_path, new_unix_name))
                    raise e
            
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    @staticmethod
    def delete(uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            with db.atomic() as transaction:
                try:
                    dest_filename = None
                    file_path = os.path.join(CuemsProject.projects_path, project.unix_name)
                    dest_filename = CopyMoveVersioned.move(file_path, CuemsProject.trash_path, project.unix_name)
                    project.in_trash = True
                    project.save()
                    logger.debug('deleting instance from table: {}'.format(project))
                except Exception as e:
                    logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(CuemsProject.trash_path, dest_filename)):
                        shutil.move( os.path.join(CuemsProject.trash_path, dest_filename), os.path.join(CuemsProject.projects_path, project.unix_name))
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))
    
    @staticmethod
    def restore(uuid):
        try:
            project_trash = Project.get((Project.uuid==uuid) & (Project.in_trash == True))
        
            with db.atomic() as transaction:
                try:
                    dest_filename = None
                    project_path = os.path.join(CuemsProject.trash_path, project_trash.unix_name)
                    dest_filename = CopyMoveVersioned.move(project_path, CuemsProject.projects_path, project_trash.unix_name)
                    project_trash.in_trash = False
                    project_trash.save()
                    logger.debug('deleting instance from table: {}'.format(project_trash))
                except Exception as e:
                    logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not sucessfull with dont need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(CuemsProject.projects_path, dest_filename)):
                        shutil.move( os.path.join(CuemsProject.projects_path, dest_filename), os.path.join(CuemsProject.trash_path, project_path.unix_name))
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    @staticmethod
    def delete_from_trash(uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == True))

            with db.atomic() as transaction:
                try:
                    project_path = os.path.join(CuemsProject.trash_path, project.unix_name)
                    project.delete_instance(recursive=True)
                    shutil.rmtree(project_path)  #non empty dir, must use rmtree
                    logger.debug('deleting project from trash: {}'.format(project))
                except Exception as e:
                    logger.error("error: {} {}; triying to delete project to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    @staticmethod
    def add_media_relations(project, project_object, data):
        media_dict = project_object.get_media()
        print(media_dict)
        for media_name, value in media_dict.items():
            media = Media.get(Media.unix_name==media_name)
            ProjectMedia.create( project=project, media=media)    
    
    @staticmethod
    def update_media_relations(project, project_object, data):
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

    
    @staticmethod
    def save_xml(unix_name, project_object):

        writer = XmlWriter(schema = '/home/ion/src/cuems/python/osc-control/src/cuems/cues.xsd', xmlfile = (os.path.join(CuemsProject.projects_path, unix_name, 'script.xml')))
        writer.write_from_object(project_object)


    @staticmethod
    def load_xml(unix_name):
        reader = XmlReader(schema = '/home/ion/src/cuems/python/osc-control/src/cuems/cues.xsd', xmlfile = (os.path.join(CuemsProject.projects_path, unix_name, 'script.xml')))
        return reader.read()
        

CuemsLibraryMaintenance.check_dir_hierarchy()
db.create_tables([Project, Media,  ProjectMedia])

