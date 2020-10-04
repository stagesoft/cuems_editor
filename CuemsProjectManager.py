from peewee import *
from datetime import datetime
import time
import uuid
import os
import shutil
import json

from ..log import *
from .CuemsUtils import StringSanitizer, MoveVersioned, CuemsLibraryMaintenance, LIBRARY_PATH
from .CuemsErrors import *
from .. import DictParser
from .. import CuemsParser
from ..XmlBuilder import XmlBuilder
from .. import XmlReader, XmlWriter


pewee_logger = logging.getLogger('peewee')

pewee_logger.setLevel(logging.INFO)
pewee_logger.addHandler(handler)


db = SqliteDatabase(os.path.join(LIBRARY_PATH, 'project-manager.db')) # TODO: get filename from settings ?


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

    def medias(self):
        return (Media
                .select(fn.COUNT(ProjectMedia.id).alias('count'))
                .join(ProjectMedia, on=ProjectMedia.media)
                .where(ProjectMedia.project == self)
                .order_by(Media.created))

class Project_Trash(BaseModel):
    uuid = UUIDField(index = True, unique = True, primary_key = True)
    name = CharField( null = True )
    unix_name = CharField(unique = True)
    created = DateTimeField()
    modified = DateTimeField()


class Media(BaseModel):
    uuid = UUIDField(index = True, unique = True, primary_key = True)
    name = CharField( null = True )
    unix_name = CharField(unique = True)
    created = DateTimeField()
    modified = DateTimeField()

    def projects(self):
        return (Project
                .select(fn.COUNT(ProjectMedia.id).alias('count'))
                .join(ProjectMedia, on=ProjectMedia.project)
                .where(ProjectMedia.media == self)
                .order_by(Project.created))

    def orphan(self):
        return (Media
                .select()
                .join(ProjectMedia, JOIN.LEFT_OUTER)
                .where(ProjectMedia.media == None)
                .order_by(Media.created))

class Media_Trash(BaseModel):
    uuid = UUIDField(index = True, unique = True, primary_key = True)
    name = CharField( null = True )
    unix_name = CharField(unique = True)
    created = DateTimeField()
    modified = DateTimeField()

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
                dest_filename = MoveVersioned.move(tmp_file_path, CuemsMedia.media_path, filename)
                Media.create(uuid=uuid.uuid1(), unix_name=dest_filename, created=now_formated(), modified=now_formated())
            except Exception as e:
                logger.error("error: {} {} triying to move new file, rolling back database insert".format(type(e), e))
                transaction.rollback()
                if os.path.exists(os.path.join(CuemsMedia.media_path, dest_filename)):
                    os.remove(os.path.join(CuemsMedia.media_path, dest_filename))
                raise e

    @staticmethod
    def list():
        media_list = list()

        medias = (Media
         .select(Media.uuid, Media.name, Media.unix_name, Media.created, Media.modified, fn.COUNT(ProjectMedia.id).alias('count'))
         .join(ProjectMedia, JOIN.LEFT_OUTER)  # Joins tweet -> favorite.
         .join(Project, JOIN.LEFT_OUTER, on=(Project.uuid==ProjectMedia.project))  # Joins user -> tweet.
         .group_by(Media.uuid))
        for media in medias:
            media_dict = {str(media.uuid): {'name': media.name, 'unix_name': media.unix_name, 'created': media.created, 'modified': media.modified, "in_projects": media.count} }
            media_list.append(media_dict)

        return media_list

    @staticmethod
    def list_trash():
        media_list_trash = list()
        medias = Media_Trash.select()
        for media in medias:
            media_dict = {str(media.uuid): {'name': media.name, 'unix_name': media.unix_name, 'created': media.created, 'modified': media.modified} }
            media_list_trash.append(media_dict)

        return media_list_trash

    @staticmethod
    def save(uuid, data):   #TODO: check uuid format
        try:
            media = Media.get(Media.uuid==uuid)
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
    def delete(uuid):
        try:
            media = Media.get(Media.uuid==uuid)
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

        with db.atomic() as transaction:
            try:
                file_path = os.path.join(CuemsMedia.media_path, media.unix_name)
                dest_filename = MoveVersioned.move(file_path, CuemsMedia.trash_path, media.unix_name)
                Media_Trash.create(uuid=media.uuid, name=media.name, unix_name=dest_filename, created=media.created, modified=now_formated())
                media.delete_instance(recursive=True)
                logger.debug('deleting instance from table: {}'.format(media))
            except Exception as e:
                logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                transaction.rollback()
                if os.path.exists(os.path.join(CuemsMedia.trash_path, dest_filename)):
                    shutil.move( os.path.join(CuemsMedia.trash_path, dest_filename), os.path.join(CuemsMedia.media_path, media.unix_name))
                raise e

    @staticmethod
    def restore(uuid):
        try:
            media_trash = Media_Trash.get(Media_Trash.uuid==uuid)
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))
        
        with db.atomic() as transaction:
            try:
                file_path = os.path.join(CuemsMedia.trash_path, media_trash.unix_name)
                dest_filename = MoveVersioned.move(file_path, CuemsMedia.media_path, media_trash.unix_name)
                Media.create(uuid=media_trash.uuid, name=media_trash.name, unix_name=dest_filename, created=media_trash.created, modified=now_formated())
                media_trash.delete_instance()
                logger.debug('deleting instance from table: {}'.format(media_trash))
            except Exception as e:
                logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                transaction.rollback()
                if os.path.exists(os.path.join(CuemsMedia.media_path, dest_filename)):
                    shutil.move( os.path.join(CuemsMedia.media_path, dest_filename), os.path.join(CuemsMedia.trash_path, media_trash.unix_name))
                raise e

    @staticmethod
    def delete_from_trash(uuid):
        try:
            media = Media_Trash.get(Media_Trash.uuid==uuid)
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

        with db.atomic() as transaction:
            try:
                file_path = os.path.join(CuemsMedia.trash_path, media.unix_name)
                media.delete_instance()
                os.remove(file_path)
                logger.debug('deleting media from trash: {}'.format(media))
            except Exception as e:
                logger.error("error: {} {}; triying to delete file from trash, rolling back database".format(type(e), e))
                transaction.rollback()
                raise e


class CuemsProject(StringSanitizer):
    
    projects_path = os.path.join(LIBRARY_PATH, 'projects')
    trash_path = os.path.join(LIBRARY_PATH, 'trash', 'projects')



    @staticmethod
    def load(uuid):
        try:
            project = Project.get(Project.uuid==uuid)
            return CuemsProject.load_xml(project.unix_name)
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

    @staticmethod
    def list():
        project_list = list()

        projects = (Project
         .select(Project.uuid, Project.name, Project.unix_name, Project.created, Project.modified, fn.COUNT(ProjectMedia.id).alias('count'))
         .join(ProjectMedia, JOIN.LEFT_OUTER)  # Joins tweet -> favorite.
         .join(Media, JOIN.LEFT_OUTER, on=(Media.uuid==ProjectMedia.media))  # Joins user -> tweet.
         .group_by(Project.uuid))

        for project in projects:
            project_dict = {str(project.uuid): {'name': project.name, 'unix_name': project.unix_name, 'created': project.created, 'modified': project.modified, 'files_used': project.count} }
            for project_media in project.project_medias:
                project_list.append(project_dict)

        return project_list
    
    @staticmethod
    def list_trash():
        project_trash_list = list()
        projects_trash = Project_Trash.select()
        for project in projects_trash:
            project_dict = {str(project.uuid): {'name': project.name, 'unix_name': project.unix_name, 'created': project.created, 'modified': project.modified} }
            project_trash_list.append(project_dict)

        return project_trash_list

    @staticmethod
    def save(uuid, data):   #TODO: check uuid format
        try:
            project = Project.get(Project.uuid==uuid)
            with db.atomic() as transaction:
                try:
                    project.name=data['CuemsScript']['name']
                    project.modified=now_formated()
                    project.save()
                    ProjectMedia.delete().where(ProjectMedia.project == project).execute() #TODO: this could be optimized, now it deletes al file references, re-scans the script and ads referenced files to accout for changes. Would be posible to comprare, delete missing ones and and new ones.

                    project_object = CuemsProject.parse_and_add_media_relations(project, data)
                    
                    CuemsProject.save_xml(project.unix_name, project_object)
                    return 'updated'
                except Exception as e:
                    logger.error("error: {} {} triying to update  project, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    raise e
            
        except DoesNotExist:
            logger.debug('project uuid not in DB, saving as new')
            return CuemsProject.new( uuid, data)

    @staticmethod
    def new(uuid, data):
        unix_name = StringSanitizer.sanitize(data['CuemsScript']['name'])
        with db.atomic() as transaction:
            try:
                project = Project.create(uuid=uuid, unix_name=unix_name, name=data['CuemsScript']['name'], created=now_formated(), modified=now_formated())
                os.mkdir(os.path.join(CuemsProject.projects_path, unix_name))
                project_object = CuemsProject.parse_and_add_media_relations(project, data)

                CuemsProject.save_xml(unix_name, project_object)
                return 'new'
            except Exception as e:
                logger.error("error: {} {} ;triying to make new  project, rolling back database insert".format(type(e), e))
                if os.path.exists(os.path.join(CuemsProject.projects_path, unix_name)):
                    shutil.rmtree(os.path.join(CuemsProject.projects_path, unix_name) )                
                transaction.rollback()
                raise e

    @staticmethod
    def delete(uuid):
        try:
            project = Project.get(Project.uuid==uuid)
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

        with db.atomic() as transaction:
            try:
                file_path = os.path.join(CuemsProject.projects_path, project.unix_name)
                dest_filename = MoveVersioned.move(file_path, CuemsProject.trash_path, project.unix_name)
                Project_Trash.create(uuid=project.uuid, name=project.name, unix_name=dest_filename, created=project.created, modified=now_formated())
                project.delete_instance(recursive=True)
                logger.debug('deleting instance from table: {}'.format(project))
            except Exception as e:
                logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                transaction.rollback()
                if os.path.exists(os.path.join(CuemsProject.trash_path, dest_filename)):
                    shutil.move( os.path.join(CuemsProject.trash_path, dest_filename), os.path.join(CuemsProject.projects_path, project.unix_name))
                raise e
    
    @staticmethod
    def restore(uuid):
        try:
            project_trash = Project_Trash.get(Project_Trash.uuid==uuid)
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))
        
        with db.atomic() as transaction:
            try:
                project_path = os.path.join(CuemsProject.trash_path, project_trash.unix_name)
                dest_filename = MoveVersioned.move(project_path, CuemsProject.projects_path, project_trash.unix_name)
                Project.create(uuid=project_trash.uuid, name=project_trash.name, unix_name=dest_filename, created=project_trash.created, modified=now_formated())
                project_trash.delete_instance()
                project= Project.get(Project.uuid==uuid)

                data = CuemsProject.load_xml(project.unix_name)

                project_object = CuemsProject.parse_and_add_media_relations(project, data)
                

                logger.debug('deleting instance from table: {}'.format(project_trash))
            except Exception as e:
                logger.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                transaction.rollback()
                if os.path.exists(os.path.join(CuemsProject.projects_path, dest_filename)):
                    shutil.move( os.path.join(CuemsProject.projects_path, dest_filename), os.path.join(CuemsProject.trash_path, project_path.unix_name))
                raise e

    @staticmethod
    def delete_from_trash(uuid):
        try:
            project = Project_Trash.get(Project_Trash.uuid==uuid)
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exit".format(uuid))

        with db.atomic() as transaction:
            try:
                project_path = os.path.join(CuemsProject.trash_path, project.unix_name)
                project.delete_instance()
                shutil.rmtree(project_path)  #non empty dir, must use rmtree
                logger.debug('deleting project from trash: {}'.format(project))
            except Exception as e:
                logger.error("error: {} {}; triying to delete project to trash, rolling back database".format(type(e), e))
                transaction.rollback()
                raise e

    @staticmethod
    def parse_and_add_media_relations(project, data):
        project_object = CuemsParser(data).parse()
        media_list = project_object.get_media()
        for media_name, value in media_list.items():
            media = Media.get(Media.unix_name==media_name)
            ProjectMedia.create( project=project, media=media)    

        return project_object
    
    @staticmethod
    def save_xml(unix_name, project_object):

        writer = XmlWriter(schema = '/home/ion/src/cuems/python/osc-control/src/cuems/cues.xsd', xmlfile = (os.path.join(CuemsProject.projects_path, unix_name, 'script.xml')))
        writer.write_from_object(project_object)


    @staticmethod
    def load_xml(unix_name):
        reader = XmlReader(schema = '/home/ion/src/cuems/python/osc-control/src/cuems/cues.xsd', xmlfile = (os.path.join(CuemsProject.projects_path, unix_name, 'script.xml')))
        return reader.read()
        

CuemsLibraryMaintenance.check_dir_hierarchy()
db.create_tables([Project, Project_Trash, Media, Media_Trash, ProjectMedia])