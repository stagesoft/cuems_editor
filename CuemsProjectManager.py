from peewee import *
from datetime import datetime
import time
import uuid
import os
import shutil

import logging
from .CuemsUtils import StringSanitizer, MoveVersioned, LIBRARY_PATH
from .. import DictParser

logger = logging.getLogger('peewee')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


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
                .select()
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
                .select()
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


db.create_tables([Project, Project_Trash, Media, Media_Trash, ProjectMedia])

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
                logging.error("error: {} {} triying to move new file, rolling back database insert".format(type(e), e))
                transaction.rollback()
                raise e

    @staticmethod
    def list():
        media_list = list()
        medias = Media.select()
        for media in medias:
            media_dict = {str(media.uuid): {'name': media.name, 'unix_name': media.unix_name, 'created': media.created, 'modified': media.modified} }
            media_list.append(media_dict)

        return media_list

    @staticmethod
    def delete(uuid):
        with db.atomic() as transaction:
            try:
                media = Media.get(Media.uuid==uuid)
                file_path = os.path.join(CuemsMedia.media_path, media.unix_name)
                dest_filename = MoveVersioned.move(file_path, CuemsMedia.trash_path, media.unix_name)
                Media_Trash.create(uuid=media.uuid, name=media.name, unix_name=dest_filename, created=media.created, modified=now_formated())
                media.delete_instance()
                logging.debug('deleting instance from table: {}'.format(media))
                
            except Exception as e:
                logging.error("error: {} {}; triying to move file to trash, rolling back database".format(type(e), e))
                transaction.rollback()
                raise e

class CuemsProject(StringSanitizer):
    
    projects_path = os.path.join(LIBRARY_PATH, 'projects')

    @staticmethod
    def list():
        project_list = list()
        projects = Project.select()
        for project in projects:
            project_dict = {str(project.uuid): {'name': project.name, 'unix_name': project.unix_name, 'created': project.created, 'modified': project.modified} }
            project_list.append(project_dict)

        return project_list

    @staticmethod
    def save(uuid, data):
        try:
            project = Project.get(Project.uuid==uuid)
            with db.atomic() as transaction:
                try:
                    project.update(name=data['CuemsScript']['name'], modified=now_formated()).execute()
                    return 'updated'
                except Exception as e:
                    logging.error("error: {} {} triying to update  project, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    raise e
            
        except DoesNotExist:
            logging.debug('project uuid not in DB, saving as new')
            return CuemsProject.new(uuid, data)

    @staticmethod
    def new(uuid, data):
        unix_name = StringSanitizer.sanitize(data['CuemsScript']['name'])
        with db.atomic() as transaction:
            try:
                Project.create(uuid=uuid, unix_name=unix_name, name=data['CuemsScript']['name'], created=now_formated(), modified=now_formated())
                os.mkdir(os.path.join(CuemsProject.projects_path, unix_name))
                
                print(data)


                with open(os.path.join(CuemsProject.projects_path, unix_name, 'script.xml'), 'w') as f:
                    f.write('bla')
                return 'new'
            except Exception as e:
                logging.error("error: {} {} triying to make new  project, rolling back database insert".format(type(e), e))
                transaction.rollback()
                raise e

    @staticmethod
    def delete(uuid):
        project = Project.get(Project.uuid==uuid)
        with db.atomic() as transaction:
            try:
                shutil.rmtree(os.path.join(CuemsProject.projects_path, project.unix_name))
                project.delete_instance()
                logging.debug('deleted project {}, uuid:{}'.format(os.path.join(CuemsProject.projects_path, project.unix_name), uuid))
            except Exception as e:
                logging.error("error: {} {} triying to delete project directory, rolling back database delete".format(type(e), e))
                transaction.rollback()
                raise e
