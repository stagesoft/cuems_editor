from peewee import *
from datetime import time, datetime
import uuid

import logging
logger = logging.getLogger('peewee')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

db = SqliteDatabase('project-manager.db')

def now_formated():
    return datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

class BaseModel(Model):
    class Meta:
        database = db

class Project(BaseModel):
    uuid = UUIDField(index = True, unique = True, primary_key = True)
    name = CharField()
    unix_name = CharField( null = True )
    created = DateTimeField()
    modified = DateTimeField()

    def medias(self):
        return (Media
                .select()
                .join(ProjectMedia, on=ProjectMedia.media)
                .where(ProjectMedia.project == self)
                .order_by(Media.name))


class Media(BaseModel):
    uuid = UUIDField(index = True, unique = True, primary_key = True)
    name = CharField()
    unix_name = CharField( null = True )
    created = DateTimeField()
    modified = DateTimeField()

    def projects(self):
        return (Project
                .select()
                .join(ProjectMedia, on=ProjectMedia.project)
                .where(ProjectMedia.media == self)
                .order_by(Project.name)) 

class ProjectMedia(BaseModel):
    project = ForeignKeyField(Project, backref='project_medias')
    media = ForeignKeyField(Media, backref='media_projects')







def populate_db():
    db.create_tables([Project, Media, ProjectMedia])

    p1 = Project.create(uuid=uuid.uuid1(), name='Proyecto 1', created=now_formated(), modified=now_formated())
    p2 = Project.create(uuid=uuid.uuid1(), name='Proyecto 2', created=now_formated(), modified=now_formated())
    p3 = Project.create(uuid=uuid.uuid1(), name='Proyecto 3', created=now_formated(), modified=now_formated())

    m1 = Media.create(uuid=uuid.uuid1(), name='Media 1', created=now_formated(), modified=now_formated())
    m2 = Media.create(uuid=uuid.uuid1(), name='Media 2', created=now_formated(), modified=now_formated())
    m3 = Media.create(uuid=uuid.uuid1(), name='Media 3', created=now_formated(), modified=now_formated())

    ProjectMedia.create( project=p1, media=m1)
    ProjectMedia.create( project=p1, media=m2)
    ProjectMedia.create( project=p1, media=m3)

    ProjectMedia.create( project=p2, media=m1)
    ProjectMedia.create( project=p2, media=m2)

    ProjectMedia.create( project=p3, media=m3)

#p1.save(force_insert = True)

db.connect()

print('Proyectos:')
print('')
for project in Project.select():
    print(project.name)
    medias = (Media
            .select()
            .where(Media.uuid.in_(project.medias()))
            .order_by(Media.name.desc()))
    for media in medias:
        print(media.name)

print('Medias:')
print('')
for media in Media.select():
    print(media.name)
    projects = (Project
            .select()
            .where(Project.uuid.in_(media.projects()))
            .order_by(Project.name.desc()))
    for project in projects:
        print(project.name)

