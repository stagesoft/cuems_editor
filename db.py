from peewee import *
from datetime import datetime
import time
import uuid

import random
import logging

logger = logging.getLogger('peewee')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

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
                .order_by(Media.created))


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
                .order_by(Project.created))

    def orphan(self):
        return (Media
                .select()
                .join(ProjectMedia, JOIN.LEFT_OUTER)
                .where(ProjectMedia.media == None)
                .order_by(Media.created))

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








def populate_db():
    db.create_tables([Project, Media, ProjectMedia])

    p1 = Project.create(uuid=uuid.uuid1(), name='Proyecto 1', created=now_formated(), modified=now_formated())
    time.sleep(random.randrange(10))
    p2 = Project.create(uuid=uuid.uuid1(), name='Proyecto 2', created=now_formated(), modified=now_formated())
    time.sleep(random.randrange(10))
    p3 = Project.create(uuid=uuid.uuid1(), name='Proyecto 3', created=now_formated(), modified=now_formated())

    m1 = Media.create(uuid=uuid.uuid1(), name='Media 1', created=now_formated(), modified=now_formated())
    time.sleep(random.randrange(10))
    m2 = Media.create(uuid=uuid.uuid1(), name='Media 2', created=now_formated(), modified=now_formated())
    time.sleep(random.randrange(10))
    m3 = Media.create(uuid=uuid.uuid1(), name='Media 3', created=now_formated(), modified=now_formated())
    
    time.sleep(random.randrange(10))
    m4 = Media.create(uuid=uuid.uuid1(), name='Orphan 1', created=now_formated(), modified=now_formated())
    time.sleep(random.randrange(10))
    m5 = Media.create(uuid=uuid.uuid1(), name='Orphan 2', created=now_formated(), modified=now_formated())

    ProjectMedia.create( project=p1, media=m1)
    ProjectMedia.create( project=p1, media=m2)
    ProjectMedia.create( project=p1, media=m3)

    ProjectMedia.create( project=p2, media=m1)
    ProjectMedia.create( project=p2, media=m2)

    ProjectMedia.create( project=p3, media=m3)

def delete_missing_refs():
    mis = ProjectMedia().missing_refs()

    for ref in mis:
        ref.delete_instance()

####################################################

#delete_missing_refs()

#p1.save(force_insert = True)

db.connect()

#populate_db()

print('Proyectos:')
print('')
for project in Project.select():
    print('Nombre: {}, uudi: {}, Creado: {}'.format(project.name, project.uuid, project.created))
    print('Medias:')
    medias = (Media
            .select()
            .where(Media.uuid.in_(project.medias()))
            .order_by(Media.name.desc()))
    for media in medias:
        print('     Nombre: {}, uudi: {}, Creado: {}'.format(media.name, media.uuid, media.created))

print(' ')
print('------')
print(' ')
print('Medias:')
print('')
for media in Media.select():
    print('Nombre: {}, uudi: {}, Creado: {}'.format(media.name, media.uuid, media.created))
    print('Presente en:')
    projects = (Project
            .select()
            .where(Project.uuid.in_(media.projects()))
            .order_by(Project.name.desc()))
    for project in projects:
        print('     Nombre: {}, uudi: {}, Creado: {}'.format(project.name, project.uuid, project.created))





print('')
print('----')

ps = Project.select().where(Project.uuid == 'b3ea3df4-f39a-11ea-a7af-1c6f65465cae').get()

ps2 = Project.get(Project.uuid=='b8258bbc-f39a-11ea-a7af-1c6f65465cae')


md = ps.medias()

print(ps.name)
print(ps2.name)

print(list(md))


print('')
print('----')


mp = Media().orphan()

print(mp)

print(list(mp))

for m in mp:
    print(m.name)

print('')
print('----')

mis = ProjectMedia().missing_refs()

print(list(mis))



