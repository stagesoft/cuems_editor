from peewee import *

from .CuemsUtils import date_now_iso_utc

database = SqliteDatabase(None, pragmas={
    'foreign_keys': 1,
    'ignore_check_constraints': 0})


# TODO: discuss this; WAL mode is faster  but creates 3 files instead of 1, does not need synchonous=2 to mantain database integrity
""" database = SqliteDatabase(None, pragmas={
    'journal_mode': 'wal',
    'cache_size': -1 * 4000,  # 4MB
    'foreign_keys': 1,
    'ignore_check_constraints': 0,
    'synchronous': 1}) """


class CuemsBaseModel(Model):
    class Meta:
        database = database

class Project(CuemsBaseModel):
    uuid = UUIDField(index = True, unique = True, primary_key = True)
    name = CharField(unique = True)
    unix_name = CharField(unique = True)
    description = TextField(null = True) #TODO: define maxsize
    created = DateTimeField(default=date_now_iso_utc())
    modified = DateTimeField(default=date_now_iso_utc())
    in_trash = BooleanField(default=False)

    @staticmethod
    def all_fields():
        return [Project.uuid, Project.name, Project.unix_name, Project.description, Project.created, Project.modified, Project.in_trash]


    def medias(self):
        return (Media
                .select( *Media.all_fields(), fn.COUNT(ProjectMedia.id).alias('count'))
                .join(ProjectMedia, on=ProjectMedia.media)
                .where(ProjectMedia.project == self)
                .order_by(Media.created)
                .group_by(Media.uuid))




class Media(CuemsBaseModel):
    uuid = UUIDField(index = True, unique = True, primary_key = True)
    name = CharField(unique = True)
    unix_name = CharField(unique = True)
    description = TextField(null = True) #TODO: define maxsize
    created = DateTimeField(default=date_now_iso_utc())
    modified = DateTimeField(default=date_now_iso_utc())
    duration = CharField(null = True)
    media_type = CharField()
    in_trash = BooleanField(default=False)

    @staticmethod
    def all_fields():
        return [Media.uuid, Media.name, Media.unix_name, Media.description, Media.created, Media.modified, Media.duration, Media.media_type, Media.in_trash]

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



class ProjectMedia(CuemsBaseModel):
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

