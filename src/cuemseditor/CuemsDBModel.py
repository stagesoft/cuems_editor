from peewee import *

from cuemsutils.helpers import new_datetime

database = SqliteDatabase(None, pragmas={
    'foreign_keys': 1,
    'ignore_check_constraints': 0})


# TODO: discuss this; WAL mode is faster  but creates 3 files instead of 1, does not need synchonous=2 to mantain database integrity
# database = SqliteDatabase(None, pragmas={
#     'journal_mode': 'wal',
#     'cache_size': -1 * 4000,  # 4MB
#     'foreign_keys': 1,
#     'ignore_check_constraints': 0,
#     'synchronous': 1})


class CuemsBaseModel(Model):
    """Peewee base model that binds all CueMS ORM classes to the shared database.

    All concrete models inherit from this class so they all operate on the
    same ``SqliteDatabase`` instance that ``CuemsDBManager`` initialises at
    startup via ``database.init(path)``.
    """

    class Meta:
        database = database


class Project(CuemsBaseModel):
    """ORM model for a CueMS project record.

    Each row corresponds to one project directory under
    ``library_path/projects/<unix_name>/``. The ``in_trash`` flag moves the
    project to the trash rather than hard-deleting it, preserving the DB
    record and unique-name constraints.

    Attributes:
        uuid: Primary key; also the ``CuemsScript.id`` in the project XML.
        name: Human-readable display name (unique, sanitized).
        unix_name: Filesystem-safe directory name (unique, sanitized).
        description: Free-text description; may be ``None``.
        created: Creation timestamp.
        modified: Last-save timestamp.
        in_trash: ``True`` when the project has been soft-deleted.
    """

    uuid = UUIDField(index=True, unique=True, primary_key=True)
    name = CharField(unique=True)
    unix_name = CharField(unique=True)
    description = TextField(null=True)  #TODO: define maxsize
    created = DateTimeField(default=new_datetime())
    modified = DateTimeField(default=new_datetime())
    in_trash = BooleanField(default=False)

    @staticmethod
    def all_fields():
        """Return the full column list for use in explicit SELECT clauses.

        Returns:
            List of Peewee field descriptors covering every column on
            ``Project``.
        """
        return [Project.uuid, Project.name, Project.unix_name, Project.description, Project.created, Project.modified, Project.in_trash]

    def medias(self):
        """Query all ``Media`` records linked to this project.

        Joins through ``ProjectMedia`` and returns each media file with a
        ``count`` alias (number of ``ProjectMedia`` rows for the pair).

        Returns:
            Peewee ``ModelSelect`` ordered by ``Media.created``.
        """
        return (Media
                .select(*Media.all_fields(), fn.COUNT(ProjectMedia.id).alias('count'))
                .join(ProjectMedia, on=ProjectMedia.media)
                .where(ProjectMedia.project == self)
                .order_by(Media.created)
                .group_by(Media.uuid))


class Media(CuemsBaseModel):
    """ORM model for a media file record.

    Each row represents one file in ``library_path/media/``.  The
    ``in_trash`` flag soft-deletes the file to ``library_path/trash/media/``,
    preserving unique-name constraints on ``name`` and ``unix_name``.

    Attributes:
        uuid: Primary key; used as the media identifier in cue XML.
        name: Human-readable filename (unique, sanitized).
        unix_name: Filesystem filename (unique, sanitized).
        description: Optional free-text description.
        created: Upload timestamp.
        modified: Last metadata-update timestamp.
        duration: ``CTimecode`` string (e.g. ``"00:03:12.450"``); ``None``
            for images.
        media_type: ``"MOVIE"``, ``"AUDIO"``, or ``"IMAGE"`` (from
            ``MediaType.name``).
        in_trash: ``True`` when the file has been soft-deleted.
    """

    uuid = UUIDField(index=True, unique=True, primary_key=True)
    name = CharField(unique=True)
    unix_name = CharField(unique=True)
    description = TextField(null=True)  #TODO: define maxsize
    created = DateTimeField(default=new_datetime())
    modified = DateTimeField(default=new_datetime())
    duration = CharField(null=True)
    media_type = CharField()
    in_trash = BooleanField(default=False)

    @staticmethod
    def all_fields():
        """Return the full column list for use in explicit SELECT clauses.

        Returns:
            List of Peewee field descriptors covering every column on
            ``Media``.
        """
        return [Media.uuid, Media.name, Media.unix_name, Media.description, Media.created, Media.modified, Media.duration, Media.media_type, Media.in_trash]

    def projects(self):
        """Query all ``Project`` records that reference this media file.

        Returns:
            Peewee ``ModelSelect`` ordered by ``Project.created``, with a
            ``count`` alias for the number of ``ProjectMedia`` join rows.
        """
        return (Project
                .select(*Project.all_fields(), fn.COUNT(ProjectMedia.id).alias('count'))
                .join(ProjectMedia, on=ProjectMedia.project)
                .where(ProjectMedia.media == self)
                .order_by(Project.created)
                .group_by(Project.uuid))

    def orphan(self):
        """Query ``Media`` records that are not referenced by any project.

        Returns:
            Peewee ``ModelSelect`` of orphaned media rows ordered by
            ``Media.created``.
        """
        return (Media
                .select()
                .join(ProjectMedia, JOIN.LEFT_OUTER)
                .where(ProjectMedia.media == None)
                .order_by(Media.created))


class ProjectMedia(CuemsBaseModel):
    """Many-to-many join table linking projects to their media files.

    ``media_filename`` is a denormalized copy of ``Media.unix_name`` kept
    alongside the foreign key so that queries can match on filename without
    an extra join to ``Media``.

    Attributes:
        id: Auto-increment surrogate primary key.
        project: FK to ``Project``.
        media: FK to ``Media``; nullable to allow orphan detection.
        media_filename: Denormalized ``Media.unix_name`` for fast lookups.
    """

    id = PrimaryKeyField()
    project = ForeignKeyField(Project, backref='project_medias')
    media = ForeignKeyField(Media, backref='media_projects', null=True)
    media_filename = CharField(null=True)  # This is a denormalized field to speed up queries

    def missing_refs(self):
        """Query ``ProjectMedia`` rows whose ``Media`` or ``Project`` FK is dangling.

        Returns:
            Peewee ``ModelSelect`` of rows where the referenced ``Media`` or
            ``Project`` record no longer exists.
        """
        return (ProjectMedia
                .select()
                .join(Media, JOIN.LEFT_OUTER, on=(Media.uuid == ProjectMedia.media))
                .join(Project, JOIN.LEFT_OUTER, on=(Project.uuid == ProjectMedia.project))
                .where(
                    (Media.uuid == None) |
                    (Project.uuid == None))
                .order_by(Media.created))
