from peewee import *
import os
from random import randint


import traceback


from cuemsutils.log import logged, Logger

from cuemseditor.CuemsDBMedia import CuemsDBMedia
from cuemseditor.CuemsDBProject import CuemsDBProject
from cuemseditor.CuemsDBModel import Project, Media, ProjectMedia, database
from cuemseditor.CuemsErrors import *


class CuemsDBManager():
    """Database facade that owns the SQLite connection, schema creation, and
    the ``project`` / ``media`` sub-managers.

    Initialise exactly once per process after ``CuemsLibraryMaintenance``
    has created the library directory tree.  The shared ``database``
    singleton is initialised and connected here; ``CuemsDBProject`` and
    ``CuemsDBMedia`` receive the same connection object.

    Attributes:
        project: ``CuemsDBProject`` instance for project CRUD operations.
        media: ``CuemsDBMedia`` instance for media CRUD operations.

    Example:
        >>> settings = {
        ...     "database_name": "cuems.db",
        ...     "library_path": "/var/lib/cuems/library",
        ...     "project_folder_name": "projects",
        ...     "media_folder_name": "media",
        ...     "trash_folder_name": "trash",
        ...     "tmp_path": "/tmp/cuems",
        ...     "script_file_name": "cue_script.xml",
        ...     "script_schema_name": "script.xsd",
        ...     "thumbnail_folder_name": "thumbnails",
        ...     "waveform_folder_name": "waveforms",
        ...     "thumbnail_extension": ".jpg",
        ...     "waveform_extension": ".json",
        ...     "thumbnail_size": [240, 120],
        ... }
        >>> db = CuemsDBManager(settings)
        >>> projects = db.project.list()
        >>> media_files = db.media.list()
    """

    def __init__(self, settings_dict):
        """Initialise the database connection and create missing tables.

        Calls ``database.init(path)`` to bind the shared Peewee
        ``SqliteDatabase`` to the file at
        ``settings_dict['library_path'] / settings_dict['database_name']``,
        then calls ``database.create_tables(..., safe=True)`` so first-run
        tables are created without dropping existing data.

        Args:
            settings_dict: Mapping consumed from ``settings.xml``; must
                contain at minimum ``database_name`` and ``library_path``.

        Raises:
            KeyError: If ``database_name`` or ``library_path`` is absent from
                *settings_dict*.
        """
        self.settings_dict = settings_dict

        try:
            self.db_name = settings_dict['database_name']
            self.library_path = settings_dict['library_path']
        except KeyError as e:
            Logger.error(f'can not read settings {e}')
            raise

        self.db_path = os.path.join(self.library_path, self.db_name)
        self.models = [Project, Media, ProjectMedia]
        database.init(self.db_path)
        database.connect()
        Logger.debug(f'database connected {database}, {self.db_name}')
        for model in self.models:
            if database.table_exists(model._meta.table):  # pylint: disable=maybe-no-member
                continue
            else:
                Logger.warning(f'table "{model._meta.table_name}" does not exist, creating')  # pylint: disable=maybe-no-member
        # safe=True uses IF NOT EXIST on table create
        database.create_tables(self.models, safe=True)
        self.project = CuemsDBProject(self.settings_dict, database)
        self.media = CuemsDBMedia(self.settings_dict, database)
