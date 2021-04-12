from peewee import *
import os
from random import randint


import traceback


from .CuemsDBMedia import CuemsDBMedia
from .CuemsDBProject import CuemsDBProject
from .CuemsDBModel import Project, Media, ProjectMedia, database
from .CuemsErrors import *
from ..log import *



pewee_logger = logging.getLogger('peewee')

pewee_logger.setLevel(logging.INFO)



SCRIPT_SCHEMA_FILE_PATH = '/etc/cuems/script.xsd' #TODO: get all this constants from config?




class CuemsDBManager():
    
    def __init__(self,settings_dict):

        try:
            self.library_path = settings_dict['library_path']
            self.db_name = settings_dict['database_name']
            self.tmp_upload_path = settings_dict['tmp_upload_path']
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
        self.media = CuemsDBMedia(self.library_path, self.tmp_upload_path, database)
 




