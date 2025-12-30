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
    
    def __init__(self,settings_dict):

        self.settings_dict = settings_dict

        try:
            self.db_name = settings_dict['database_name']
            self.library_path = settings_dict['library_path']
        except KeyError as e:
            Logger.error(f'can not read settings {e}')
            raise 

        self.db_path = os.path.join(self.library_path, self.db_name)
        self.models = [Project, Media,  ProjectMedia]
        database.init(self.db_path)
        database.connect()
        Logger.debug(f'database connected {database}, {self.db_name}')
        for model in self.models:
            if database.table_exists(model._meta.table): # pylint: disable=maybe-no-member
                continue
            else:
                Logger.warning(f'table "{model._meta.table_name	}" does not exist, creating') # pylint: disable=maybe-no-member
        # safe=True uses IF NOT EXIST on table create
        database.create_tables( self.models, safe=True) 
        self.project = CuemsDBProject(self.settings_dict, database)
        self.media = CuemsDBMedia(self.settings_dict, database)
 




