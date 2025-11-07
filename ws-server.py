

from cuemsutils.log import Logger, logged
from cuemsutils.tools.ConfigManager import ConfigManager
from cuemsutils.xml.Settings import ProjectMappings, NetworkMap
from CuemsWsServer import CuemsWsServer

from multiprocessing import Queue
import time
import uuid
import os
import json



settings_dict = {}
settings_dict['session_uuid'] = str(uuid.uuid1())
settings_dict['library_path'] = '/opt/cuems_library'
settings_dict['tmp_path'] = '/tmp/cuems'
settings_dict['database_name'] = 'project-manager.db'
settings_dict['project_folder_name'] = 'projects'
settings_dict['script_file_name'] = 'script.xml'
settings_dict['script_schema_name'] = 'script'
settings_dict['media_folder_name'] = 'media'
settings_dict['trash_folder_name'] = 'trash'
settings_dict['thumbnail_folder_name'] = 'thumbnails'
settings_dict['waveform_folder_name'] = 'waveforms'
settings_dict['thumbnail_extension'] = '.png'
settings_dict['waveform_extension'] = '.dat'
settings_dict['thumbnail_size'] = (240, 240)  # width, height
settings_dict['editor_ipc'] = '/tmp/editor.ipc'

cf_manager = ConfigManager(load_all=False)
settings_file = cf_manager.conf_path('default_mappings.xml')
project_mappings = ProjectMappings(settings_file)
print(json.dumps(project_mappings.get_dict()))
mappings_dict = project_mappings.get_dict()



try:
    if not os.path.exists(settings_dict['tmp_path']):
        os.mkdir(settings_dict['tmp_path'])
        Logger.info('creating tmp upload folder {}'.format(settings_dict['tmp_path']))
except Exception as e:
    print("error: {} {}".format(type(e), e))




server = CuemsWsServer(settings_dict, mappings_dict)
Logger.info('start server')
time.sleep(5)
server.start(9092)


#server.stop()
