import asyncio
import concurrent.futures
import json
import os
import websockets as ws
from websockets.asyncio.server import serve
from multiprocessing import Process
import signal
from random import randint  #TODO: clean unused
from hashlib import md5
import uuid as uuid_module
import re

from cuemsutils.log import logged, Logger

from create_dummy import create_dummy_script


from CuemsProjectManager import CuemsDBManager
from CuemsWsUser import CuemsWsUser
from CuemsUpload import CuemsUpload
from CuemsErrors import *

from cuemsutils.ComunicatorServices import Comunicator


class CuemsWsServer():
    
    def __init__(self, settings_dict, mappings_dict ):
        self.engine_comunicator = Comunicator(address="ipc:///tmp/test1.sock")  
        #self.engine_queue = Comunicator(address="ipc:///tmp/test2.sock")
        self.engine_messages = list()
        self.users = dict()
        self.sessions = dict()
        self.settings_dict = settings_dict
        self.mappings_dict = mappings_dict
        try:
            self.tmp_path = self.settings_dict['tmp_path']
            self.session_uuid = self.settings_dict['session_uuid']
            self.library_path = self.settings_dict['library_path']
        except KeyError as e:
            Logger.error(f'can not read settings {e}')
            raise e
        Logger.debug(f'library path set to : {self.library_path}')

        if (not os.path.exists(self.tmp_path)) or ( not os.access(self.tmp_path,  os.X_OK & os.R_OK & os.W_OK)):
            Logger.error("error: upload folder is not usable")
            raise FileNotFoundError('Can not access upload folder')


    def start(self, port):
        self.port = port
        self.host = 'localhost'
        newfeature = asyncio.get_event_loop().run_until_complete(self.run_async_server())
    
    async def run_async_server(self):
        self.db = CuemsDBManager(self.settings_dict)
        self.event_loop = asyncio.get_event_loop()
        
        self.executor =  concurrent.futures.ThreadPoolExecutor(thread_name_prefix='ws_ProjectManager_ThreadPoolExecutor', max_workers=5) # TODO: adjust max workers
        #self.event_loop.set_exception_handler(self.exception_handler) ### TODO:UNCOMENT FOR PRODUCTION 
        self.project_server = await serve(self.connection_handler, self.host, self.port) 
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.event_loop.add_signal_handler(sig, self.stop)
        Logger.info('server listening on {}, port {}'.format(self.host, self.port))
        await self.project_server.serve_forever()
        # self.event_loop.run_forever()
        # self.event_loop.close()
        

        
    def stop(self):
        #self.event_loop.call_soon_threadsafe(self.queue_task.cancel)
        self.event_loop.call_soon_threadsafe(self.project_server.close)
        Logger.info('ws server closing')
        asyncio.run_coroutine_threadsafe(self.stop_async(), self.event_loop)
              

    async def stop_async(self):
        await self.project_server.wait_closed()
        Logger.info('ws server closed')
        self.event_loop.call_soon(self.event_loop.stop)
        Logger.info('event loop stoped')
    



    async def connection_handler(self, websocket):
        Logger.info("new connection: {}, path: {}".format(websocket.remote_address, websocket.request.path))
        path = websocket.request.path
        if (path == '/' or path[0:9] == '/?session'):                                    # project manager
            await self.project_manager_session(websocket, path)
        elif path == '/upload':                            # file upload
            await self.upload_session(websocket)
        else:
            Logger.info("unknow path: {}".format(path))

    async def project_manager_session(self, websocket, path):
        user_session = CuemsWsUser(self, websocket)
        await self.register(user_session, path)
        await user_session.outgoing.put(self.initial_json_template())
        await user_session.outgoing.put(self.initial_setting_message())
        try:
            consumer_task = asyncio.create_task(user_session.consumer_handler())
            producer_task = asyncio.create_task(user_session.producer_handler())
            # start 3 message processing task so a load or any other time consuming action still leaves with 2 tasks running  and interface feels responsive. TODO:discuss this
            processor_tasks = [asyncio.create_task(user_session.consumer()) for _ in range(3)]
            
            done_tasks, pending_tasks = await asyncio.wait([consumer_task, producer_task, *processor_tasks], return_when=asyncio.FIRST_COMPLETED)
            for task in pending_tasks:
                task.cancel()

        finally:
            await self.unregister(user_session)

    async def upload_session(self, websocket):
        user_upload_session = CuemsUpload(self, websocket)
        Logger.info("new upload session: {}".format(user_upload_session))

        await user_upload_session.message_handler()
        Logger.info("upload session ended: {}".format(user_upload_session))

    async def register(self, user_session, path):
        Logger.info("user registered: {}".format(id(user_session.websocket)))
        self.users[user_session] = None
        await self.notify_users("users")
        user_session.session_id =  await self.check_session(user_session, path)
        await self.load_session(user_session)

    async def check_session(self, user_session, path):
        session_uuid_patern = r"/\?session=(?P<uuid>[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[1][0-9A-Fa-f]{3}-[89AB][0-9A-Fa-f]{3}-[0-9A-Fa-f]{12})?"
        matches = re.search(session_uuid_patern, path)
        if matches:
            if (matches.groupdict()['uuid'] != None):
                uuid = matches.groupdict()['uuid']
                if uuid  not in self.sessions:
                    Logger.debug(f"uuid not found {uuid}, creating new session")
                    uuid = str(uuid_module.uuid1())
                else:
                    Logger.debug(f"session_id found, reusing {uuid}")
            else:
                uuid = str(uuid_module.uuid1())
        else:
            uuid = str(uuid_module.uuid1())
        try:
            self.sessions[uuid]['ws']=id(user_session.websocket)
        except KeyError:
            self.sessions[uuid]= {'ws': id(user_session.websocket)}

        await self.notify_session(user_session, uuid)

        return uuid

    async def load_session(self, user_session):
        pass
        # try:
        #     await user_session.send_project(self.sessions[user_session.session_id]['loaded_project'], 'project_load')
        # except KeyError:
        #     pass
    
    async def notify_session(self, user_session, uuid):
        message = json.dumps({"type": "session_id", "value": uuid})
        await user_session.outgoing.put(message)

    async def unregister(self, user_task):
        Logger.info("user unregistered: {}".format(id(user_task.websocket)))
        self.users.pop(user_task, None)
        await self.notify_users("users")


    async def notify_others_list_changes(self, calling_user, list_type):
        if self.users:  #notify others, not the user trigering the action, and only if the have same project loaded
            message = json.dumps({"type": "list_update", "value": list_type})
            for user, project in self.users.items():
                if user is not calling_user:
                    await user.outgoing.put(message)
                    Logger.debug('notifing {} {}'.format(user, list_type))
            
    async def notify_others_same_project(self, calling_user, msg_type, project_uuid=None):
        if self.users:  #notify others, not the user trigering the action, and only if the have same project loaded
            message = json.dumps({"type" : "project_update", "value" : project_uuid})
            for user, project in self.users.items():
                if user is not calling_user:
                    if project_uuid is not None:
                        if str(project) != str(project_uuid):
                            continue
                    else:
                        if str(project) != str(self.users[calling_user]):
                            continue

                    Logger.debug('same project loaded')
                    await user.outgoing.put(message)
                    Logger.debug('notifing {}'.format(user))
    
    async def notify_users(self, type):
        if self.users:  # asyncio.wait doesn't accept an empty dcit
            message = self.users_event(type)
            for user in self.users:
                await user.outgoing.put(message)



    # warning, these non async functions should be not blocking or user @sync_to_async to get their own thread
  

    # send initial json template to the client
    def initial_json_template(self):
        initial_template = create_dummy_script()

        return json.dumps({"type": "initial_template", "value": {"CuemsScript": initial_template}}) 

  
    def initial_setting_message(self):
        return json.dumps({"type": "initial_mappings", "value": self.mappings_dict })


    def users_event(self, type, uuid=None):
        if type == "users":
            return json.dumps({"type": type, "value": len(self.users)})
        else:
            return json.dumps({"type": type, "uuid": uuid, "value" : "modified in server"}) # TODO: not used

    def exception_handler(self, loop, context):
        Logger.debug("Caught the following exception: (ignore if on closing)")
        Logger.debug(context['message'])