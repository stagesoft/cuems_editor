import sys
import asyncio
import concurrent.futures
import json
import os
import shutil
import aiofiles
import websockets as ws
from multiprocessing import Process
from queue import Empty
import signal
from random import randint
from hashlib import md5
import uuid as uuid_module
import re

import time

from ..log import *

from .CuemsProjectManager import CuemsDBManager, CuemsDBMedia, CuemsDBProject
from .CuemsErrors import *
from .CuemsUtils import StringSanitizer, CuemsLibraryMaintenance





formatter = logging.Formatter('Cuems:ws-server: %(levelname)s (PID: %(process)d)-%(threadName)-9s)-(%(funcName)s) %(message)s')


logger_ws_server = logging.getLogger('ws-server')
logger_ws_server.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger_ws_server.addHandler(handler)

logger_asyncio = logging.getLogger('asyncio')
logger_asyncio.setLevel(logging.INFO)  # asyncio debug level 
logger_asyncio.addHandler(handler)

logger_ws = logging.getLogger('websockets')
logger_ws.setLevel(logging.INFO)  # websockets debug level,  in debug prints all frames, also binary frames! 
logger_ws.addHandler(handler)

class CuemsWsServer():
    
    def __init__(self, _queue, settings_dict ):
        self.queue = _queue
        self.state = {"value": 0} #TODO: provisional
        self.users = dict()
        self.sessions = dict()
        self.tmp_upload_path = settings_dict['tmp_upload_path']
        self.session_uuid = settings_dict['session_uuid']
        self.library_path = settings_dict['library_path']
        logger.debug(f'library path set to : {self.library_path}')

        if (not os.path.exists(self.tmp_upload_path)) or ( not os.access(self.tmp_upload_path,  os.X_OK & os.R_OK & os.W_OK)):
            logger.error("error: upload folder is not usable")
            raise FileNotFoundError('Can not access upload folder')

        self.db = CuemsDBManager(settings_dict)

        

    def start(self, port):
        self.process = Process(target=self.run_async_server, args=(self.queue,))
        self.port = port
        self.host = 'localhost'
        self.process.start()

        

    def run_async_server(self, event):
        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)
        self.executor =  concurrent.futures.ThreadPoolExecutor(thread_name_prefix='ws_ProjectManager_ThreadPoolExecutor', max_workers=5) # TODO: adjust max workers
        #self.event_loop.set_exception_handler(self.exception_handler) ### TODO:UNCOMENT FOR PRODUCTION 
        self.project_server = ws.serve(self.connection_handler, self.host, self.port, max_size=None) #TODO: choose max packets size from ui and limit it here
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.event_loop.add_signal_handler(sig, self.ask_exit)
        logger.info('server listening on {}, port {}'.format(self.host, self.port))
        self.event_loop.run_until_complete(self.project_server)
    #    self.event_loop.create_task(self.queue_handler())
        self.event_loop.run_forever()
        self.event_loop.close()
        
    def stop(self):
        os.kill(self.process.pid, signal.SIGTERM)
        self.process.join()
        logger.info('ws process joined')
        
    def ask_exit(self):
        self.event_loop.call_soon_threadsafe(self.project_server.ws_server.close)
        logger.info('ws server closing')
        asyncio.run_coroutine_threadsafe(self.stop_async(), self.event_loop)
              

    async def stop_async(self):
        await self.project_server.ws_server.wait_closed()
        logger.info('ws server closed')
        self.event_loop.call_soon(self.event_loop.stop)
        logger.info('event loop stoped')
    
    @asyncio.coroutine
    def async_get(self):

        """ Calls q.get() in a separate Thread. 
        q.get is an I/O call, so it should release the GIL.
        """
        return (yield from self.event_loop.run_in_executor(concurrent.futures.ThreadPoolExecutor(thread_name_prefix='ws_QueueGet_ThreadPoolExecutor', max_workers=2), 
                                           self.queue.get))
            

    async def connection_handler(self, websocket, path):
        
        logger.info("new connection: {}, path: {}".format(websocket, path))

        if (path == '/' or path[0:9] == '/?session'):                                    # project manager
            await self.project_manager_session(websocket, path)
        elif path == '/upload':                            # file upload
            await self.upload_session(websocket)
        else:
            logger.info("unknow path: {}".format(path))

    async def project_manager_session(self, websocket, path):
        user_session = CuemsWsUser(self, websocket)
        await self.register(user_session, path)
        await user_session.outgoing.put(self.counter_event())
        try:
            consumer_task = asyncio.create_task(user_session.consumer_handler())
            producer_task = asyncio.create_task(user_session.producer_handler())
            # start 3 message processing task so a load or any other time consuming action still leaves with 2 tasks running  and interface feels responsive. TODO:discuss this
            processor_tasks = [asyncio.create_task(user_session.consumer()) for _ in range(3)]
            queue_task = asyncio.create_task(user_session.queue_handler())
            
            done_tasks, pending_tasks = await asyncio.wait([consumer_task, producer_task, *processor_tasks, queue_task], return_when=asyncio.FIRST_COMPLETED)
            for task in pending_tasks:
                task.cancel()

        finally:
            await self.unregister(user_session)

    async def upload_session(self, websocket):
        user_upload_session = CuemsUpload(self, websocket)
        logger.info("new upload session: {}".format(user_upload_session))

        await user_upload_session.message_handler()
        logger.info("upload session ended: {}".format(user_upload_session))

    async def register(self, user_session, path):
        logger.info("user registered: {}".format(id(user_session.websocket)))
        self.users[user_session] = None
        await self.notify_users("users")
        user_session.session_id =  await self.check_session(user_session, path)
        await self.load_session(user_session)

    async def check_session(self, user_session, path):
        session_uuid_patern = r"/\?session=(?P<uuid>[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[1][0-9A-Fa-f]{3}-[89AB][0-9A-Fa-f]{3}-[0-9A-Fa-f]{12})?"
        matches = re.search(session_uuid_patern, path)
        if (matches.groupdict()['uuid'] != None):
            uuid = matches.groupdict()['uuid']
            if uuid  not in self.sessions:
                logger.debug(f"uuid not found {uuid}, creating new session")
                uuid = str(uuid_module.uuid1())
            else:
                logger.debug(f"session_id found, reusing {uuid}")
        else:
            uuid = str(uuid_module.uuid1())
        
        try:
            self.sessions[uuid]['ws']=id(user_session.websocket)
        except KeyError:
            self.sessions[uuid]= {'ws': id(user_session.websocket)}

        await self.notify_session(user_session, uuid)

        return uuid

    async def load_session(self, user_session):
        try:
            await user_session.send_project(self.sessions[user_session.session_id]['loaded_project'], 'project_load')
        except KeyError:
            pass
    
    async def notify_session(self, user_session, uuid):
        message = json.dumps({"type": "session_id", "value": uuid})
        await user_session.outgoing.put(message)

    async def unregister(self, user_task):
        logger.info("user unregistered: {}".format(id(user_task.websocket)))
        self.users.pop(user_task, None)
        await self.notify_users("users")

    async def notify_state(self):
        if self.users:  # asyncio.wait doesn't accept an empty dcit
            message = self.counter_event()
            for user in self.users:
                await user.outgoing.put(message)

    async def notify_others_list_changes(self, calling_user, list_type):
        if self.users:  #notify others, not the user trigering the action, and only if the have same project loaded
            message = json.dumps({"type": "list_update", "value": list_type})
            for user, project in self.users.items():
                if user is not calling_user:
                    await user.outgoing.put(message)
                    logger.debug('notifing {} {}'.format(user, list_type))
            
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

                    logger.debug('same project loaded')
                    await user.outgoing.put(message)
                    logger.debug('notifing {}'.format(user))
    
    async def notify_users(self, type):
        if self.users:  # asyncio.wait doesn't accept an empty dcit
            message = self.users_event(type)
            await asyncio.wait([user.outgoing.put(message) for user in self.users])



    # warning, this non async function should bet not blocking or user @sync_to_async to get their own thread
    def counter_event(self):
        return json.dumps({"type": "counter", **self.state})


    def users_event(self, type, uuid=None):
        if type == "users":
            return json.dumps({"type": type, "value": len(self.users)})
        else:
            return json.dumps({"type": type, "uuid": uuid, "value" : "modified in server"}) # TODO: not used

    def exception_handler(self, loop, context):
        logger.debug("Caught the following exception: (ignore if on closing)")
        logger.debug(context['message'])


class CuemsWsUser():
    
    def __init__(self, server, websocket):
        self.server = server
        asyncio.set_event_loop(server.event_loop)
        self.incoming = asyncio.Queue()
        self.outgoing = asyncio.Queue()
        self.websocket = websocket
        self.session_id = None
        server.users[self] = None

    async def queue_handler(self):
        while True:
            item = await self.server.async_get()
            print(f'{id(self.websocket)} gets {item}')
            message = json.dumps({"type": "play_status", "value": item})
            await self.outgoing.put(message)

    async def consumer_handler(self):
        try:
            async for message in self.websocket:
                await self.incoming.put(message)
        except (ws.exceptions.ConnectionClosed, ws.exceptions.ConnectionClosedOK, ws.exceptions.ConnectionClosedError) as e:
                logger.debug(e)

    async def producer_handler(self):
        while True:
            message = await self.outgoing.get()
            try:
                await self.websocket.send(message)
            except (ws.exceptions.ConnectionClosed, ws.exceptions.ConnectionClosedOK, ws.exceptions.ConnectionClosedError) as e:
                logger.debug(e)
                break


    async def consumer(self):
        while True:
            message = await self.incoming.get()
            try:
                data = json.loads(message)
            except Exception as e:
                logger.error("error: {} {}".format(type(e), e))
                await self.notify_error_to_user('error decoding json') 
                continue
            try:
                if "action" not in data:
                    logger.error("unsupported event: {}".format(data))
                    await self.notify_error_to_user("unsupported event: {}".format(data))
                elif data["action"] == "minus":
                    self.server.state["value"] -= 1
                    await self.server.notify_state()
                elif data["action"] == "plus":
                    self.server.state["value"] += 1
                    await self.server.notify_state()
                elif data["action"] == "project_load":
                    await self.send_project(data["value"], data["action"])
                elif data["action"] == "project_save":
                    await self.received_project(data["value"], data["action"])
                elif data["action"] == "project_delete":
                    await self.request_delete_project(data["value"], data["action"])
                elif data["action"] == "project_restore":
                    await self.request_restore_project(data["value"], data["action"])
                elif data["action"] == "project_trash_delete":
                    await self.request_delete_project_trash(data["value"], data["action"])
                elif data["action"] == "project_list":
                    await self.list_project(data["action"])
                elif data["action"] == "project_duplicate":
                    await self.request_duplicate_project(data["value"], data["action"])
                elif data["action"] == "file_list":
                    await self.list_file(data["action"])
                elif data["action"] == "project_trash_list":
                    await self.list_project_trash(data["action"])
                elif data["action"] == "file_trash_list":
                    await self.list_file_trash(data["action"])
                elif data["action"] == "file_save":
                    await self.received_file_data(data["value"], data["action"])
                elif data["action"] == "file_load_meta":
                    await self.request_file_load_meta(data["value"], data["action"])
                elif data["action"] == "file_delete":
                    await self.request_delete_file(data["value"], data["action"])
                elif data["action"] == "file_restore":
                    await self.request_restore_file(data["value"], data["action"])
                elif data["action"] == "file_trash_delete":
                    await self.request_delete_file_trash(data["value"], data["action"])
                else:
                    logger.error("unsupported action: {}".format(data))
                    await self.notify_error_to_user("unsupported action: {}".format(data))
            except Exception as e:
                logger.error("error: {} {}".format(type(e), e))
                await self.notify_error_to_user('error processing request')


    async def notify_user(self, msg=None, uuid=None, action=None):
        if (uuid is None) and (action is None) and (msg is not None):
            await self.outgoing.put(json.dumps({"type": "state", "value":msg}))
        elif (msg is None):
            await self.outgoing.put(json.dumps({"type": action, "value": uuid}))

    async def notify_error_to_user(self, msg=None, uuid=None, action=None):
        if (msg is not None) and (uuid is None) and (action is None):
            await self.outgoing.put(json.dumps({"type": "error", "value": msg}))
        elif (action is not None) and (msg is not None) and (uuid is None):
            await self.outgoing.put(json.dumps({"type": "error", "action": action, "value": msg}))
        elif (action is not None) and (msg is not None) and (uuid is not None):
            await self.outgoing.put(json.dumps({"type": "error", "uuid": uuid, "action": action, "value": msg}))


    async def list_project(self, action):
        logger.info("user {} loading project list".format(id(self.websocket)))
        try:
            project_list = await self.server.event_loop.run_in_executor(self.server.executor, self.load_project_list)    
            await self.outgoing.put(json.dumps({"type": action, "value": project_list}))
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e),  action=action)

    async def send_project(self, project_uuid, action):
        try:
            if project_uuid == '':
                raise NonExistentItemError('project uuid is empty')
            logger.info("user {} loading project {}".format(id(self.websocket), project_uuid))
            project = await self.server.event_loop.run_in_executor(self.server.executor, self.load_project, project_uuid)
            msg = json.dumps({"type":"project", "value":project})
            await self.outgoing.put(msg)
            self.server.users[self] = project_uuid
            self.server.sessions[self.session_id]['loaded_project']=project_uuid
        except NonExistentItemError as e:
            logger.info(e)
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action )
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action )

    async def received_project(self, data, action):
        try:
            project_uuid = None
            new_project = False

            try:
                project_uuid = data['CuemsScript']['uuid']
            except KeyError:
                new_project = True

            if new_project:
                project_uuid = await self.server.event_loop.run_in_executor(self.server.executor, self.new_project, data)
            else:
                await self.server.event_loop.run_in_executor(self.server.executor, self.update_project, project_uuid, data)

            logger.info("user {} saving project {}".format(id(self.websocket), project_uuid))
            
            
            self.server.users[self] = project_uuid
            await self.notify_user(uuid=project_uuid, action=action)
            await self.server.notify_others_list_changes(self, "project_list")
            await self.server.notify_others_same_project(self, "project_modified", project_uuid)
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user((str(type(e)) + str(e)), uuid=project_uuid, action="project_save")

    async def list_project_trash(self, action):
        logger.info("user {} loading project trash list".format(id(self.websocket)))
        try:
            project_trash_list = await self.server.event_loop.run_in_executor(self.server.executor, self.load_project_trash_list)    
            await self.outgoing.put(json.dumps({"type": action, "value": project_trash_list}))
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e),  action=action)

    async def request_delete_project(self, project_uuid, action):
        try:
            logger.info("user {} deleting project: {}".format(id(self.websocket), project_uuid))
            
            await self.server.event_loop.run_in_executor(self.server.executor, self.delete_project, project_uuid)

            await self.notify_user(uuid=project_uuid, action=action)
            await self.server.notify_others_same_project(self, "project_update", project_uuid=project_uuid)
            await self.server.notify_others_list_changes(self, "project_list")
            await self.server.notify_others_list_changes(self, "project_trash_list")
        except NonExistentItemError as e:
            logger.info(e)
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)

    async def request_duplicate_project(self, project_uuid, action):
        try:
            logger.info("user {} duplicating project: {}".format(id(self.websocket), project_uuid))
            new_project_uuid = await self.server.event_loop.run_in_executor(self.server.executor, self.duplicate_project, project_uuid)
            await self.notify_user(uuid=project_uuid, action=action)
            await self.server.notify_others_list_changes(self, "project_list")
            await self.server.notify_others_list_changes(self, "file_list")
        except NonExistentItemError as e:
            logger.info(e)
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)

    async def request_restore_project(self, project_uuid, action):
        try:
            logger.info("user {} restoring project: {}".format(id(self.websocket), project_uuid))
            await self.server.event_loop.run_in_executor(self.server.executor, self.restore_project, project_uuid)
            await self.notify_user(uuid=project_uuid, action=action)
            await self.server.notify_others_list_changes(self, "project_list")
            await self.server.notify_others_list_changes(self, "project_trash_list")
        except NonExistentItemError as e:
            logger.info(e)
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)

    async def request_delete_project_trash(self, project_uuid, action):
        try:
            logger.info("user {} deleting project from trash: {}".format(id(self.websocket), project_uuid))
            
            await self.server.event_loop.run_in_executor(self.server.executor, self.delete_project_trash, project_uuid)

            await self.notify_user(uuid=project_uuid, action=action)
            await self.server.notify_others_list_changes(self, "project_trash_list")
        except NonExistentItemError as e:
            logger.info(e)
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)

    async def list_file(self, action):
        logger.info("user {} loading file list".format(id(self.websocket)))
        try:
            file_list = await self.server.event_loop.run_in_executor(self.server.executor, self.load_file_list)    
            await self.outgoing.put(json.dumps({"type": action, "value": file_list}))
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e),  action=action)

    async def received_file_data(self, data, action):
        try:
            file_uuid = data['uuid']

            logger.info("user {} update file data {}".format(id(self.websocket), file_uuid))
            
            return_message = await self.server.event_loop.run_in_executor(self.server.executor, self.save_file, file_uuid, data)
            await self.notify_user(uuid=file_uuid, action=action)
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
            
    async def request_file_load_meta(self, file_uuid, action):
        try:

            logger.info("user {} loading file meta data {}".format(id(self.websocket), file_uuid))
            
            file_meta_data = await self.server.event_loop.run_in_executor(self.server.executor, self.load_file_meta, file_uuid)
            await self.outgoing.put(json.dumps({"type": action, "value": file_meta_data}))
        except NonExistentItemError as e:
            logger.info(e)
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)

    async def list_file_trash(self, action):
        logger.info("user {} loading file trash list".format(id(self.websocket)))
        try:
            file_trash_list = await self.server.event_loop.run_in_executor(self.server.executor, self.load_file_trash_list)    
            await self.outgoing.put(json.dumps({"type": action, "value": file_trash_list}))
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e),  action=action)


    async def request_delete_file(self, file_uuid, action):
        try:
            logger.debug("user {} deleting file: {}".format(id(self.websocket), file_uuid))
            await self.server.event_loop.run_in_executor(self.server.executor, self.delete_file, file_uuid)
            await self.notify_user(uuid=file_uuid, action=action)
            await self.server.notify_others_list_changes(self, "file_list")
            await self.server.notify_others_list_changes(self, "file_trash_list")
        except NonExistentItemError as e:
            logger.info(e)
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)

    async def request_restore_file(self, file_uuid, action):
        try:
            logger.debug("user {} restoring file: {}".format(id(self.websocket), file_uuid))
            await self.server.event_loop.run_in_executor(self.server.executor, self.restore_file, file_uuid)
            await self.notify_user(uuid=file_uuid, action=action)
            await self.server.notify_others_list_changes(self, "file_list")
            await self.server.notify_others_list_changes(self, "file_trash_list")
        except NonExistentItemError as e:
            logger.info(e)
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)

    async def request_delete_file_trash(self, file_uuid, action):
        try:
            logger.info("user {} deleting file from trash: {}".format(id(self.websocket), file_uuid))
            await self.server.event_loop.run_in_executor(self.server.executor, self.delete_file_trash, file_uuid)
            await self.notify_user(uuid=file_uuid, action=action)
            await self.server.notify_others_list_changes(self, "file_trash_list")
        except NonExistentItemError as e:
            logger.info(e)
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)

    # call blocking functions asynchronously with run_in_executor ThreadPoolExecutor
    def load_project_list(self):
        logger.info("loading project list")
        return self.server.db.project.list()

    
    def load_project(self, project_uuid):
        logger.info("loading project: {}".format(project_uuid))
        return self.server.db.project.load(project_uuid)

    def new_project(self, data):
        logger.debug('saving new project, data:{}'.format(data))
        return self.server.db.project.new(data)

    def update_project(self, project_uuid, data):
        logger.debug('saving project, uuid:{}, data:{}'.format(project_uuid, data))
        self.server.db.project.update(project_uuid, data)

    def duplicate_project(self, project_uuid):
        logger.debug('duplicating project, uuid:{}'.format(project_uuid))
        self.server.db.project.duplicate(project_uuid)

    def delete_project(self, project_uuid):
        self.server.db.project.delete(project_uuid)

    def restore_project(self, project_uuid):
        self.server.db.project.restore(project_uuid)

    def load_project_trash_list(self):
        logger.info("loading project trash list")
        return self.server.db.project.list_trash()

    def delete_project_trash(self, project_uuid):
        self.server.db.project.delete_from_trash(project_uuid)

    def load_file_list(self):
        logger.info("loading file list")
        return self.server.db.media.list()

    def load_file_meta(self, uuid):
        logger.info("loading file list")
        return self.server.db.media.load_meta(uuid)


    def save_file(self, file_uuid, data):
        logger.info("saving file data")
        self.server.db.media.save(file_uuid, data)

    def delete_file(self, file_uuid):
        self.server.db.media.delete(file_uuid)

    def restore_file(self, file_uuid):
        self.server.db.media.restore(file_uuid)

    def load_file_trash_list(self):
        logger.info("loading file trash list")
        return self.server.db.media.list_trash()

    def delete_file_trash(self, file_uuid):
        self.server.db.media.delete_from_trash(file_uuid)

class CuemsUpload(StringSanitizer):

    uploading = False
    filename = None
    tmp_filename = None
    bytes_received = 0
    filesize = 0
    file_handle = None

    def __init__(self, server, websocket):
        self.server = server
        self.websocket = websocket
        self.tmp_upload_path = self.server.tmp_upload_path
        self.media_path = self.server.db.media.media_path
        
    async def message_handler(self):
        while True:
            try:
                message = await self.websocket.recv()
                if isinstance(message, str):
                    await self.process_upload_message(message)
                elif isinstance(message, bytes):
                    await self.process_upload_packet(message)
            except (ws.exceptions.ConnectionClosed, ws.exceptions.ConnectionClosedOK, ws.exceptions.ConnectionClosedError):
                logger.debug('upload connection closed, exiting loop')
                break

    async def message_sender(self, message):
        try:
            await self.websocket.send(message)
        except (ws.exceptions.ConnectionClosed, ws.exceptions.ConnectionClosedOK, ws.exceptions.ConnectionClosedError) as e:
                logger.debug(e)

    async def process_upload_message(self, message):
        data = json.loads(message)
        if 'action' not in data:
            return False
        if data['action'] == 'upload':
            await self.set_upload(file_info=data["value"])
        elif data['action'] == 'finished':
            await self.upload_done(data["value"])

    async def set_upload(self, file_info):
        
        if not os.path.exists(self.media_path):
            logger.error("upload folder doenst exists")
            await self.message_sender(json.dumps({'error' : 'upload folder doenst exist', 'fatal': True}))
            return False
        
        self.filename = StringSanitizer.sanitize_file_name(file_info['name'])
        self.tmp_filename = self.filename + '.tmp' + str(randint(100000, 999999))
        logger.debug('tmp upload path: {}'.format(self.tmp_file_path()))

        if not os.path.exists(self.tmp_file_path()):
            self.filesize = file_info['size']
            self.uploading = 'Ready'
            await self.message_sender(json.dumps({"ready" : True}))
        else:
            await self.message_sender(json.dumps({'error' : 'file allready exists', 'fatal': True}))
            logger.error("file allready exists")

    async def process_upload_packet(self, bin_data):

        if self.uploading == 'Ready':
            async with aiofiles.open(self.tmp_file_path(), mode='wb', loop=self.server.event_loop, executor=self.server.executor) as stream:
                await stream.write(bin_data)
                self.bytes_received += len(bin_data)
                await self.message_sender(json.dumps({"ready" : True}))

                while True:
                    message = await self.websocket.recv()
                    if isinstance(message, bytes):
                        await stream.write(message)
                        self.bytes_received += len(message)
                        await self.message_sender(json.dumps({"ready" : True}))
                    else:
                        await self.process_upload_message(message)
                        break


    async def upload_done(self, received_md5):
        try:
            
            await self.server.event_loop.run_in_executor(self.server.executor, self.check_file_integrity,  self.tmp_file_path(), received_md5)
            
            await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.media.new,  self.tmp_file_path(), self.filename)
            self.tmp_filename = None
            logger.debug('upload completed')
            await self.message_sender(json.dumps({"close" : True}))
            await self.server.notify_others_list_changes(None, "file_list")
        except Exception as e:
            logger.error("error: {} {}".format(type(e), e))
            await self.message_sender(json.dumps({'error' : 'error saving file', 'fatal': True}))

    def check_file_integrity(self, path, original_md5):
        
        with open(path, 'rb') as file_to_check:
            data = file_to_check.read()    
            returned_md5 = md5(data).hexdigest()
        if original_md5 != returned_md5:
            raise FileIntegrityError('MD5 mistmatch')
            
        return True

    def tmp_file_path(self):
        if not self.tmp_filename is None:
            return os.path.join(self.tmp_upload_path, self.tmp_filename)

    def __del__(self):
        try:
            if self.tmp_file_path():
                os.remove(self.tmp_file_path())  # TODO: change to pathlib ?  
                logger.debug('cleaning tmp upload file on object destruction: ({})'.format(self.tmp_file_path()))
        except FileNotFoundError:
            pass
