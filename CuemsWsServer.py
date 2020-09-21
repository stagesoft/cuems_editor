import sys
import asyncio
import concurrent.futures
import json
import logging
import os
import shutil
import aiofiles
import websockets as ws
from multiprocessing import Process, Event
import signal
from random import randint
from hashlib import md5

import time

from CuemsProjectManager import CuemsMedia




stream = logging.StreamHandler()
formatter = logging.Formatter('Cuems:ws-server: %(levelname)s (PID: %(process)d)-%(threadName)-9s)-(%(funcName)s) %(message)s')
stream.setFormatter(formatter)

logger_ws_server = logging.getLogger()
logger_ws_server.setLevel(logging.DEBUG)
logger_ws_server.addHandler(stream)

logger_asyncio = logging.getLogger('asyncio')
logger_asyncio.setLevel(logging.INFO)  # asyncio debug level 

logger_ws = logging.getLogger('websockets')
logger_ws.setLevel(logging.INFO)  # websockets debug level,  in debug prints all frames, also binary frames! 

class CuemsWsServer():
    
    state = {"value": 0} #TODO: provisional
    users = dict()
    projects=[{"CuemsScript": {"uuid": "76861217-2d40-47a2-bdb5-8f9c91293855", "name": "Proyecto test 0", "date": "14/08/2020 11:18:16", "timecode_cuelist": {"CueList": {"Cue": [{"uuid": "bf2d217f-881d-47c1-9ad1-f5999769bcc5", "time": {"CTimecode": "00:00:33:00"}, "type": "mtc", "loop": "False", "outputs": {"CueOutputs": {"id": 5, "bla": "ble"}}}, {"uuid": "8ace53f3-74f5-4195-822e-93c12fdf3725", "time": {"NoneType": "None"}, "type": "floating", "loop": "False", "outputs": {"CueOutputs": {"physiscal": 1, "virtual": 3}}}], "AudioCue": {"uuid": "be288e38-887a-446f-8cbf-c16c9ec6724a", "time": {"CTimecode": "00:00:45:00"}, "type": "virtual", "loop": "True", "outputs": {"AudioCueOutputs": {"stereo": 1}}}}}, "floating_cuelist": {"CueList": {"DmxCue": {"uuid": "f36fa4b3-e220-4d75-bff1-210e14655c11", "time": {"CTimecode": "00:00:23:00"}, "dmx_scene": {"DmxScene": {"DmxUniverse": [{"id": 0, "DmxChannel": [{"id": 0, "&": 10}, {"id": 1, "&": 50}]}, {"id": 1, "DmxChannel": [{"id": 20, "&": 23}, {"id": 21, "&": 255}]}, {"id": 2, "DmxChannel": [{"id": 5, "&": 10}, {"id": 6, "&": 23}, {"id": 7, "&": 125}, {"id": 8, "&": 200}]}]}}, "outputs": {"DmxCueOutputs": {"universe0": 3}}}, "Cue": {"uuid": "17376d8f-84c6-4f28-859a-a01260a1dadb", "time": {"CTimecode": "00:00:05:00"}, "type": "virtual", "loop": "False", "outputs": {"CueOutputs": {"id": 3}}}}}}}, {"CuemsScript": {"uuid": "e05de59a-b281-4abf-83ba-97198d661a63", "name": "Segundo proyecto", "date": "13/08/2020 07:23:12", "timecode_cuelist": {"CueList": {"Cue": [{"uuid": "d47a75e2-f76e-4c77-b33e-e1df40ffdf02", "time": {"CTimecode": "00:00:33:00"}, "type": "mtc", "loop": "False", "outputs": {"CueOutputs": {"id": 5, "bla": "ble"}}}, {"uuid": "b5c35e3d-91f6-42d8-9825-0176354b44c1", "time": {"NoneType": "None"}, "type": "floating", "loop": "False", "outputs": {"CueOutputs": {"physiscal": 1, "virtual": 3}}}], "AudioCue": {"uuid": "aef5e289-03b0-4b39-99cd-90063d9b8c80", "time": {"CTimecode": "00:00:45:00"}, "type": "virtual", "loop": "True", "outputs": {"AudioCueOutputs": {"stereo": 1}}}}}, "floating_cuelist": {"CueList": {"DmxCue": {"uuid": "5d4ef443-5a49-4986-a283-9563ee7a9e85", "time": {"CTimecode": "00:00:23:00"}, "dmx_scene": {"DmxScene": {"DmxUniverse": [{"id": 0, "DmxChannel": [{"id": 0, "&": 10}, {"id": 1, "&": 50}]}, {"id": 1, "DmxChannel": [{"id": 20, "&": 23}, {"id": 21, "&": 255}]}, {"id": 2, "DmxChannel": [{"id": 5, "&": 10}, {"id": 6, "&": 23}, {"id": 7, "&": 125}, {"id": 8, "&": 200}]}]}}, "outputs": {"DmxCueOutputs": {"universe0": 3}}}, "Cue": {"uuid": "37f80125-1c41-4cce-aab1-13328dd8c94e", "time": {"CTimecode": "00:00:05:00"}, "type": "virtual", "loop": "False", "outputs": {"CueOutputs": {"id": 3}}}}}}}]
    tmp_upload_forlder_path = '/tmp/cuemsupload' 
    upload_forlder_path = os.path.join(os.getcwd(), 'upload')     #TODO: get upload folder path from settings?
    
    def __init__(self):
        try:
            if not os.path.exists(self.tmp_upload_forlder_path):
                os.mkdir(self.tmp_upload_forlder_path)
                logging.info('creating tmp upload folder {}'.format(self.tmp_upload_forlder_path))
        except Exception as e:
            logging.error("error: {} {}".format(type(e), e))



    def start(self, port):
        self.event = Event()
        self.process = Process(target=self.run_async_server, args=(self.event,))
        self.port = port
        self.host = 'localhost'
        self.process.start()

        

    def run_async_server(self, event):
        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)
        self.executor =  concurrent.futures.ThreadPoolExecutor(thread_name_prefix='ws_load_ThreadPoolExecutor', max_workers=5) # TODO: adjust max workers
        #self.event_loop.set_exception_handler(self.exception_handler) ### TODO:UNCOMENT FOR PRODUCTION 
        self.project_server = ws.serve(self.connection_handler, self.host, self.port, max_size=None) #TODO: choose max packets size from ui and limit it here
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.event_loop.add_signal_handler(sig, self.ask_exit)
        logging.info('server listening on {}, port {}'.format(self.host, self.port))
        self.event_loop.run_until_complete(self.project_server)
        self.event_loop.run_forever()
        self.event_loop.close()
        
    def stop(self):
        os.kill(self.process.pid, signal.SIGTERM)
        self.process.join()
        logging.info('ws process joined')
        
    def ask_exit(self):
        self.event_loop.call_soon_threadsafe(self.project_server.ws_server.close)
        logging.info('ws server closing')
        asyncio.run_coroutine_threadsafe(self.stop_async(), self.event_loop)
              

    async def stop_async(self):
        await self.project_server.ws_server.wait_closed()
        logging.info('ws server closed')
        self.event_loop.call_soon(self.event_loop.stop)
        logging.info('event loop stoped')

    async def connection_handler(self, websocket, path):
        
        logging.info("new connection: {}, path: {}".format(websocket, path))

        if path == '/':                                    # project manager
            await self.project_manager_session(websocket)
        elif path == '/upload':                            # file upload
            await self.upload_session(websocket)
        else:
            logging.info("unknow path: {}".format(path))

    async def project_manager_session(self, websocket):
        user_session = CuemsWsUser(self, websocket)
        await self.register(user_session)
        await user_session.outgoing.put(self.counter_event())
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
        logging.info("new upload session: {}".format(user_upload_session))

        await user_upload_session.message_handler()
        logging.info("upload session ended: {}".format(user_upload_session))

    async def register(self, user_task):
        logging.info("user registered: {}".format(id(user_task.websocket)))
        self.users[user_task] = None
        await self.notify_users("users")

    async def unregister(self, user_task):
        logging.info("user unregistered: {}".format(id(user_task.websocket)))
        self.users.pop(user_task, None)
        await self.notify_users("users")

    async def notify_state(self):
        if self.users:  # asyncio.wait doesn't accept an empty dcit
            message = self.counter_event()
            for user in self.users:
                await user.outgoing.put(message)
            
    async def notify_others(self, calling_user, type, project_uuid=None):
        if self.users:  #notify others, not the user trigering the action, and only if the have same project loaded
            message = self.users_event(type)
            for user, project in self.users.items():
                if user is not calling_user:
                    if project_uuid is not None:
                        if str(project) != str(project_uuid):
                            continue
                    else:
                        if str(project) != str(self.users[calling_user]):
                            continue

                    logging.debug('same project loaded')
                    await user.outgoing.put(message)
                    logging.debug('notifing {}'.format(user))
    
    async def notify_users(self, type):
        if self.users:  # asyncio.wait doesn't accept an empty dcit
            message = self.users_event(type)
            await asyncio.wait([user.outgoing.put(message) for user in self.users])



    # warning, this non async function should bet not blocking or user @sync_to_async to get their own thread
    def counter_event(self):
        return json.dumps({"type": "counter", **self.state})


    def users_event(self, type):
        if type == "users":
            return json.dumps({"type": type, "count": len(self.users)})
        elif type == "changes":
            return json.dumps({"type": "state", "value" : "project modified in server"})

    def exception_handler(self, loop, context):
        logging.debug("Caught the following exception: (ignore if on closing)")
        logging.debug(context['message'])


class CuemsWsUser():
    
    def __init__(self, server, websocket):
        self.server = server
        asyncio.set_event_loop(server.event_loop)
        self.incoming = asyncio.Queue()
        self.outgoing = asyncio.Queue()
        self.websocket = websocket
        server.users[self] = None

    async def consumer_handler(self):
        try:
            async for message in self.websocket:
                await self.incoming.put(message)
        except (ws.exceptions.ConnectionClosed, ws.exceptions.ConnectionClosedOK, ws.exceptions.ConnectionClosedError) as e:
                logging.debug(e)

    async def producer_handler(self):
        while True:
            message = await self.producer()
            try:
                await self.websocket.send(message)
            except (ws.exceptions.ConnectionClosed, ws.exceptions.ConnectionClosedOK, ws.exceptions.ConnectionClosedError) as e:
                logging.debug(e)
                break


    async def consumer(self):
        while True: 
            message = await self.incoming.get()
            data = json.loads(message)
            if "action" not in data:
                logging.error("unsupported event: {}".format(data))
                await self.notify_error_to_user("unsupported event: {}".format(data))
            elif data["action"] == "minus":
                self.server.state["value"] -= 1
                await self.server.notify_state()
            elif data["action"] == "plus":
                self.server.state["value"] += 1
                await self.server.notify_state()
            elif data["action"] == "load_project":
                await self.send_project(data["value"])
            elif data["action"] == "save_project":
                await self.received_project(data["value"])
            elif data["action"] == "delete_project":
                await self.request_delete(data["value"])
            elif data["action"] == "list_projects":
                await self.list_projects()
            elif data["action"] == "list_files":
                await self.list_files()
            else:
                logging.error("unsupported action: {}".format(data))
                await self.notify_error_to_user("unsupported action: {}".format(data))

    
                    
                       

    async def producer(self):
        while True:
            message = await self.outgoing.get()
            return message

    async def notify_user(self, msg):
        await self.outgoing.put(json.dumps({"type": "state", "value":msg}))

    async def notify_error_to_user(self, msg):
        await self.outgoing.put(json.dumps({"type": "error", "value":msg}))


    async def list_projects(self):
        logging.info("user {} loading project list".format(id(self.websocket)))
        try:
            project_list = await self.server.event_loop.run_in_executor(self.server.executor, self.load_project_list)    
            await self.outgoing.put(json.dumps({"type": "project_list", "value": project_list}))
        except Exception as e:
            logging.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user('error loading project list')

    async def send_project(self, project_uuid):
        try:
            if project_uuid == '':
                raise NonExistentItemError('project uuid is empty')
            logging.info("user {} loading project {}".format(id(self.websocket), project_uuid))
            project = await self.server.event_loop.run_in_executor(self.server.executor, self.load_project, project_uuid)
            msg = json.dumps({"type":"project", "value":json.dumps(project)})
            await self.outgoing.put(msg)
            await self.notify_user("project loaded")
            self.server.users[self] = project_uuid
        except Exception as e:
            logging.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user('error loading project')

    async def received_project(self, data):
        try:
            project = json.loads(data)
            project_uuid = project['CuemsScript']['uuid']

            logging.info("user {} saving project {}".format(id(self.websocket), project_uuid))
            
            return_message = await self.server.event_loop.run_in_executor(self.server.executor, self.save_project, project_uuid, project)
            self.server.users[self] = project_uuid
            await self.notify_user("{} project saved".format(return_message))
            await self.server.notify_others(self, "changes")
        except Exception as e:
            logging.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user('error saving project')

    async def request_delete(self, project_uuid):
        try:
            logging.info("user {} deleting project: {}".format(id(self.websocket), project_uuid))
            
            await self.server.event_loop.run_in_executor(self.server.executor, self.delete_project,project_uuid)

            # self.users[self] = None  #TODO:what is now the active project? deleted project was the active one?
            await self.notify_user("project {} deleted".format(project_uuid))
            await self.server.notify_others(self, "changes", project_uuid=project_uuid)
        except Exception as e:
            logging.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user('error deleting project')

    async def list_files(self):
        logging.info("user {} loading file list".format(id(self.websocket)))
        try:
            file_list = await self.server.event_loop.run_in_executor(self.server.executor, self.load_file_list)    
            await self.outgoing.put(json.dumps({"type": "file_list", "value": file_list}))
        except Exception as e:
            logging.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user('error loading file list')




    # call blocking functions asynchronously with run_in_executor ThreadPoolExecutor
    def load_project_list(self):
        logging.info("loading project list")
        project_list = list()
        for project in self.server.projects: #TODO: provisional
            try:
                project_list.append({project['CuemsScript']['uuid']:{"name":project['CuemsScript']['name'], "date":project['CuemsScript']['date']}})
            except:
                raise NonExistentItemError('malformed project')
        return project_list

    
    def load_project(self, project):
        logging.info("loading project: {}".format(project))
        for elem in self.server.projects:
            if elem['CuemsScript']['uuid'] == project:
                logging.debug("loading project: {}".format(elem))
                return elem
        raise NonExistentItemError('Can not find project uuid for loading')

    def save_project(self, uuid, data):
        logging.info("loading project: {}".format(uuid))
        logging.debug('saving project, uuid:{}, data:{}'.format(uuid, data))
        for num, elem in enumerate(self.server.projects):
            if elem['CuemsScript']['uuid'] == uuid:
                self.server.projects[num] = data
                return 'updated'

        self.server.projects.append(data)
        return 'new'

    def delete_project(self, uuid):
        logging.info('deleting project, uuid:{}'.format(uuid))
        for num, elem in enumerate(self.server.projects):
            if elem['CuemsScript']['uuid'] == uuid:
                del self.server.projects[num]
                return True

        raise NonExistentItemError('Can not find project uuid for deletion')

    def load_file_list(self):
        logging.info("loading project list")
        return CuemsMedia.list()

class CuemsUpload():

    uploading = False
    filename = None
    tmp_filename = None
    bytes_received = 0
    filesize = 0
    file_handle = None

    def __init__(self, server, websocket):
        self.server = server
        self.websocket = websocket
        self.tmp_upload_forlder_path = server.tmp_upload_forlder_path
        self.upload_forlder_path = server.upload_forlder_path
        
    async def message_handler(self):
        while True:
            try:
                message = await self.websocket.recv()
                if isinstance(message, str):
                    await self.process_upload_message(message)
                elif isinstance(message, bytes):
                    await self.process_upload_packet(message)
            except (ws.exceptions.ConnectionClosed, ws.exceptions.ConnectionClosedOK, ws.exceptions.ConnectionClosedError):
                logging.debug('upload connection closed, exiting loop')
                break

    async def message_sender(self, message):
        try:
            await self.websocket.send(message)
        except (ws.exceptions.ConnectionClosed, ws.exceptions.ConnectionClosedOK, ws.exceptions.ConnectionClosedError) as e:
                logging.debug(e)

    async def process_upload_message(self, message):
        data = json.loads(message)
        if 'action' not in data:
            return False
        if data['action'] == 'upload':
            await self.set_upload(file_info=data["value"])
        elif data['action'] == 'finished':
            await self.upload_done(data["value"])

    async def set_upload(self, file_info):
        
        if not os.path.exists(self.upload_forlder_path):
            logging.error("upload folder doenst exists")
            await self.message_sender(json.dumps({'error' : 'upload folder doenst exist', 'fatal': True}))
            return False
        
        self.filename = file_info['name']
        self.tmp_filename = self.filename + '.tmp' + str(randint(100000, 999999))
        logging.debug('tmp upload path: {}'.format(self.tmp_file_path()))

        if not os.path.exists(self.tmp_file_path()):
            self.filesize = file_info['size']
            self.uploading = 'Ready'
            await self.message_sender(json.dumps({"ready" : True}))
        else:
            await self.message_sender(json.dumps({'error' : 'file allready exists', 'fatal': True}))
            logging.error("file allready exists")

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

            i = 0
            (base, ext) = os.path.splitext(self.filename)
            while True:     
                if not os.path.exists(self.file_path()):
                    logging.info('new file uploaded, saving to: {}'.format(self.file_path()))
                    shutil.move( self.tmp_file_path(), self.file_path())
                    self.tmp_filename = None
                    break
                else:
                    i += 1
                    self.filename = base + str(i) + ext
                    continue
            
            await self.server.event_loop.run_in_executor(self.server.executor, CuemsMedia,  self.filename)
            logging.debug('upload completed')
            await self.message_sender(json.dumps({"close" : True}))
        except Exception as e:
            if self.tmp_filename is None:
                try:
                    os.remove(self.file_path())  # TODO: change to pathlib ?  
                    logging.error('cleaning file upload because db insertion failed: ({})'.format(self.file_path()))
                except FileNotFoundError:
                    pass
            logging.error("error: {} {}".format(type(e), e))
            await self.message_sender(json.dumps({'error' : 'error saving file', 'fatal': True}))

    def check_file_integrity(self, path, original_md5):
        
        with open(path, 'rb') as file_to_check:
            data = file_to_check.read()    
            returned_md5 = md5(data).hexdigest()
        if original_md5 != returned_md5:
            raise FileIntegrityError('MD5 mistmatch')
            
        return True

    def file_path(self):
        return os.path.join(self.upload_forlder_path, self.filename)
    
    def tmp_file_path(self):
        if not self.tmp_filename is None:
            return os.path.join(self.tmp_upload_forlder_path, self.tmp_filename)

    def __del__(self):
        try:
            if self.tmp_file_path():
                os.remove(self.tmp_file_path())  # TODO: change to pathlib ?  
                logging.debug('cleaning tmp upload file on object destruction: ({})'.format(self.tmp_file_path()))
        except FileNotFoundError:
            pass
        

class CuemsWsServerError(Exception):
    pass
class FileIntegrityError(CuemsWsServerError):
    pass
class NonExistentItemError(CuemsWsServerError):
    pass