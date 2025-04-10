import json
import asyncio
import uuid as uuid_module
from datetime import datetime
import websockets as ws

from cuemsutils.log import logged, Logger

from CuemsErrors import *

class CuemsWsUser():
    
    def __init__(self, server, websocket):
        self.server = server
        asyncio.set_event_loop(server.event_loop)
        self.incoming = asyncio.Queue()
        self.outgoing = asyncio.Queue()
        self.websocket = websocket
        self.session_id = None
        server.users[self] = None

    async def consumer_handler(self):
        try:
            async for message in self.websocket:
                await self.incoming.put(message)
        except (ws.exceptions.ConnectionClosed, ws.exceptions.ConnectionClosedOK, ws.exceptions.ConnectionClosedError) as e:
                Logger.debug(e)

    async def producer_handler(self):
        while True:
            message = await self.outgoing.get()
            try:
                await self.websocket.send(message)
            except (ws.exceptions.ConnectionClosed, ws.exceptions.ConnectionClosedOK, ws.exceptions.ConnectionClosedError) as e:
                Logger.debug(e)
                break


    async def consumer(self):
        while True:
            message = await self.incoming.get()
            try:
                data = json.loads(message)
            except Exception as e:
                Logger.error("error: {} {}".format(type(e), e))
                await self.notify_error_to_user('error decoding json') 
                continue
            try:
                if "action" not in data:
                    Logger.error("unsupported event: {}".format(data))
                    await self.notify_error_to_user("unsupported event: {}".format(data))
                elif data["action"] == "project_load":
                    await self.send_project(data["value"], data["action"])
                elif data["action"] == "project_ready":
                    await self.project_ready(data["value"], data["action"])
                elif data["action"] == "hw_discovery":
                    await self.hw_discovery(data["action"])
                elif data["action"] == "project_deploy":
                    await self.project_deploy(data["value"], data["action"])
                elif data["action"] == "project_new":
                    await self.received_new_project(data["value"], data["action"], data["unix_name"])
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
                elif data["action"] == "file_load_thumbnail":
                    await self.request_file_load_thumbnail(data["value"], data["action"])
                elif data["action"] == "file_load_waveform":
                    await self.request_file_load_waveform(data["value"], data["action"])
                elif data["action"] == "file_delete":
                    await self.request_delete_file(data["value"], data["action"])
                elif data["action"] == "file_restore":
                    await self.request_restore_file(data["value"], data["action"])
                elif data["action"] == "file_trash_delete":
                    await self.request_delete_file_trash(data["value"], data["action"])
                else:
                    Logger.error("unsupported action: {}".format(data))
                    await self.notify_error_to_user("unsupported action: {}".format(data))
            except KeyError as e:
                Logger.error("error missing key:  {}".format(e))
                await self.notify_error_to_user('error missing key {} in request'.format(e))
            except Exception as e:
                Logger.error("error: {} {}".format(type(e), e))
                await self.notify_error_to_user('error processing request')


    async def notify_user(self, msg=None, uuid=None,  action=None, new_uuid=None):
        if (uuid is None) and (action is None) and (msg is not None):
            await self.outgoing.put(json.dumps({"type": "state", "value":msg}))
        elif (msg is None and new_uuid is None):
            await self.outgoing.put(json.dumps({"type": action, "value": uuid}))
        elif (msg is None and new_uuid is not None):
            await self.outgoing.put(json.dumps({"type": action, "value": { "uuid" : uuid, "new_uuid" : new_uuid}}))

    async def notify_error_to_user(self, msg=None, uuid=None, action=None):
        if (msg is not None) and (uuid is None) and (action is None):
            await self.outgoing.put(json.dumps({"type": "error", "value": msg}))
        elif (action is not None) and (msg is not None) and (uuid is None):
            await self.outgoing.put(json.dumps({"type": "error", "action": action, "value": msg}))
        elif (action is not None) and (msg is not None) and (uuid is not None):
            await self.outgoing.put(json.dumps({"type": "error", "uuid": uuid, "action": action, "value": msg}))

    async def comunicate_with_engine(self, action, action_uuid, engine_command):
        try:
            try:
                response = await self.server.engine_comunicator.send_request(engine_command)
            except Exception as e:
                raise EngineError(f'can not connect to engine: {e}')
            start_time = datetime.now()
            while True:
                time_delta = datetime.now() - start_time
                if time_delta.total_seconds() >= 30: #TODO: decide timeout, or get it from settings?
                    raise TimeoutError(f'Timeout waiting {action} response from engine')
                if response:
                    
                        if "action_uuid" in response:
                            if action_uuid in response['action_uuid']:
                                if 'type'  not in response:
                                    raise EngineError(f'Engine reports error {response}')
                                if response['type'] != action or response['value'] != 'OK':
                                    raise EngineError(f'Engine reports error {response}')
                                return response['value']
                
                await asyncio.sleep(0.25)

        except Exception as e:
            raise e

        

    async def project_ready(self, project_uuid, action):
        Logger.info(f"user {id(self.websocket)} requesting ready project {project_uuid}")
        try:
            unix_name = await self.server.event_loop.run_in_executor(self.server.executor, self.get_project_unix_name, project_uuid)
            action_uuid = str(uuid_module.uuid1())
            engine_command = {"action" : "project_ready", "action_uuid": action_uuid, "value" : unix_name}

            result = await self.comunicate_with_engine(action, action_uuid, engine_command)

            await self.outgoing.put(json.dumps({"type": "project_ready", "value": project_uuid}))

        except Exception as e:
            Logger.error(f"error: {type(e)} {e}")
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action )

    async def hw_discovery(self, action):
        Logger.info(f"user {id(self.websocket)} requesting hardware dicovery")
        try:
            action_uuid = str(uuid_module.uuid1())
            engine_command = {"action" : "hw_discovery", "action_uuid": action_uuid}

            result = await self.comunicate_with_engine(action, action_uuid, engine_command)

            await self.outgoing.put(json.dumps({"type": "hw_discovery", "value": result}))

        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), action=action )

    async def project_deploy(self, project_uuid, action):
        Logger.info(f"user {id(self.websocket)} requesting deploy project {project_uuid}")
        try:
            unix_name = await self.server.event_loop.run_in_executor(self.server.executor, self.get_project_unix_name, project_uuid)
            action_uuid = str(uuid_module.uuid1())
            engine_command = {"action" : "project_deploy", "action_uuid": action_uuid, "value" : unix_name}

            result = await self.comunicate_with_engine(action, action_uuid, engine_command)

            await self.outgoing.put(json.dumps({"type": "project_deploy", "value": project_uuid}))

        except Exception as e:
            Logger.error(f"error: {type(e)} {e}")
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action )

    async def list_project(self, action):
        Logger.info("user {} loading project list".format(id(self.websocket)))
        try:
            project_list = await self.server.event_loop.run_in_executor(self.server.executor, self.load_project_list)    
            await self.outgoing.put(json.dumps({"type": action, "value": project_list}))
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e),  action=action)


    async def send_project(self, project_uuid, action):
        try:
            Logger.info("user {} loading project {}".format(id(self.websocket), project_uuid))
            project = await self.server.event_loop.run_in_executor(self.server.executor, self.load_project, project_uuid)
            msg = json.dumps({"type":"project", "value":project})
            await self.outgoing.put(msg)
            self.server.users[self] = project_uuid
            self.server.sessions[self.session_id]['loaded_project']=project_uuid
        except NonExistentItemError as e:
            Logger.info(e)
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action )
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action )


    async def received_new_project(self, data, action, unix_name):
        try:

            project_uuid = await self.server.event_loop.run_in_executor(self.server.executor, self.new_project, data, unix_name)
            
            Logger.info("user {} new project {}".format(id(self.websocket), project_uuid))
            
            self.server.users[self] = project_uuid
            await self.notify_user(uuid=project_uuid, action=action)
            await self.server.notify_others_list_changes(self, "project_list")
            await self.server.notify_others_same_project(self, "project_modified", project_uuid)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user((str(type(e)) + str(e)), action="project_new")
    async def received_project(self, data, action):
        try:

            project_uuid = data['CuemsScript']['id']
            await self.server.event_loop.run_in_executor(self.server.executor, self.update_project, project_uuid, data)
            Logger.info("user {} saving project {}".format(id(self.websocket), project_uuid))
            
            
            self.server.users[self] = project_uuid
            await self.notify_user(uuid=project_uuid, action=action)
            await self.server.notify_others_list_changes(self, "project_list")
            await self.server.notify_others_same_project(self, "project_modified", project_uuid)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user((str(type(e)) + str(e)), uuid=project_uuid, action=action)

    async def list_project_trash(self, action):
        Logger.info("user {} loading project trash list".format(id(self.websocket)))
        try:
            project_trash_list = await self.server.event_loop.run_in_executor(self.server.executor, self.load_project_trash_list)    
            await self.outgoing.put(json.dumps({"type": action, "value": project_trash_list}))
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e),  action=action)

    async def request_delete_project(self, project_uuid, action):
        try:
            Logger.info("user {} deleting project: {}".format(id(self.websocket), project_uuid))
            
            await self.server.event_loop.run_in_executor(self.server.executor, self.delete_project, project_uuid)

            await self.notify_user(uuid=project_uuid, action=action)
            await self.server.notify_others_same_project(self, "project_update", project_uuid=project_uuid)
            await self.server.notify_others_list_changes(self, "project_list")
            await self.server.notify_others_list_changes(self, "project_trash_list")
        except NonExistentItemError as e:
            Logger.info(e)
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)

    async def request_duplicate_project(self, project_uuid, action):
        try:
            Logger.info("user {} duplicating project: {}".format(id(self.websocket), project_uuid))
            new_project_uuid = await self.server.event_loop.run_in_executor(self.server.executor, self.duplicate_project, project_uuid)
            await self.notify_user(uuid=project_uuid, action=action, new_uuid=new_project_uuid)
            await self.server.notify_others_list_changes(self, "project_list")
            await self.server.notify_others_list_changes(self, "file_list")
        except NonExistentItemError as e:
            Logger.info(e)
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)

    async def request_restore_project(self, project_uuid, action):
        try:
            Logger.info("user {} restoring project: {}".format(id(self.websocket), project_uuid))
            await self.server.event_loop.run_in_executor(self.server.executor, self.restore_project, project_uuid)
            await self.notify_user(uuid=project_uuid, action=action)
            await self.server.notify_others_list_changes(self, "project_list")
            await self.server.notify_others_list_changes(self, "project_trash_list")
        except NonExistentItemError as e:
            Logger.info(e)
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)

    async def request_delete_project_trash(self, project_uuid, action):
        try:
            Logger.info("user {} deleting project from trash: {}".format(id(self.websocket), project_uuid))
            
            await self.server.event_loop.run_in_executor(self.server.executor, self.delete_project_trash, project_uuid)

            await self.notify_user(uuid=project_uuid, action=action)
            await self.server.notify_others_list_changes(self, "project_trash_list")
        except NonExistentItemError as e:
            Logger.info(e)
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action)

    async def list_file(self, action):
        Logger.info("user {} loading file list".format(id(self.websocket)))
        try:
            file_list = await self.server.event_loop.run_in_executor(self.server.executor, self.load_file_list)    
            await self.outgoing.put(json.dumps({"type": action, "value": file_list}))
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e),  action=action)

    async def received_file_data(self, data, action):
        try:
            file_uuid = data['uuid']

            Logger.info("user {} update file data {}".format(id(self.websocket), file_uuid))
            
            return_message = await self.server.event_loop.run_in_executor(self.server.executor, self.save_file, file_uuid, data)
            await self.notify_user(uuid=file_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
            
    async def request_file_load_meta(self, file_uuid, action):
        try:

            Logger.info("user {} loading file meta data {}".format(id(self.websocket), file_uuid))
            
            file_meta_data = await self.server.event_loop.run_in_executor(self.server.executor, self.load_file_meta, file_uuid)
            await self.outgoing.put(json.dumps({"type": action, "value": file_meta_data}))
        except NonExistentItemError as e:
            Logger.info(e)
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)

    async def request_file_load_thumbnail(self, file_uuid, action):
        try:

            Logger.info("user {} loading file thumbnail {}".format(id(self.websocket), file_uuid))
            
            file_thumbnail = await self.server.event_loop.run_in_executor(self.server.executor, self.load_file_thumbnail, file_uuid)
            await self.outgoing.put(file_thumbnail) #TODO: add uuid encoded in the binary message
        except NonExistentItemError as e:
            Logger.warning(e)
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)

    async def request_file_load_waveform(self, file_uuid, action):
        try:

            Logger.info("user {} loading file waveform {}".format(id(self.websocket), file_uuid))
            
            file_waveform = await self.server.event_loop.run_in_executor(self.server.executor, self.load_file_waveform, file_uuid)
            await self.outgoing.put(file_waveform) #TODO: add uuid encoded in the binary message
        except NonExistentItemError as e:
            Logger.warning(e)
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)

    async def list_file_trash(self, action):
        Logger.info("user {} loading file trash list".format(id(self.websocket)))
        try:
            file_trash_list = await self.server.event_loop.run_in_executor(self.server.executor, self.load_file_trash_list)    
            await self.outgoing.put(json.dumps({"type": action, "value": file_trash_list}))
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e),  action=action)


    async def request_delete_file(self, file_uuid, action):
        try:
            Logger.debug("user {} deleting file: {}".format(id(self.websocket), file_uuid))
            await self.server.event_loop.run_in_executor(self.server.executor, self.delete_file, file_uuid)
            await self.notify_user(uuid=file_uuid, action=action)
            await self.server.notify_others_list_changes(self, "file_list")
            await self.server.notify_others_list_changes(self, "file_trash_list")
        except NonExistentItemError as e:
            Logger.info(e)
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)

    async def request_restore_file(self, file_uuid, action):
        try:
            Logger.debug("user {} restoring file: {}".format(id(self.websocket), file_uuid))
            await self.server.event_loop.run_in_executor(self.server.executor, self.restore_file, file_uuid)
            await self.notify_user(uuid=file_uuid, action=action)
            await self.server.notify_others_list_changes(self, "file_list")
            await self.server.notify_others_list_changes(self, "file_trash_list")
        except NonExistentItemError as e:
            Logger.info(e)
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)

    async def request_delete_file_trash(self, file_uuid, action):
        try:
            Logger.info("user {} deleting file from trash: {}".format(id(self.websocket), file_uuid))
            await self.server.event_loop.run_in_executor(self.server.executor, self.delete_file_trash, file_uuid)
            await self.notify_user(uuid=file_uuid, action=action)
            await self.server.notify_others_list_changes(self, "file_trash_list")
        except NonExistentItemError as e:
            Logger.info(e)
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)

    # call blocking functions asynchronously with run_in_executor ThreadPoolExecutor
    def load_project_list(self):
        Logger.info("loading project list")
        return self.server.db.project.list()

    def get_project_unix_name(self, project_uuid):
        Logger.info("loading project unix_name")
        return self.server.db.project.get_project_unix_name(project_uuid)

    
    def load_project(self, project_uuid):
        Logger.info("loading project: {}".format(project_uuid))
        return self.server.db.project.load(project_uuid)

    def new_project(self, data, unix_name):
        Logger.debug('saving new project, data:{}, unix_name{}'.format(data, unix_name))
        return self.server.db.project.new(data, unix_name)

    def update_project(self, project_uuid, data):
        Logger.debug('saving project, uuid:{}, data:{}'.format(project_uuid, data))
        self.server.db.project.update(project_uuid, data)

    def duplicate_project(self, project_uuid):
        Logger.debug('duplicating project, uuid:{}'.format(project_uuid))
        return self.server.db.project.duplicate(project_uuid)

    def delete_project(self, project_uuid):
        self.server.db.project.delete(project_uuid)

    def restore_project(self, project_uuid):
        self.server.db.project.restore(project_uuid)

    def load_project_trash_list(self):
        Logger.info("loading project trash list")
        return self.server.db.project.list_trash()

    def delete_project_trash(self, project_uuid):
        self.server.db.project.delete_from_trash(project_uuid)

    def load_file_list(self):
        Logger.info("loading file list")
        return self.server.db.media.list()

    def load_file_meta(self, uuid):
        Logger.info("loading file meta")
        return self.server.db.media.load_meta(uuid)
        
    def load_file_thumbnail(self, uuid):
        Logger.info("loading file thumbnail")
        return self.server.db.media.load_thumbnail(uuid)

    def load_file_waveform(self, uuid):
        Logger.info("loading file waveform")
        return self.server.db.media.load_waveform(uuid)


    def save_file(self, file_uuid, data):
        Logger.info("saving file data")
        self.server.db.media.save(file_uuid, data)

    def delete_file(self, file_uuid):
        self.server.db.media.delete(file_uuid)

    def restore_file(self, file_uuid):
        self.server.db.media.restore(file_uuid)

    def load_file_trash_list(self):
        Logger.info("loading file trash list")
        return self.server.db.media.list_trash()

    def delete_file_trash(self, file_uuid):
        self.server.db.media.delete_from_trash(file_uuid)
