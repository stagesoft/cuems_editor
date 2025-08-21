import json
import asyncio
from datetime import datetime, timedelta
from cuemsutils.helpers import new_uuid, new_datetime
import websockets as ws
import sys

from cuemsutils.log import logged, Logger

from CuemsErrors import *

TIMEOUT = 25 #TODO: make it configurable, or get from settings

functionNameAsString = lambda n=0: sys._getframe(n + 1).f_code.co_name

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
                action = data.get("action")
                value = data.get("value")
                if not action:
                    Logger.error(f"unsupported event: {data}")
                    await self.notify_error_to_user(f"unsupported event: {data}")
                    return

                action_map = {
                    "project_load": lambda: self.send_project(value, action),
                    "project_ready": lambda: self.project_ready(value, action),
                    "project_deploy": lambda: self.project_deploy(value, action),
                    "project_new": lambda: self.received_new_project(value, action, data.get("unix_name")),
                    "project_save": lambda: self.received_project(value, action),
                    "project_delete": lambda: self.request_delete_project(value, action),
                    "project_restore": lambda: self.request_restore_project(value, action),
                    "project_trash_delete": lambda: self.request_delete_project_trash(value, action),
                    "project_list": lambda: self.list_project(action),
                    "project_duplicate": lambda: self.request_duplicate_project(value, action),
                    "file_list": lambda: self.list_file(action),
                    "project_trash_list": lambda: self.list_project_trash(action),
                    "file_trash_list": lambda: self.list_file_trash(action),
                    "file_save": lambda: self.received_file_data(value, action),
                    "file_load_meta": lambda: self.request_file_load_meta(value, action),
                    "file_load_thumbnail": lambda: self.request_file_load_thumbnail(value, action),
                    "file_load_waveform": lambda: self.request_file_load_waveform(value, action),
                    "file_delete": lambda: self.request_delete_file(value, action),
                    "file_restore": lambda: self.request_restore_file(value, action),
                    "file_trash_delete": lambda: self.request_delete_file_trash(value, action),
                    "hw_discovery": lambda: self.hw_discovery(action),
                    "nodeconf": lambda: self.nodeconf(action),
                }

                if action in action_map:
                    await action_map[action]()
                else:
                    Logger.error(f"unsupported action: {data}")
                    await self.notify_error_to_user(f"unsupported action: {data}")
            except KeyError as e:
                Logger.error("error missing key:  {}".format(e))
                await self.notify_error_to_user('error missing key {} in request'.format(e))
            except Exception as e:
                Logger.error("error: {} {}".format(type(e), e))
                await self.notify_error_to_user('error processing request')

    async def comunicate_with_engine(self, action, action_uuid, engine_command):
        try:
            try:
                async with asyncio.timeout(TIMEOUT):
                    response = await self.server.engine_communicator.send_request(engine_command)
            except TimeoutError:
                raise EngineError(f' Timeout Erro: Engine did not respond in 30 secs for {action} action with uuid {action_uuid}')
            except Exception as e:
                raise EngineError(f'can not connect to engine: {e}, {type(e)})')

            if response:
                    Logger.debug(f'got response from engine {response}')
                    if "action_uuid" in response.keys():
                        if action_uuid in response['action_uuid']:
                            if 'type'  not in response:
                                raise EngineError(f'Engine reports error {response}')
                            if response['type'] != action or response['value'] != 'OK':
                                raise EngineError(f'Engine reports error {response["value"]}')
                            Logger.debug(f'Engine response for {action} is OK')
                            return response['value']
                    else:
                        raise EngineError(f'Engine reports error {response}')

            else:
                raise EngineError(f'Engine did not respond with valid response')
            

        except Exception as e:
            raise e

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


    async def project_ready(self, project_uuid, action):
        Logger.info(f"user {id(self.websocket)} requesting ready project {project_uuid}")
        try:
            unix_name = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.project.get_project_unix_name, project_uuid)
            action_uuid = str(new_uuid())
            engine_command = {"action" : functionNameAsString(), "action_uuid": action_uuid, "value" : unix_name}

            result = await self.comunicate_with_engine(action, action_uuid, engine_command)
            Logger.debug(f"project {project_uuid} ready: {result}")

            await self.outgoing.put(json.dumps({"type": functionNameAsString(), "value": project_uuid}))

        except Exception as e:
            Logger.error(f"error: {type(e)} {e}")
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action )

    async def hw_discovery(self, action):
        Logger.info(f"user {id(self.websocket)} requesting {functionNameAsString()} dicovery")
        try:
            action_uuid = str(new_uuid())
            engine_command = {"action" : functionNameAsString(), "action_uuid": action_uuid}

            result = await self.comunicate_with_engine(action, action_uuid, engine_command)

            await self.outgoing.put(json.dumps({"type": functionNameAsString(), "value": result}))

        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), action=action )

    async def nodeconf(self, action):
        Logger.info(f"user {id(self.websocket)} requesting {functionNameAsString()} dicovery")
        try:
            action_uuid = str(new_uuid())
            engine_command = {"action" : functionNameAsString(), "action_uuid": action_uuid}

            result = await self.comunicate_with_engine(action, action_uuid, engine_command)

            await self.outgoing.put(json.dumps({"type": functionNameAsString(), "value": result}))

        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), action=action )

    async def project_deploy(self, project_uuid, action):
        Logger.info(f"user {id(self.websocket)} requesting deploy project {project_uuid}")
        try:
            unix_name = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.project.get_project_unix_name, project_uuid)
            action_uuid = str(new_uuid())
            engine_command = {"action" : functionNameAsString(), "action_uuid": action_uuid, "value" : unix_name}

            result = await self.comunicate_with_engine(action, action_uuid, engine_command)

            await self.outgoing.put(json.dumps({"type": functionNameAsString(), "value": project_uuid}))

        except Exception as e:
            Logger.error(f"error: {type(e)} {e}")
            await self.notify_error_to_user(str(e), uuid=project_uuid, action=action )

    async def list_project(self, action):
        Logger.info("user {} loading project list".format(id(self.websocket)))
        try:
            project_list = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.project.list)    
            await self.outgoing.put(json.dumps({"type": action, "value": project_list}))
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e),  action=action)


    async def send_project(self, project_uuid, action):
        try:
            Logger.info("user {} loading project {}".format(id(self.websocket), project_uuid))
            project = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.project.load, project_uuid)
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

            project_uuid = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.project.new, data, unix_name)
            
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
            await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.project.update, project_uuid, data)
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
            project_trash_list = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.project.list_trash)    
            await self.outgoing.put(json.dumps({"type": action, "value": project_trash_list}))
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e),  action=action)

    async def request_delete_project(self, project_uuid, action):
        try:
            Logger.info("user {} deleting project: {}".format(id(self.websocket), project_uuid))
            
            await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.project.delete, project_uuid)

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
            new_project_uuid = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.project.duplicate, project_uuid)
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
            await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.project.restore, project_uuid)
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
            
            await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.project.delete_from_trash, project_uuid)

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
            file_list = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.media.list)    
            await self.outgoing.put(json.dumps({"type": action, "value": file_list}))
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e),  action=action)

    async def received_file_data(self, data, action):
        try:
            file_uuid = data['uuid']

            Logger.info("user {} update file data {}".format(id(self.websocket), file_uuid))
            
            return_message = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.media.save, file_uuid, data)
            await self.notify_user(uuid=file_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
            
    async def request_file_load_meta(self, file_uuid, action):
        try:

            Logger.info("user {} loading file meta data {}".format(id(self.websocket), file_uuid))
            
            file_meta_data = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.media.load_meta, file_uuid)
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
            
            file_thumbnail = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.media.load_thumbnail, file_uuid)
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
            
            file_waveform = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.media.load_waveform, file_uuid)
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
            file_trash_list = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.media.list_trash)    
            await self.outgoing.put(json.dumps({"type": action, "value": file_trash_list}))
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e),  action=action)


    async def request_delete_file(self, file_uuid, action):
        try:
            Logger.debug("user {} deleting file: {}".format(id(self.websocket), file_uuid))
            await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.media.delete, file_uuid)
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
            await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.media.restore, file_uuid)
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
            await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.media.delete_from_trash, file_uuid)
            await self.notify_user(uuid=file_uuid, action=action)
            await self.server.notify_others_list_changes(self, "file_trash_list")
        except NonExistentItemError as e:
            Logger.info(e)
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.notify_error_to_user(str(e), uuid=file_uuid, action=action)
