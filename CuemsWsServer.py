#!/usr/bin/env python

# WS server example that synchronizes state across clients

import sys
import asyncio
import json
import logging
import websockets as ws
import  threading as th
from asgiref.sync import async_to_sync, sync_to_async

import time

logging.basicConfig(format='Cuems:ws-server: (%(threadName)-9s)-(%(funcName)s) %(message)s', level=logging.INFO)


class CuemsWsServer():
    state = {"value": 0}
    users = dict()
    projects=[{"CuemsScript": {"uuid": "76861217-2d40-47a2-bdb5-8f9c91293855", "name": "Proyecto test 0", "date": "14/08/2020 11:18:16", "timecode_cuelist": {"CueList": {"Cue": [{"uuid": "bf2d217f-881d-47c1-9ad1-f5999769bcc5", "time": {"CTimecode": "00:00:33:00"}, "type": "mtc", "loop": "False", "outputs": {"CueOutputs": {"id": 5, "bla": "ble"}}}, {"uuid": "8ace53f3-74f5-4195-822e-93c12fdf3725", "time": {"NoneType": "None"}, "type": "floating", "loop": "False", "outputs": {"CueOutputs": {"physiscal": 1, "virtual": 3}}}], "AudioCue": {"uuid": "be288e38-887a-446f-8cbf-c16c9ec6724a", "time": {"CTimecode": "00:00:45:00"}, "type": "virtual", "loop": "True", "outputs": {"AudioCueOutputs": {"stereo": 1}}}}}, "floating_cuelist": {"CueList": {"DmxCue": {"uuid": "f36fa4b3-e220-4d75-bff1-210e14655c11", "time": {"CTimecode": "00:00:23:00"}, "dmx_scene": {"DmxScene": {"DmxUniverse": [{"id": 0, "DmxChannel": [{"id": 0, "&": 10}, {"id": 1, "&": 50}]}, {"id": 1, "DmxChannel": [{"id": 20, "&": 23}, {"id": 21, "&": 255}]}, {"id": 2, "DmxChannel": [{"id": 5, "&": 10}, {"id": 6, "&": 23}, {"id": 7, "&": 125}, {"id": 8, "&": 200}]}]}}, "outputs": {"DmxCueOutputs": {"universe0": 3}}}, "Cue": {"uuid": "17376d8f-84c6-4f28-859a-a01260a1dadb", "time": {"CTimecode": "00:00:05:00"}, "type": "virtual", "loop": "False", "outputs": {"CueOutputs": {"id": 3}}}}}}}, {"CuemsScript": {"uuid": "e05de59a-b281-4abf-83ba-97198d661a63", "name": "Segundo proyecto", "date": "13/08/2020 07:23:12", "timecode_cuelist": {"CueList": {"Cue": [{"uuid": "d47a75e2-f76e-4c77-b33e-e1df40ffdf02", "time": {"CTimecode": "00:00:33:00"}, "type": "mtc", "loop": "False", "outputs": {"CueOutputs": {"id": 5, "bla": "ble"}}}, {"uuid": "b5c35e3d-91f6-42d8-9825-0176354b44c1", "time": {"NoneType": "None"}, "type": "floating", "loop": "False", "outputs": {"CueOutputs": {"physiscal": 1, "virtual": 3}}}], "AudioCue": {"uuid": "aef5e289-03b0-4b39-99cd-90063d9b8c80", "time": {"CTimecode": "00:00:45:00"}, "type": "virtual", "loop": "True", "outputs": {"AudioCueOutputs": {"stereo": 1}}}}}, "floating_cuelist": {"CueList": {"DmxCue": {"uuid": "5d4ef443-5a49-4986-a283-9563ee7a9e85", "time": {"CTimecode": "00:00:23:00"}, "dmx_scene": {"DmxScene": {"DmxUniverse": [{"id": 0, "DmxChannel": [{"id": 0, "&": 10}, {"id": 1, "&": 50}]}, {"id": 1, "DmxChannel": [{"id": 20, "&": 23}, {"id": 21, "&": 255}]}, {"id": 2, "DmxChannel": [{"id": 5, "&": 10}, {"id": 6, "&": 23}, {"id": 7, "&": 125}, {"id": 8, "&": 200}]}]}}, "outputs": {"DmxCueOutputs": {"universe0": 3}}}, "Cue": {"uuid": "37f80125-1c41-4cce-aab1-13328dd8c94e", "time": {"CTimecode": "00:00:05:00"}, "type": "virtual", "loop": "False", "outputs": {"CueOutputs": {"id": 3}}}}}}}]

    def __init__(self):
        
        self.event_loop = asyncio.new_event_loop()
        self.event_loop.set_exception_handler(self.exception_handler)
        self.thread = th.Thread(target=self.run_async_server, daemon=False)
        

    def start(self, port):
        self.port = port
        self.host = 'localhost'
        self.thread.start()
        

    def run_async_server(self):
        asyncio.set_event_loop(self.event_loop)
        self.project_server = ws.serve(self.handle, self.host, self.port)
        logging.info('server listening on {}, port {}'.format(self.host, self.port))
        self.event_loop.run_until_complete(self.project_server)
        self.event_loop.run_forever()
        self.event_loop.close()

    def stop(self):
        self.event_loop.call_soon_threadsafe(self.project_server.ws_server.close)
        logging.info('ws server closing')
        asyncio.run_coroutine_threadsafe(self.stop_async(), self.event_loop)
        
        self.thread.join()
        logging.info('ws thread joined')

    async def stop_async(self):
        await self.project_server.ws_server.wait_closed()
        logging.info('ws server closed')
        self.event_loop.call_soon(self.event_loop.stop)
        logging.info('event loop stoped')

    async def handle(self, websocket, path):
        user_task = CuemsWsUser(websocket, path)
        await self.register(user_task)
        await user_task.outgoing.put(self.counter_event())
        try:
            consumer_task = asyncio.create_task(user_task.consumer_handler())
            producer_task = asyncio.create_task(user_task.producer_handler())
            processor_tasks = [asyncio.create_task(user_task.consumer()) for _ in range(3)] # start 3 message processing task so a load or any other time consuming action still leaves with 2 tasks running  and interface feels responsive. TODO:discuss this
            done_tasks, pending_tasks = await asyncio.wait([consumer_task, producer_task, *processor_tasks], return_when=asyncio.FIRST_COMPLETED)
            
            
            for task in pending_tasks:
                task.cancel()

        finally:
            await self.unregister(user_task)


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


class CuemsWsUser(CuemsWsServer):
    def __init__(self, websocket, path):
        self.websocket = websocket
        self.path = path
        self.incoming = asyncio.Queue()
        self.outgoing = asyncio.Queue()
        self.users[self] = None

        

        

    async def consumer_handler(self):
        async for message in self.websocket:
            await self.incoming.put(message)

    async def producer_handler(self):
        while True:
            message = await self.producer()
            await self.websocket.send(message)

    async def consumer(self):
        while True: 
            message = await self.incoming.get()
            data = json.loads(message)
            if data["action"] == "minus":
                self.state["value"] -= 1
                await self.notify_state()
            elif data["action"] == "plus":
                self.state["value"] += 1
                await self.notify_state()
            elif data["action"] == "load":
                await self.send_project(data["value"])
            elif data["action"] == "save":
                await self.received_project(data["value"])
            elif data["action"] == "delete":
                await self.request_delete(data["value"])
            elif data["action"] == "list":
                await self.list_projects()
            else:
                logging.error("unsupported event: {}".format(data))
                await self.notify_error_to_user("unsupported event: {}".format(data))

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
            project_list = await self.load_project_list()    
            await self.outgoing.put(json.dumps({"type": "list", "value": project_list}))
        except Exception as e:
            print("error loading project list")
            print("error: {} {}".format(type(e), e))
            await self.notify_error_to_user('error loading project list')

    async def send_project(self, project_uuid):
        try:
            if project_uuid == '':
                raise NameError
            logging.info("user {} loading project {}".format(id(self.websocket), project_uuid))
            project = await self.load_project(project_uuid)
            msg = json.dumps({"type":"project", "value":json.dumps(project)})
            await self.outgoing.put(msg)
            await self.notify_user("project loaded")
            self.users[self] = project_uuid
        except Exception as e:
            print("error loading project")
            print("error: {} {}".format(type(e), e))
            await self.notify_error_to_user('error loading project')

    async def received_project(self, data):
        try:
            project = json.loads(data)
            project_uuid = project['CuemsScript']['uuid']

            logging.info("user {} saving project {}".format(id(self.websocket), project_uuid))
            
            return_message = await self.save_project(project_uuid, project)
            self.users[self] = project_uuid
            await self.notify_user("{} project saved".format(return_message))
            await self.notify_others(self, "changes")
        except Exception as e:
            print("error saving project")
            print("error: {} {}".format(type(e), e))
            await self.notify_error_to_user('error saving project')

    async def request_delete(self, project_uuid):
        try:
            logging.info("user {} deleting project: {}".format(id(self.websocket), project_uuid))
            
            if (await self.delete_project(project_uuid)):

                # self.users[self] = None  #TODO:what is now the active project? deleted project was the active one?
                await self.notify_user("project {} deleted".format(project_uuid))
                await self.notify_others(self, "changes", project_uuid=project_uuid)
        except Exception as e:
            print("error deleting project")
            print("error: {} {}".format(type(e), e))
            await self.notify_error_to_user('error deleting project')




    @sync_to_async # call blocking function asynchronously (gets a thread)
    def load_project_list(self):
        logging.info("loading project list")
        project_list = list()
        for project in self.projects:
            try:
                project_list.append({project['CuemsScript']['uuid']:{"name":project['CuemsScript']['name'], "date":project['CuemsScript']['date']}})
            except:
                print('malformed project')
        return project_list

    @sync_to_async # call blocking function asynchronously (gets a thread)
    def load_project(self, project):
        logging.info("loading project: {}".format(project))
        for elem in self.projects:
            if elem['CuemsScript']['uuid'] == project:
                logging.debug("loading project: {}".format(elem))
                return elem
        raise NameError

    @sync_to_async
    def save_project(self, uuid, data):
        logging.info("loading project: {}".format(uuid))
        logging.debug('saving project, uuid:{}, data:{}'.format(uuid, data))
        for num, elem in enumerate(self.projects):
            if elem['CuemsScript']['uuid'] == uuid:
                self.projects[num] = data
                return 'updated'

        self.projects.append(data)
        return 'new'

    @sync_to_async
    def delete_project(self, uuid):
        logging.info('deleting project, uuid:{}'.format(uuid))
        for num, elem in enumerate(self.projects):
            if elem['CuemsScript']['uuid'] == uuid:
                del self.projects[num]
                return True

        raise NameError