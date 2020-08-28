#!/usr/bin/env python

# WS server example that synchronizes state across clients

import sys
import asyncio
import json
import logging
import websockets as ws
from asgiref.sync import async_to_sync, sync_to_async

import time

logging.basicConfig(format='Cuems:ws-server: (%(threadName)-9s)-(%(funcName)s) %(message)s', level=logging.INFO)


class CuemsWsServer:
    state = {"value": 0}
    users = dict()
    projects = None

    def __init__(self):
        
        self.event_loop = asyncio.get_event_loop()


        

    def start(self, port):
        self.port = port
        self.host = 'localhost'
        start_server = ws.serve(self.handle, self.host, self.port)
        print('server listening on {}, port {}'.format(self.host, self.port))
        self.event_loop.run_until_complete(start_server)
        self.event_loop.run_forever()
        return start_server

    async def handle(self, websocket, path):
        logging.info('ws: {}, path: {}'.format(websocket, path))
        user_task = CuemsWsUser(websocket, path)
        await self.register(user_task)
        await user_task.outgoing.put(self.counter_event())
        try:
            consumer_task = asyncio.create_task(user_task.consumer_handler())
            producer_task = asyncio.create_task(user_task.producer_handler())
            processor_task = asyncio.create_task(user_task.consumer())
            done, pending = await asyncio.wait([consumer_task, producer_task, processor_task], return_when=asyncio.FIRST_COMPLETED)
            
            
            for task in pending:
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
                print('{} ->>OUT puttting new message into the outgoing queue'.format(user))


           # await asyncio.wait([user.outgoing.put(message) for user in self.users])
           # await asyncio.wait([user.send(message) for user in self.users])
            
    async def notify_others(self, calling_user, type):
        if self.users:  #notify others, not the user trigering the action, and only if the have same project loaded
            message = self.users_event(type)
            for user, project in self.users.items():
                if user is not calling_user:
                    if project is self.users[calling_user]:
                        await user.outgoing.put(message)

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


class CuemsWsUser(CuemsWsServer):
    def __init__(self, websocket, path):
        self.websocket = websocket
        self.path = path
        self.incoming = asyncio.Queue()
        self.outgoing = asyncio.Queue()
        super().__init__()
        print(self.state)

        

    async def consumer_handler(self):
        async for message in self.websocket:
            print('{} -->>IN putting new message into the incoming queue'.format(self))
            await self.incoming.put(message)

    async def producer_handler(self):
        while True:
            message = await self.producer()
            await self.websocket.send(message)

    async def consumer(self):
        print('here we go')
        while True: 
            message = await self.incoming.get()
            print('{} <<--IN getting new message from the incoming queue'.format(self))
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
            elif data["action"] == "list":
                await self.list_projects()
            else:
                logging.error("unsupported event: {}".format(data))

    async def producer(self):
        while True:
            message = await self.outgoing.get()
            print('{} <<--OUT getting new message from the outgoing queue'.format(self))
            return message

    async def notify_user(self, msg):
        await self.outgoing.put(json.dumps({"type": "state", "value":msg}))

    async def list_projects(self):
        
        project_list = await self.load_project_list()    
        await self.outgoing.put(json.dumps({"type": "list", "value": project_list}))

    async def send_project(self, project_uuid):
        try:
            print(project_uuid)
            if project_uuid == '':
                raise NameError
            logging.info("user {} loading project {}".format(id(self.websocket), project_uuid))
            project = await self.load_project(project_uuid)
            msg = json.dumps({"type":"project", "value":json.dumps(project)})
            await self.outgoing.put(msg)
            await self.notify_user("project loaded")
            self.users[self] = project_uuid
        except:
            print("error loading project")

    async def received_project(self, data):
        try:
            logging.info("user {} saving project {} : {}".format(id(self.websocket), self.users[self], data))
            if (self.save_project(self.users[self], json.loads(data))):
                await self.notify_user("project saved")
                await self.notify_others(self, "changes")
        except:
            print("error saving project")



    @sync_to_async # call blocking function asynchronously (gets a thread)
    def load_project_list(self):
        logging.info("loading project list")
        time.sleep(8)
        
        self.projects=[{"CuemsScript": {"uuid": "76861217-2d40-47a2-bdb5-8f9c91293855", "name": "Proyecto test 0", "date": "14/08/2020 11:18:16", "timecode_cuelist": {"CueList": {"Cue": [{"uuid": "bf2d217f-881d-47c1-9ad1-f5999769bcc5", "time": {"CTimecode": "00:00:33:00"}, "type": "mtc", "loop": "False", "outputs": {"CueOutputs": {"id": 5, "bla": "ble"}}}, {"uuid": "8ace53f3-74f5-4195-822e-93c12fdf3725", "time": {"NoneType": "None"}, "type": "floating", "loop": "False", "outputs": {"CueOutputs": {"physiscal": 1, "virtual": 3}}}], "AudioCue": {"uuid": "be288e38-887a-446f-8cbf-c16c9ec6724a", "time": {"CTimecode": "00:00:45:00"}, "type": "virtual", "loop": "True", "outputs": {"AudioCueOutputs": {"stereo": 1}}}}}, "floating_cuelist": {"CueList": {"DmxCue": {"uuid": "f36fa4b3-e220-4d75-bff1-210e14655c11", "time": {"CTimecode": "00:00:23:00"}, "dmx_scene": {"DmxScene": {"DmxUniverse": [{"id": 0, "DmxChannel": [{"id": 0, "&": 10}, {"id": 1, "&": 50}]}, {"id": 1, "DmxChannel": [{"id": 20, "&": 23}, {"id": 21, "&": 255}]}, {"id": 2, "DmxChannel": [{"id": 5, "&": 10}, {"id": 6, "&": 23}, {"id": 7, "&": 125}, {"id": 8, "&": 200}]}]}}, "outputs": {"DmxCueOutputs": {"universe0": 3}}}, "Cue": {"uuid": "17376d8f-84c6-4f28-859a-a01260a1dadb", "time": {"CTimecode": "00:00:05:00"}, "type": "virtual", "loop": "False", "outputs": {"CueOutputs": {"id": 3}}}}}}}, {"CuemsScript": {"uuid": "e05de59a-b281-4abf-83ba-97198d661a63", "name": "Segundo proyecto", "date": "13/08/2020 07:23:12", "timecode_cuelist": {"CueList": {"Cue": [{"uuid": "d47a75e2-f76e-4c77-b33e-e1df40ffdf02", "time": {"CTimecode": "00:00:33:00"}, "type": "mtc", "loop": "False", "outputs": {"CueOutputs": {"id": 5, "bla": "ble"}}}, {"uuid": "b5c35e3d-91f6-42d8-9825-0176354b44c1", "time": {"NoneType": "None"}, "type": "floating", "loop": "False", "outputs": {"CueOutputs": {"physiscal": 1, "virtual": 3}}}], "AudioCue": {"uuid": "aef5e289-03b0-4b39-99cd-90063d9b8c80", "time": {"CTimecode": "00:00:45:00"}, "type": "virtual", "loop": "True", "outputs": {"AudioCueOutputs": {"stereo": 1}}}}}, "floating_cuelist": {"CueList": {"DmxCue": {"uuid": "5d4ef443-5a49-4986-a283-9563ee7a9e85", "time": {"CTimecode": "00:00:23:00"}, "dmx_scene": {"DmxScene": {"DmxUniverse": [{"id": 0, "DmxChannel": [{"id": 0, "&": 10}, {"id": 1, "&": 50}]}, {"id": 1, "DmxChannel": [{"id": 20, "&": 23}, {"id": 21, "&": 255}]}, {"id": 2, "DmxChannel": [{"id": 5, "&": 10}, {"id": 6, "&": 23}, {"id": 7, "&": 125}, {"id": 8, "&": 200}]}]}}, "outputs": {"DmxCueOutputs": {"universe0": 3}}}, "Cue": {"uuid": "37f80125-1c41-4cce-aab1-13328dd8c94e", "time": {"CTimecode": "00:00:05:00"}, "type": "virtual", "loop": "False", "outputs": {"CueOutputs": {"id": 3}}}}}}}]
        project_list = list()
        for project in self.projects:
            project_list.append({project['CuemsScript']['uuid']:{"name":project['CuemsScript']['name'], "date":project['CuemsScript']['date']}})
        
        return project_list

    @sync_to_async # call blocking function asynchronously (gets a thread)
    def load_project(self, project):
        logging.info("loading project: {}".format(project))
        time.sleep(5)
        ret = None
        for elem in self.projects:
            if elem['CuemsScript']['uuid'] == project:
                ret = elem
        return ret

    @sync_to_async
    def save_project(self, project, data):

        time.sleep(6.6)
        for num, elem in enumerate(self.projects):
            if elem['CuemsScript']['uuid'] == project:
                self.projects[num] = data
                return True





server = CuemsWsServer().start(9092)