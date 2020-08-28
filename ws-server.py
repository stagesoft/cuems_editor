#!/usr/bin/env python

# WS server example that synchronizes state across clients

import asyncio
import json
import logging
import websockets

logging.basicConfig(level=logging.INFO)

STATE = {"value": 0}

USERS = dict()

PROJECTS = [{"CuemsScript": {"uuid": "76861217-2d40-47a2-bdb5-8f9c91293855", "name": "Proyecto test 0", "date": "14/08/2020 11:18:16", "timecode_cuelist": {"CueList": {"Cue": [{"uuid": "bf2d217f-881d-47c1-9ad1-f5999769bcc5", "time": {"CTimecode": "00:00:33:00"}, "type": "mtc", "loop": "False", "outputs": {"CueOutputs": {"id": 5, "bla": "ble"}}}, {"uuid": "8ace53f3-74f5-4195-822e-93c12fdf3725", "time": {"NoneType": "None"}, "type": "floating", "loop": "False", "outputs": {"CueOutputs": {"physiscal": 1, "virtual": 3}}}], "AudioCue": {"uuid": "be288e38-887a-446f-8cbf-c16c9ec6724a", "time": {"CTimecode": "00:00:45:00"}, "type": "virtual", "loop": "True", "outputs": {"AudioCueOutputs": {"stereo": 1}}}}}, "floating_cuelist": {"CueList": {"DmxCue": {"uuid": "f36fa4b3-e220-4d75-bff1-210e14655c11", "time": {"CTimecode": "00:00:23:00"}, "dmx_scene": {"DmxScene": {"DmxUniverse": [{"id": 0, "DmxChannel": [{"id": 0, "&": 10}, {"id": 1, "&": 50}]}, {"id": 1, "DmxChannel": [{"id": 20, "&": 23}, {"id": 21, "&": 255}]}, {"id": 2, "DmxChannel": [{"id": 5, "&": 10}, {"id": 6, "&": 23}, {"id": 7, "&": 125}, {"id": 8, "&": 200}]}]}}, "outputs": {"DmxCueOutputs": {"universe0": 3}}}, "Cue": {"uuid": "17376d8f-84c6-4f28-859a-a01260a1dadb", "time": {"CTimecode": "00:00:05:00"}, "type": "virtual", "loop": "False", "outputs": {"CueOutputs": {"id": 3}}}}}}}
, {"CuemsScript": {"uuid": "e05de59a-b281-4abf-83ba-97198d661a63", "name": "Segundo proyecto", "date": "13/08/2020 07:23:12", "timecode_cuelist": {"CueList": {"Cue": [{"uuid": "d47a75e2-f76e-4c77-b33e-e1df40ffdf02", "time": {"CTimecode": "00:00:33:00"}, "type": "mtc", "loop": "False", "outputs": {"CueOutputs": {"id": 5, "bla": "ble"}}}, {"uuid": "b5c35e3d-91f6-42d8-9825-0176354b44c1", "time": {"NoneType": "None"}, "type": "floating", "loop": "False", "outputs": {"CueOutputs": {"physiscal": 1, "virtual": 3}}}], "AudioCue": {"uuid": "aef5e289-03b0-4b39-99cd-90063d9b8c80", "time": {"CTimecode": "00:00:45:00"}, "type": "virtual", "loop": "True", "outputs": {"AudioCueOutputs": {"stereo": 1}}}}}, "floating_cuelist": {"CueList": {"DmxCue": {"uuid": "5d4ef443-5a49-4986-a283-9563ee7a9e85", "time": {"CTimecode": "00:00:23:00"}, "dmx_scene": {"DmxScene": {"DmxUniverse": [{"id": 0, "DmxChannel": [{"id": 0, "&": 10}, {"id": 1, "&": 50}]}, {"id": 1, "DmxChannel": [{"id": 20, "&": 23}, {"id": 21, "&": 255}]}, {"id": 2, "DmxChannel": [{"id": 5, "&": 10}, {"id": 6, "&": 23}, {"id": 7, "&": 125}, {"id": 8, "&": 200}]}]}}, "outputs": {"DmxCueOutputs": {"universe0": 3}}}, "Cue": {"uuid": "37f80125-1c41-4cce-aab1-13328dd8c94e", "time": {"CTimecode": "00:00:05:00"}, "type": "virtual", "loop": "False", "outputs": {"CueOutputs": {"id": 3}}}}}}}
]

def save_project(project, data):
    global PROJECTS
    for num, elem in enumerate(PROJECTS):
        if elem['CuemsScript']['uuid'] == project:
            PROJECTS[num] = data
            return True

def load_project(project):
    ret = None
    for elem in PROJECTS:
        if elem['CuemsScript']['uuid'] == project:
            ret = elem
    return ret

 


def counter_event():
    return json.dumps({"type": "counter", **STATE})


def users_event(type):
    if type == "users":
        return json.dumps({"type": type, "count": len(USERS)})
    elif type == "changes":
        return json.dumps({"type": "state", "value" : "project modified in server"})

async def list_projects(websocket):
    project_list = list()
    for project in PROJECTS:
        project_list.append({project['CuemsScript']['uuid']:{"name":project['CuemsScript']['name'], "date":project['CuemsScript']['date']}})
        
    await websocket.send(json.dumps({"type": "list", "value": project_list}))

async def send_project(websocket, project_uuid):
    try:
        print(project_uuid)
        if project_uuid == '':
            raise NameError
        logging.info("user {} loading project {}".format(id(websocket), project_uuid))
        msg = json.dumps({"type":"project", "value":json.dumps(load_project(project_uuid))})
        await websocket.send(msg)
        await notify_user(websocket, "project loaded")
        USERS[websocket] = project_uuid
    except:
        print("error loading project")

async def received_project(websocket, data):
    try:
        logging.info("user {} saving project {} : {}".format(id(websocket), USERS[websocket], data))
        if (save_project(USERS[websocket], json.loads(data))):
            await notify_user(websocket, "project saved")
            await notify_others(websocket, "changes")
    except:
        print("error saving project")

async def notify_state():
    if USERS:  # asyncio.wait doesn't accept an empty dcit
        message = counter_event()
        await asyncio.wait([user.send(message) for user in USERS])


async def notify_users(type):
    if USERS:  # asyncio.wait doesn't accept an empty dcit
        message = users_event(type)
        await asyncio.wait([user.send(message) for user in USERS])

async def notify_others(websocket, type):
    if USERS:  #notify others, not the user trigering the action, and only if the have same project loaded
        message = users_event(type)
        for user, project in USERS.items():
            if user is not websocket:
                if project is USERS[websocket]:
                    await user.send(message)

async def notify_user(websocket, msg):
    await websocket.send(json.dumps({"type": "state", "value":msg}))


async def register(websocket):
    logging.info("user registered: {}".format(id(websocket)))
    USERS[websocket] = None
    await notify_users("users")


async def unregister(websocket):
    logging.info("user unregistered: {}".format(id(websocket)))
    USERS.pop(websocket, None)
    await notify_users("users")


async def counter(websocket, path):
    # register(websocket) sends user_event() to websocket
    await register(websocket)
    try:
        await websocket.send(counter_event())
        async for message in websocket:
            data = json.loads(message)
            if data["action"] == "minus":
                STATE["value"] -= 1
                await notify_state()
            elif data["action"] == "plus":
                STATE["value"] += 1
                await notify_state()
            elif data["action"] == "load":
                await send_project(websocket, data["value"])
            elif data["action"] == "save":
                await received_project(websocket, data["value"])
            elif data["action"] == "list":
                await list_projects(websocket)
            else:
                logging.error("unsupported event: {}".format(data))
    finally:
        await unregister(websocket)


start_server = websockets.serve(counter, "localhost", 9092)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()