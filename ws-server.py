#!/usr/bin/env python

# WS server example that synchronizes state across clients

import asyncio
import json
import logging
import websockets

logging.basicConfig(level=logging.INFO)

STATE = {"value": 0}

USERS = dict()

PROJECT = {0: {'Cue': [{'time': None, 'type': 'virtual', 'loop': 'False'}, {'time': None, 'type': 'floating', 'loop': 'False'}, {'time': None, 'type': 'virtual', 'loop': 'False'}], 'AudioCue': [{'time': None, 'type': 'virtual', 'loop': 'False'}], 'DmxCue': [{'time': None, 'dmx_scene': {'DmxUniverse': [{'@id': 0, 'DmxChannel': [{'@id': 0, '$': 10}, {'@id': 1, '$': 50}]}, {'@id': 1, 'DmxChannel': [{'@id': 20, '$': 23}, {'@id': 21, '$': 255}]}, {'@id': 2, 'DmxChannel': [{'@id': 5, '$': 10}, {'@id': 6, '$': 23}, {'@id': 7, '$': 125}, {'@id': 8, '$': 200}]}]}}]}
, 1: "bla"}

def save_project(project, data):
    global PROJECT
    PROJECT[project] = data

def load_project(project):
    return PROJECT[int(project)]

def counter_event():
    return json.dumps({"type": "counter", **STATE})


def users_event(type):
    if type == "users":
        return json.dumps({"type": type, "count": len(USERS)})
    elif type == "changes":
        return json.dumps({"type": "state", "value" : "project modified in server"})


async def send_project(websocket, project):
    logging.info("user {} loading project {}".format(id(websocket), project))
    msg = json.dumps({"type":"msg", "value":json.dumps(load_project(project))})
    await websocket.send(msg)
    await notify_user(websocket, "project loaded")
    USERS[websocket] = project

async def received_project(websocket, data):
    save_project(USERS[websocket], json.loads(data))
    logging.info("user {} saving project {} : {}".format(id(websocket), USERS[websocket], data))
    await notify_user(websocket, "project saved")
    await notify_others(websocket, "changes")

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
                await send_project(websocket, data["data"])
            elif data["action"] == "save":
                await received_project(websocket, data["data"])
            else:
                logging.error("unsupported event: {}".format(data))
    finally:
        await unregister(websocket)


start_server = websockets.serve(counter, "localhost", 6789)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()