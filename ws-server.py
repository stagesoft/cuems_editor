#!/usr/bin/env python

# WS server example that synchronizes state across clients

import asyncio
import json
import logging
import websockets

logging.basicConfig(level=logging.INFO)

STATE = {"value": 0}

USERS = set()

PROJECT = "hola"

def save_project(data):
    global PROJECT
    PROJECT = data

def load_project():
    return PROJECT

def counter_event():
    return json.dumps({"type": "counter", **STATE})


def users_event(type):
    if type == "users":
        return json.dumps({"type": type, "count": len(USERS)})
    elif type == "changes":
        return json.dumps({"type": "state", "value" : "project modified in server"})


async def send_project(websocket):
    msg = json.dumps({"type":"msg", "value":load_project()})
    await websocket.send(msg)

async def received_project(websocket, data):
    save_project(data)
    logging.info("saving data : {}".format(data))
    await notify_user(websocket, "project saved")
    await notify_others(websocket, "changes")

async def notify_state():
    if USERS:  # asyncio.wait doesn't accept an empty list
        message = counter_event()
        await asyncio.wait([user.send(message) for user in USERS])


async def notify_users(type):
    if USERS:  # asyncio.wait doesn't accept an empty list
        message = users_event(type)
        await asyncio.wait([user.send(message) for user in USERS])

async def notify_others(websocket, type):
    if USERS:  # asyncio.wait doesn't accept an empty list
        message = users_event(type)
        for user in USERS:
            if user is not websocket:
               await user.send(message)

async def notify_user(websocket, msg):
    await websocket.send(json.dumps({"type": "state", "value":msg}))


async def register(websocket):
    logging.info("user registered: {}".format(websocket))
    USERS.add(websocket)
    await notify_users("users")


async def unregister(websocket):
    logging.info("user unregistered: {}".format(websocket))
    USERS.remove(websocket)
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
                await send_project(websocket)
            elif data["action"] == "save":
                await received_project(websocket, data["data"])
            else:
                logging.error("unsupported event: {}".format(data))
    finally:
        await unregister(websocket)


start_server = websockets.serve(counter, "localhost", 6789)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()