import asyncio
import asgiref.sync as asgi

asgi.SyncToAsync()
asgi.async_to_sync()
async def get_chat_id(name):
    await asyncio.sleep(3)
    return "chat-%s" % name

async def main():
    id_coroutine = get_chat_id("django")
    result = await id_coroutine
    print(result)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
# %%


#!/usr/bin/env python

# WS server example that synchronizes state across clients

import sys
import asyncio
import json
import logging
import websockets as ws

class CuemsWsServer:
    def def __init__(self, name, age):
      self.name = name
      self.age = age
      
    def __await__(self):
        # see: http://stackoverflow.com/a/33420721/1113207
        return self._async_init().__await__()

    async def _async_init(self):
        self._conn = ws.connect("wss://echo.websocket.org")
        self.websocket = await self._conn.__aenter__()
        return self

    async def close(self):
        await self._conn.__aexit__(*sys.exc_info())

    async def send(self, message):
        await self.websocket.send(message)

    async def receive(self):
        return await self.websocket.recv()


async def main():
    echo = await EchoWebsocket()
    try:
        await echo.send("what?!")
        print(await echo.receive())  # "Hello!"
    finally:
        await echo.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

