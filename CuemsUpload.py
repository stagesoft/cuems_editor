import os
import json
import aiofiles
from hashlib import md5
from random import randint
import websockets as ws


from .CuemsUtils import StringSanitizer
from .CuemsErrors import *
from ..log import *



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
                        data = json.loads(message)
                        if 'action' not in data:
                            return False
                        if data['action'] == 'finished':
                            await stream.flush()
                            await stream.close()
                            await self.upload_done(data["value"])
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

        hash_md5 = md5()
        with open(path, "rb") as file_to_check:
            for chunk in iter(lambda: file_to_check.read(65536), b""):
                hash_md5.update(chunk)
        
        returned_md5 = hash_md5.hexdigest()
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
