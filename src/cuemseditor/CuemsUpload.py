import os
import json
import aiofiles
from hashlib import md5
from random import randint
import websockets as ws


from cuemsutils.log import logged, Logger
from cuemsutils.tools.StringSanitizer import StringSanitizer

from cuemseditor.CuemsErrors import *



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
        self.tmp_path = self.server.settings_dict['tmp_path']
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
                Logger.debug('upload connection closed, exiting loop')
                break

    async def message_sender(self, message):
        try:
            await self.websocket.send(message)
        except (ws.exceptions.ConnectionClosed, ws.exceptions.ConnectionClosedOK, ws.exceptions.ConnectionClosedError) as e:
                Logger.debug(e)

    async def process_upload_message(self, message):
        data = json.loads(message)
        if 'action' not in data:
            return False
        if data['action'] == 'upload':
            await self.set_upload(file_info=data["value"])

    async def set_upload(self, file_info):
        
        if not os.path.exists(self.media_path):
            Logger.error("upload folder doenst exists")
            await self.message_sender(json.dumps({'error' : 'upload folder doenst exist', 'fatal': True}))
            return False
        
        self.filename = StringSanitizer.sanitize_file_name(file_info['name'])
        self.tmp_filename = self.filename + '.tmp' + str(randint(100000, 999999))
        Logger.debug('tmp upload path: {}'.format(self.tmp_file_path()))

        if not os.path.exists(self.tmp_file_path()):
            self.filesize = file_info['size']
            self.uploading = 'Ready'
            await self.message_sender(json.dumps({"ready" : True}))
        else:
            await self.message_sender(json.dumps({'error' : 'file already exists', 'fatal': True}))
            Logger.error("file already exists")

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
            
            dest_filename = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.media.new,  self.tmp_file_path(), self.filename)
            self.tmp_filename = None
            Logger.debug('upload completed')
            await self.server.event_loop.run_in_executor(self.server.executor, self.check_if_media_existed,  dest_filename)
            await self.message_sender(json.dumps({"close" : True}))
            await self.server.notify_others_list_changes(None, "file_list")
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.message_sender(json.dumps({'error' : 'error saving file', 'fatal': True}))

### SYNC METHODS
    def check_if_media_existed(self, filename):
        Logger.debug(f"checking if file {filename} already existed in projects")
        projectmedia_list = self.server.db.media.check_if_media_existed_in_projects(filename)
        if projectmedia_list:
            for projectmedia in projectmedia_list:
                Logger.info("file {} already existed in project: {}".format(filename, projectmedia.project_id))
                self.server.db.project.update_projects_existed_media(projectmedia.project_id, filename)
        else:
            Logger.debug("file did not exist in any project, no action needed")

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
            return os.path.join(self.tmp_path, self.tmp_filename)

    def __del__(self):
        try:
            if self.tmp_file_path():
                os.remove(self.tmp_file_path())  # TODO: change to pathlib ?  
                Logger.debug('cleaning tmp upload file on object destruction: ({})'.format(self.tmp_file_path()))
        except FileNotFoundError:
            pass
