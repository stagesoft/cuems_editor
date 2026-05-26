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
    """Async binary upload handler for the ``/upload`` WebSocket endpoint.

    Implements a two-phase upload protocol over a dedicated WebSocket
    connection (separate from the main project-manager connection):

    1. **Init phase** — client sends a JSON frame:
       ``{"action": "upload", "value": {"name": "...", "size": <bytes>}}``.
       Server responds ``{"ready": true}`` and transitions to the data phase.

    2. **Data phase** — client sends raw binary frames; server acknowledges
       each with ``{"ready": true}``.  When all bytes are received the client
       sends ``{"action": "finished", "value": "<md5-hex>"}``; the server
       verifies integrity, moves the file into the media library via
       ``CuemsDBMedia.new``, and sends ``{"close": true}``.

    The tmp file is automatically deleted if the upload fails or the object
    is garbage-collected.

    Example:
        >>> # Inside CuemsWsServer.upload_session:
        >>> upload = CuemsUpload(server, websocket)
        >>> await upload.message_handler()
    """

    uploading = False
    filename = None
    tmp_filename = None
    bytes_received = 0
    filesize = 0
    file_handle = None

    def __init__(self, server, websocket):
        """Bind the upload session to *server* and *websocket*.

        Args:
            server: The ``CuemsWsServer`` instance; provides ``settings_dict``,
                ``event_loop``, ``executor``, and ``db``.
            websocket: The WebSocket connection for the ``/upload`` path.
        """
        self.server = server
        self.websocket = websocket
        self.tmp_path = self.server.settings_dict['tmp_path']
        self.media_path = self.server.db.media.media_path

    async def message_handler(self):
        """Main receive loop for the upload WebSocket connection.

        Dispatches string frames to :meth:`process_upload_message` and binary
        frames to :meth:`process_upload_packet`.  Exits cleanly when the
        WebSocket is closed.
        """
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
        """Send a JSON string frame to the client, ignoring closed-connection errors.

        Args:
            message: JSON-serialised string to send.
        """
        try:
            await self.websocket.send(message)
        except (ws.exceptions.ConnectionClosed, ws.exceptions.ConnectionClosedOK, ws.exceptions.ConnectionClosedError) as e:
            Logger.debug(e)

    async def process_upload_message(self, message):
        """Dispatch a JSON control frame from the client.

        Handles the ``"upload"`` action by delegating to :meth:`set_upload`.
        Unknown actions are silently ignored.

        Args:
            message: Raw JSON string received from the WebSocket.

        Returns:
            ``False`` if the frame has no ``action`` key; otherwise ``None``.
        """
        data = json.loads(message)
        if 'action' not in data:
            return False
        if data['action'] == 'upload':
            await self.set_upload(file_info=data["value"])

    async def set_upload(self, file_info):
        """Initialise upload state and signal readiness to the client.

        Sanitises the filename, creates a unique tmp filename
        ``<name>.tmp<random>``, verifies the tmp path is accessible, and
        sends ``{"ready": true}`` to the client.

        Args:
            file_info: Dict from the client init frame with at minimum
                ``name`` (str) and ``size`` (int in bytes).

        Returns:
            ``False`` on error (error message already sent to client).
        """
        if not os.path.exists(self.media_path):
            Logger.error("upload folder doenst exists")
            await self.message_sender(json.dumps({'error': 'upload folder doenst exist', 'fatal': True}))
            return False

        self.filename = StringSanitizer.sanitize_file_name(file_info['name'])
        self.tmp_filename = self.filename + '.tmp' + str(randint(100000, 999999))
        Logger.debug('tmp upload path: {}'.format(self.tmp_file_path()))

        if not os.path.exists(self.tmp_file_path()):
            self.filesize = file_info['size']
            self.uploading = 'Ready'
            await self.message_sender(json.dumps({"ready": True}))
        else:
            await self.message_sender(json.dumps({'error': 'file already exists', 'fatal': True}))
            Logger.error("file already exists")

    async def process_upload_packet(self, bin_data):
        """Receive and assemble binary data frames into the tmp file.

        Opens the tmp file in write-binary mode and loops, writing each
        incoming binary frame and ACKing it with ``{"ready": true}``.
        When the client sends the ``"finished"`` JSON control frame the loop
        exits and :meth:`upload_done` is called.

        Args:
            bin_data: First binary frame received (written before the loop).

        Returns:
            ``False`` if no ``action`` key is present in the finish frame.
        """
        if self.uploading == 'Ready':
            async with aiofiles.open(self.tmp_file_path(), mode='wb', loop=self.server.event_loop, executor=self.server.executor) as stream:
                await stream.write(bin_data)
                self.bytes_received += len(bin_data)
                await self.message_sender(json.dumps({"ready": True}))

                while True:
                    message = await self.websocket.recv()
                    if isinstance(message, bytes):
                        await stream.write(message)
                        self.bytes_received += len(message)
                        await self.message_sender(json.dumps({"ready": True}))
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
        """Verify upload integrity and move the file into the media library.

        Steps:

        1. :meth:`check_file_integrity` — verifies the MD5 digest of the tmp
           file against *received_md5*.
        2. ``CuemsDBMedia.new`` — moves the tmp file to the media library and
           creates the DB record and sidecars.
        3. :meth:`check_if_media_existed` — re-links projects that already
           referenced the uploaded filename.
        4. Sends ``{"close": true}`` to the client.

        Args:
            received_md5: Hex MD5 digest string sent by the client.
        """
        try:
            await self.server.event_loop.run_in_executor(self.server.executor, self.check_file_integrity, self.tmp_file_path(), received_md5)

            dest_filename = await self.server.event_loop.run_in_executor(self.server.executor, self.server.db.media.new, self.tmp_file_path(), self.filename)
            self.tmp_filename = None
            Logger.debug('upload completed')
            await self.server.event_loop.run_in_executor(self.server.executor, self.check_if_media_existed, dest_filename)
            await self.message_sender(json.dumps({"close": True}))
            await self.server.notify_others_list_changes(None, "file_list")
        except Exception as e:
            Logger.error("error: {} {}".format(type(e), e))
            await self.message_sender(json.dumps({'error': 'error saving file', 'fatal': True}))

    # --- Synchronous helpers (run via executor) ---

    def check_if_media_existed(self, filename):
        """Re-link projects that already referenced *filename* before this upload.

        Queries ``ProjectMedia`` for rows whose ``media_filename`` matches
        *filename*, then calls
        ``CuemsDBProject.update_projects_existed_media`` for each referencing
        project so their XML UUIDs stay consistent.

        Args:
            filename: Destination filename returned by ``CuemsDBMedia.new``.
        """
        Logger.debug(f"checking if file {filename} already existed in projects")
        projectmedia_list = self.server.db.media.check_if_media_existed_in_projects(filename)
        if projectmedia_list:
            for projectmedia in projectmedia_list:
                Logger.info("file {} already existed in project: {}".format(filename, projectmedia.project_id))
                self.server.db.project.update_projects_existed_media(projectmedia.project_id, filename)
        else:
            Logger.debug("file did not exist in any project, no action needed")

    def check_file_integrity(self, path, original_md5):
        """Verify the MD5 digest of *path* against *original_md5*.

        Reads the file in 64 KiB chunks to avoid loading large uploads into
        memory all at once.

        Args:
            path: Absolute path to the tmp file.
            original_md5: Hex MD5 string provided by the client.

        Returns:
            ``True`` if the digests match.

        Raises:
            FileIntegrityError: If the computed digest differs from
                *original_md5*.

        Example:
            >>> upload.check_file_integrity("/tmp/cuems/video.tmp123456", "d41d8cd98f00b204e9800998ecf8427e")
            True
        """
        hash_md5 = md5()
        with open(path, "rb") as file_to_check:
            for chunk in iter(lambda: file_to_check.read(65536), b""):
                hash_md5.update(chunk)

        returned_md5 = hash_md5.hexdigest()
        if original_md5 != returned_md5:
            raise FileIntegrityError('MD5 mistmatch')

        return True

    def tmp_file_path(self):
        """Return the absolute path to the current tmp upload file.

        Returns:
            Absolute path string, or ``None`` if no upload is in progress.
        """
        if not self.tmp_filename is None:
            return os.path.join(self.tmp_path, self.tmp_filename)

    def __del__(self):
        """Clean up the tmp file if the upload session is destroyed mid-transfer."""
        try:
            if self.tmp_file_path():
                os.remove(self.tmp_file_path())  # TODO: change to pathlib?
                Logger.debug('cleaning tmp upload file on object destruction: ({})'.format(self.tmp_file_path()))
        except FileNotFoundError:
            pass
