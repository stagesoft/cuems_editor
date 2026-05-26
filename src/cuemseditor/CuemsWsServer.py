import asyncio
import concurrent.futures
import json
import os
import time
import websockets as ws
from websockets.asyncio.server import serve
from multiprocessing import Process
import signal
from random import randint  #TODO: clean unused
from hashlib import md5
import re

from cuemsutils.log import logged, Logger

from cuemseditor.CuemsProjectManager import CuemsDBManager
from cuemseditor.CuemsWsUser import CuemsWsUser
from cuemseditor.CuemsUpload import CuemsUpload
from cuemseditor.CuemsErrors import *

from cuemsutils.tools.CommunicatorServices import Communicator
from cuemsutils.tools.ConfigManager import ConfigManager
from cuemsutils.xml import NetworkMap
from cuemsutils.create_script import create_script, new_uuid


class CuemsWsServer():
    """Top-level asyncio WebSocket server for the CueMS editor backend.

    Accepts connections on two paths:

    * ``/`` or ``/?session=<uuid>`` — main project-manager session handled by
      :class:`CuemsWsUser`.
    * ``/upload`` — binary file upload session handled by
      :class:`CuemsUpload`.

    Owns the NNG ``Communicator`` (engine IPC), the ``CuemsDBManager``, the
    active ``users`` dict, the ``sessions`` state, and a thread-pool
    ``executor`` for blocking DB and filesystem operations.

    Example:
        >>> server = CuemsWsServer(settings_dict, mappings_dict)
        >>> server.start(port=9092)

    The ``settings_dict`` must contain at minimum:

    .. code-block:: python

        {
            "editor_ipc": "ipc:///tmp/cuems-editor.sock",
            "tmp_path": "/tmp/cuems",
            "session_uuid": "<uuid>",
            "library_path": "/var/lib/cuems/library",
            # ... plus all keys required by CuemsDBManager
        }
    """

    def __init__(self, settings_dict, mappings_dict):
        """Initialise the server: bind the NNG communicator and validate the tmp path.

        Does **not** start listening; call :meth:`start` to enter the event
        loop.

        Args:
            settings_dict: Settings dict from ``settings.xml``; required keys
                include ``editor_ipc``, ``tmp_path``, ``session_uuid``, and
                ``library_path``.
            mappings_dict: Output mapping dict from ``project_mappings.xml``;
                passed verbatim to the frontend as the initial mappings
                message and updated when nodes change adoption status.

        Raises:
            KeyError: If a required key is absent from *settings_dict*.
            FileNotFoundError: If ``tmp_path`` does not exist or is not
                read/write/executable.
        """
        self.engine_communicator = Communicator(address=settings_dict['editor_ipc'])
        #self.engine_queue = Comunicator(address="ipc:///tmp/test2.sock")
        self.engine_messages = list()
        self.users = dict()
        self.sessions = dict()
        self.settings_dict = settings_dict
        self.mappings_dict = mappings_dict
        self.initital_template = create_script()
        try:
            self.tmp_path = self.settings_dict['tmp_path']
            self.session_uuid = self.settings_dict['session_uuid']
            self.library_path = self.settings_dict['library_path']
        except KeyError as e:
            Logger.error(f'can not read settings {e}')
            raise e
        Logger.debug(f'library path set to : {self.library_path}')

        if (not os.path.exists(self.tmp_path)) or (not os.access(self.tmp_path, os.X_OK & os.R_OK & os.W_OK)):
            Logger.error("error: upload folder is not usable")
            raise FileNotFoundError('Can not access upload folder')

        self.reload_network_map_nodes()

    def start(self, port=None):
        """Enter the asyncio event loop and start listening for connections.

        Blocks until :meth:`stop` is called (via SIGINT or SIGTERM).

        Args:
            port: TCP port to listen on.  May also be pre-set via
                ``self.port``; raises ``ValueError`` if neither is provided.

        Raises:
            ValueError: If *port* is ``None`` and ``self.port`` has not been
                set.
        """
        # Use provided port, or fall back to self.port if set
        if port is None:
            if not hasattr(self, 'port') or self.port is None:
                raise ValueError("port argument is required or must be set on server instance")
            port = self.port
        self.port = port
        self.host = 'localhost'
        newfeature = asyncio.get_event_loop().run_until_complete(self.run_async_server())

    async def run_async_server(self):
        """Initialise the DB, executor, and WebSocket server, then serve forever.

        Called by :meth:`start`.  Sets up:

        * ``self.db`` — ``CuemsDBManager`` instance.
        * ``self.executor`` — ``ThreadPoolExecutor`` for blocking work.
        * ``self.project_server`` — the ``websockets`` server object.
        * SIGINT / SIGTERM handlers that call :meth:`stop`.
        """
        self.db = CuemsDBManager(self.settings_dict)
        self.event_loop = asyncio.get_event_loop()

        self.executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix='ws_ProjectManager_ThreadPoolExecutor', max_workers=5)  # TODO: adjust max workers
        #self.event_loop.set_exception_handler(self.exception_handler) ### TODO:UNCOMENT FOR PRODUCTION
        self.project_server = await serve(self.connection_handler, self.host, self.port)
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.event_loop.add_signal_handler(sig, self.stop)
        Logger.info('server listening on {}, port {}'.format(self.host, self.port))
        await self.project_server.serve_forever()

    def stop(self):
        """Initiate a graceful shutdown from a signal handler.

        Schedules ``project_server.close()`` and :meth:`stop_async` on the
        event loop from any thread context.
        """
        #self.event_loop.call_soon_threadsafe(self.queue_task.cancel)
        self.event_loop.call_soon_threadsafe(self.project_server.close)
        Logger.info('ws server closing')
        asyncio.run_coroutine_threadsafe(self.stop_async(), self.event_loop)

    async def stop_async(self):
        """Wait for the WebSocket server to finish closing, then stop the event loop."""
        await self.project_server.wait_closed()
        Logger.info('ws server closed')
        self.event_loop.call_soon(self.event_loop.stop)
        Logger.info('event loop stoped')

    async def connection_handler(self, websocket):
        """Route an incoming WebSocket connection to the correct session type.

        ``/`` and ``/?session=<uuid>`` go to :meth:`project_manager_session`;
        ``/upload`` goes to :meth:`upload_session`; other paths are logged and
        dropped.

        Args:
            websocket: Incoming ``websockets`` connection object.
        """
        Logger.info("new connection: {}, path: {}".format(websocket.remote_address, websocket.request.path))
        path = websocket.request.path
        if (path == '/' or path[0:9] == '/?session'):                   # project manager
            await self.project_manager_session(websocket, path)
        elif path == '/upload':                                           # file upload
            await self.upload_session(websocket)
        else:
            Logger.info("unknown path: {}".format(path))

    async def project_manager_session(self, websocket, path):
        """Manage the full lifecycle of a project-manager WebSocket session.

        Creates a :class:`CuemsWsUser`, registers it, sends the initial
        template and mappings messages, then runs the consumer/producer/
        processor task set.  Cleans up via :meth:`unregister` on exit.

        Args:
            websocket: The accepted WebSocket connection.
            path: The request path (used to extract an optional session UUID).
        """
        user_session = CuemsWsUser(self, websocket)
        await self.register(user_session, path)
        await user_session.outgoing.put(self.initial_json_template())
        await user_session.outgoing.put(self.initial_setting_message())
        try:
            consumer_task = asyncio.create_task(user_session.consumer_handler())
            producer_task = asyncio.create_task(user_session.producer_handler())
            # start 3 message processing task so a load or any other time consuming action still leaves with 2 tasks running  and interface feels responsive. TODO:discuss this
            processor_tasks = [asyncio.create_task(user_session.consumer()) for _ in range(3)]

            done_tasks, pending_tasks = await asyncio.wait([consumer_task, producer_task, *processor_tasks], return_when=asyncio.FIRST_COMPLETED)
            for task in pending_tasks:
                task.cancel()

        except Exception as e:
            Logger.debug(f"{e}, {type(e)}")

        finally:
            await self.unregister(user_session)

    async def upload_session(self, websocket):
        """Handle a binary file upload over the ``/upload`` WebSocket path.

        Creates a :class:`CuemsUpload` and runs its :meth:`~CuemsUpload.message_handler`
        until the connection closes.

        Args:
            websocket: The accepted WebSocket connection on the ``/upload`` path.
        """
        user_upload_session = CuemsUpload(self, websocket)
        Logger.info("new upload session: {}".format(user_upload_session))

        await user_upload_session.message_handler()
        Logger.info("upload session ended: {}".format(user_upload_session))

    async def register(self, user_session, path):
        """Register a new user session and restore any prior session state.

        Adds *user_session* to ``self.users``, broadcasts the updated user
        count, assigns a ``session_id`` via :meth:`check_session`, and calls
        :meth:`load_session`.

        Args:
            user_session: The :class:`CuemsWsUser` to register.
            path: Request path string (may carry a ``?session=<uuid>`` param).
        """
        Logger.info("user registered: {}".format(id(user_session.websocket)))
        self.users[user_session] = None
        await self.notify_users("users")
        user_session.session_id = await self.check_session(user_session, path)
        await self.load_session(user_session)

    async def check_session(self, user_session, path):
        """Parse or generate a session UUID from the request path.

        If the path carries a ``?session=<uuid>`` parameter and the UUID is
        already in ``self.sessions``, it is reused.  Otherwise a new UUID is
        generated.  The session entry is updated with the current WebSocket id
        and the UUID is sent to the client via :meth:`notify_session`.

        Args:
            user_session: The :class:`CuemsWsUser` being registered.
            path: Request path string.

        Returns:
            Session UUID string.
        """
        session_uuid_patern = r"/\?session=(?P<uuid>[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12})?"

        matches = re.search(session_uuid_patern, path)
        if matches:
            if (matches.groupdict()['uuid'] != None):
                uuid = matches.groupdict()['uuid']
                if uuid not in self.sessions:
                    Logger.debug(f"uuid not found {uuid}, creating new session")
                    uuid = str(new_uuid())
                else:
                    Logger.debug(f"session_id found, reusing {uuid}")
            else:
                uuid = str(new_uuid())
        else:
            uuid = str(new_uuid())
        try:
            self.sessions[uuid]['ws'] = id(user_session.websocket)
        except KeyError:
            self.sessions[uuid] = {'ws': id(user_session.websocket)}

        await self.notify_session(user_session, uuid)

        return uuid

    async def load_session(self, user_session):
        """Restore session state for a reconnecting user.

        Currently a no-op; the commented-out body would re-send the previously
        loaded project.  Reserved for future reconnection support.

        Args:
            user_session: The :class:`CuemsWsUser` whose prior state to
                restore.
        """
        pass
        # try:
        #     await user_session.send_project(self.sessions[user_session.session_id]['loaded_project'], 'project_load')
        # except KeyError:
        #     pass

    async def notify_session(self, user_session, uuid):
        """Send the assigned session UUID to the client.

        Args:
            user_session: The :class:`CuemsWsUser` to notify.
            uuid: Session UUID string to send.
        """
        message = json.dumps({"type": "session_id", "value": uuid})
        await user_session.outgoing.put(message)

    async def unregister(self, user_task):
        """Remove a user session and broadcast the updated user count.

        Args:
            user_task: The :class:`CuemsWsUser` to remove.
        """
        Logger.info("user unregistered: {}".format(id(user_task.websocket)))
        self.users.pop(user_task, None)
        await self.notify_users("users")

    async def notify_others_list_changes(self, calling_user, list_type):
        """Notify all other connected users that a list has changed.

        Used after project/media create, delete, or restore operations so all
        open browser tabs refresh their lists.

        Args:
            calling_user: The user who triggered the change; excluded from
                the notification.
            list_type: String list name to include in the message value (e.g.
                ``"project_list"``, ``"file_list"``).
        """
        if self.users:  # notify others, not the user trigering the action, and only if the have same project loaded
            message = json.dumps({"type": "list_update", "value": list_type})
            for user, project in self.users.items():
                if user is not calling_user:
                    await user.outgoing.put(message)
                    Logger.debug('notifing {} {}'.format(user, list_type))

    async def notify_others_same_project(self, calling_user, msg_type, project_uuid=None):
        """Notify users who have the same project loaded that it was modified.

        If *project_uuid* is provided, only users with that exact project
        loaded are notified.  If ``None``, uses the calling user's loaded
        project.

        Args:
            calling_user: The user who triggered the change; excluded from
                the notification.
            msg_type: Message type string (not currently used in the payload).
            project_uuid: UUID string of the modified project, or ``None`` to
                infer from *calling_user*'s session.
        """
        if self.users:  # notify others, not the user trigering the action, and only if the have same project loaded
            message = json.dumps({"type": "project_update", "value": project_uuid})
            for user, project in self.users.items():
                if user is not calling_user:
                    if project_uuid is not None:
                        if str(project) != str(project_uuid):
                            continue
                    else:
                        if str(project) != str(self.users[calling_user]):
                            continue

                    Logger.debug('same project loaded')
                    await user.outgoing.put(message)
                    Logger.debug('notifing {}'.format(user))

    async def notify_users(self, type):
        """Broadcast a message to all connected users.

        Args:
            type: Message type string; only ``"users"`` is currently used
                (sends the connected-user count).
        """
        if self.users:  # asyncio.wait doesn't accept an empty dict
            message = self.users_event(type)
            for user in self.users:
                await user.outgoing.put(message)

    # warning, these non async functions should be not blocking or user @sync_to_async to get their own thread

    def merge_node_data(self, existing_nodes, new_nodes):
        """Merge existing node data (with outputs) with new node data (with updated status).

        Matches nodes by UUID and preserves outputs configuration while updating
        basic fields (``online``, ``adopted``, ``ip``, ``name``, ``node_type``,
        ``mac``).

        Args:
            existing_nodes: List of existing node dicts with outputs configuration.
            new_nodes: List of new node dicts from ``network_map.xml`` with
                updated status.

        Returns:
            List of merged node dicts; nodes absent from *existing_nodes* are
            included as-is from *new_nodes*.
        """
        # Create a lookup dict for existing nodes by UUID
        existing_by_uuid = {}
        for node_item in existing_nodes:
            if 'node' in node_item:
                uuid = node_item['node'].get('uuid')
                if uuid:
                    existing_by_uuid[uuid] = node_item

        # Merge new nodes with existing data
        merged_nodes = []
        for new_node_item in new_nodes:
            if 'node' not in new_node_item:
                continue

            new_node = new_node_item['node']
            uuid = new_node.get('uuid')

            if uuid and uuid in existing_by_uuid:
                # Node exists - merge data
                existing_node = existing_by_uuid[uuid]['node'].copy()

                # Update basic fields from network_map (online, adopted, ip, name, etc.)
                basic_fields = ['online', 'adopted', 'ip', 'name', 'node_type', 'mac']
                for field in basic_fields:
                    if field in new_node:
                        existing_node[field] = new_node[field]

                # Keep outputs (audio, video, dmx) from existing node
                merged_nodes.append({'node': existing_node})
            else:
                # New node not in existing data - add as-is
                merged_nodes.append(new_node_item)

        return merged_nodes

    def reload_network_map_nodes(self):
        """Reload ``network_map.xml`` and update ``mappings_dict`` with current node status.

        Retries up to 3 times with exponential back-off in case the file is
        being written concurrently.  Merges the reloaded node data with the
        existing ``mappings_dict`` via :meth:`merge_node_data` to preserve
        output configurations while refreshing online/adopted status.

        Returns:
            ``True`` on success, ``False`` if the file does not exist or all
            retries fail.
        """
        max_retries = 3
        initial_delay = 0.1
        delay_after_write = 0.05

        for attempt in range(max_retries):
            try:
                cf_manager = ConfigManager(load_all=False)
                network_map_file = cf_manager.conf_path('network_map.xml')

                if not os.path.isfile(network_map_file):
                    if attempt == 0:
                        Logger.warning(f'network_map.xml not found at {network_map_file}')
                    return False

                time.sleep(delay_after_write)

                cf_manager.load_network_map()
                # network_map is now a dict with 'node_list' key
                network_map_dict = cf_manager.network_map
                nodes, new_nodes = NetworkMap.get_nodes_by_adoption(network_map_dict)

                # Merge with existing data to preserve outputs configuration
                # Combine both lists to handle nodes that change adoption status
                existing_nodes = self.mappings_dict.get('nodes') or []
                existing_new_nodes = self.mappings_dict.get('new_nodes') or []
                all_existing = existing_nodes + existing_new_nodes

                merged_nodes = self.merge_node_data(all_existing, nodes)
                merged_new_nodes = self.merge_node_data(all_existing, new_nodes)

                self.mappings_dict['nodes'] = merged_nodes
                self.mappings_dict['new_nodes'] = merged_new_nodes
                Logger.debug(f'Network map reloaded successfully: {len(merged_nodes)} adopted nodes, {len(merged_new_nodes)} new nodes')
                return True

            except Exception as e:
                if attempt < max_retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    Logger.warning(f'Error loading network_map (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {delay}s...')
                    time.sleep(delay)
                else:
                    Logger.error(f'Error loading network_map after {max_retries} attempts: {e}')
                    return False

        return False

    def initial_json_template(self):
        """Build the initial ``CuemsScript`` template message sent to new clients.

        Returns:
            JSON string ``{"type": "initial_template", "value": {"CuemsScript": ...}}``.
        """
        return json.dumps({"type": "initial_template", "value": {"CuemsScript": self.initital_template}})

    def initial_setting_message(self):
        """Build the initial mappings message sent to new clients.

        Returns:
            JSON string ``{"type": "initial_mappings", "value": <mappings_dict>}``.
        """
        return json.dumps({"type": "initial_mappings", "value": self.mappings_dict})

    async def notify_all_node_list_update(self):
        """Reload the network map and broadcast the updated node list to all clients.

        Runs :meth:`reload_network_map_nodes` in the thread-pool executor to
        avoid blocking the event loop during file I/O.  Broadcasts
        :meth:`initial_setting_message` to every connected user on success.
        """
        reload_success = await self.event_loop.run_in_executor(
            self.executor,
            self.reload_network_map_nodes
        )
        if reload_success:
            if self.users:
                message = self.initial_setting_message()
                for user in self.users:
                    await user.outgoing.put(message)
                Logger.debug(f'Broadcasted updated node list to {len(self.users)} connected client(s)')
            else:
                Logger.debug('Node list updated but no clients connected')
        else:
            Logger.warning('Failed to reload network map, not broadcasting update')

    def users_event(self, type, uuid=None):
        """Build a users-count or item-modified event message.

        Args:
            type: ``"users"`` to send the connected-user count; any other
                value builds an item-modified message (currently unused).
            uuid: Item UUID for non-``"users"`` message types.

        Returns:
            JSON string.
        """
        if type == "users":
            return json.dumps({"type": type, "value": len(self.users)})
        else:
            return json.dumps({"type": type, "uuid": uuid, "value": "modified in server"})  # TODO: not used

    def exception_handler(self, loop, context):
        """Log uncaught asyncio exceptions at DEBUG level.

        Registered as the event loop's exception handler in production mode
        (currently commented out to keep stack traces visible during
        development).

        Args:
            loop: The asyncio event loop.
            context: Exception context dict provided by asyncio.
        """
        Logger.debug("Caught the following exception: (ignore if on closing)")
        Logger.debug(context['message'])
