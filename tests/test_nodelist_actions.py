# SPDX-FileCopyrightText: 2026 Stagelab Coop SCCL
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-FileContributor: Ion Reguera <ion@stagelab.coop>

"""The node-adoption WebSocket surface the UI team consumes.

Three behaviours are pinned here:

* `nodelist_modify` refuses a missing/garbage uuid before the engine hop.
  Without that check a None travels editor -> engine -> nodeconf and returns as
  the baffling "Node None not found".
* `nodelist_get` re-reads network_map.xml on demand and answers with the same
  `initial_mappings` payload a client already gets on connect, so refreshing
  needs no reconnect and no new client-side handling.
* `nodeconf_available` tells the UI whether adoption can work at all on this
  host — cuems-nodeconf ships disabled on most of the fleet, and there every
  adopt/un-adopt click can only end in an error.
"""

import asyncio
import concurrent.futures
import json
from unittest.mock import MagicMock, patch

import pytest

from cuemseditor.CuemsWsUser import CuemsWsUser


NODE = '4b9b5a1e-0000-4000-8000-0123456789ab'


@pytest.fixture
def user():
    """A CuemsWsUser bound to a stub server, with no sockets involved."""
    server = MagicMock()
    server.users = {}
    server.event_loop = asyncio.get_event_loop_policy().new_event_loop()
    # A real executor: run_in_executor rejects a MagicMock, and nodelist_get
    # reads the map through it exactly as the production path does.
    server.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    server.mappings_dict = {'nodes': [], 'new_nodes': []}
    server.initial_setting_message = lambda: json.dumps(
        {'type': 'initial_mappings', 'value': server.mappings_dict}
    )
    session = CuemsWsUser(server, MagicMock())
    yield session
    server.executor.shutdown(wait=False)
    server.event_loop.close()


async def _drain(user):
    """Return the messages the session queued for the client."""
    out = []
    while not user.outgoing.empty():
        out.append(json.loads(await user.outgoing.get()))
    return out


# ─── nodelist_modify input validation ────────────────────────────────────


class TestNodelistModifyValidation:
    @pytest.mark.parametrize('bad_uuid', [None, '', 0, [], {}])
    def test_bad_uuid_never_reaches_the_engine(self, user, bad_uuid):
        user.server.event_loop.run_until_complete(
            user.nodelist_modify(bad_uuid, 'nodelist_modify', 'ADD')
        )
        msgs = user.server.event_loop.run_until_complete(_drain(user))

        assert len(msgs) == 1
        assert msgs[0]['type'] == 'error'
        assert msgs[0]['action'] == 'nodelist_modify'
        assert 'node uuid' in msgs[0]['value']

    @pytest.mark.parametrize('bad_action', [None, '', 'ad', 'add', 'DELETE'])
    def test_bad_modify_action_never_reaches_the_engine(self, user, bad_action):
        user.server.event_loop.run_until_complete(
            user.nodelist_modify(NODE, 'nodelist_modify', bad_action)
        )
        msgs = user.server.event_loop.run_until_complete(_drain(user))

        assert msgs[0]['type'] == 'error'
        assert 'Invalid modify_action' in msgs[0]['value']

    def test_a_valid_request_is_forwarded_and_confirmed(self, user):
        with patch.object(
            user, 'comunicate_with_engine', return_value='OK'
        ) as mock_engine:
            user.server.event_loop.run_until_complete(
                user.nodelist_modify(NODE, 'nodelist_modify', 'ADD')
            )
        msgs = user.server.event_loop.run_until_complete(_drain(user))

        mock_engine.assert_called_once()
        command = mock_engine.call_args[0][2]
        assert command['action'] == 'nodelist_modify'
        assert command['value'] == NODE
        assert command['modify_action'] == 'ADD'
        # The frontend matches on exactly this frame; do not change its shape.
        assert msgs[0] == {'type': 'nodelist_modify', 'value': 'OK'}


# ─── nodelist_get ────────────────────────────────────────────────────────


class TestNodelistGet:
    def test_rereads_the_map_and_answers_with_initial_mappings(self, user):
        user.server.reload_network_map_nodes = MagicMock(return_value=True)
        user.server.mappings_dict = {
            'nodes': [{'node': {'uuid': NODE, 'adopted': True}}],
            'new_nodes': [],
        }
        user.server.initial_setting_message = lambda: json.dumps(
            {'type': 'initial_mappings', 'value': user.server.mappings_dict}
        )

        user.server.event_loop.run_until_complete(user.nodelist_get('nodelist_get'))
        msgs = user.server.event_loop.run_until_complete(_drain(user))

        assert len(msgs) == 1
        assert msgs[0]['type'] == 'initial_mappings'
        assert msgs[0]['value']['nodes'][0]['node']['uuid'] == NODE

    def test_unreadable_map_is_an_error_not_a_silent_empty_list(self, user):
        user.server.reload_network_map_nodes = MagicMock(return_value=False)

        user.server.event_loop.run_until_complete(user.nodelist_get('nodelist_get'))
        msgs = user.server.event_loop.run_until_complete(_drain(user))

        assert msgs[0]['type'] == 'error'
        assert msgs[0]['action'] == 'nodelist_get'


# ─── the action map ──────────────────────────────────────────────────────


def test_both_node_actions_are_registered():
    """Guards against a handler that exists but is unreachable from the wire."""
    import inspect

    from cuemseditor import CuemsWsUser as module

    source = inspect.getsource(module.CuemsWsUser.consumer)
    assert '"nodelist_modify":' in source
    assert '"nodelist_get":' in source


# ─── the map watcher ─────────────────────────────────────────────────────


class TestNetworkMapWatcher:
    """cuems-nodeconf is resident and rewrites network_map.xml on its own.

    Before the watcher, the editor read that file only on connect and right
    after its own successful nodelist_modify — so a node powered on while the
    settings panel was already open never showed up in `new_nodes`.
    """

    def _server(self, map_file):
        from cuemseditor.CuemsWsServer import CuemsWsServer

        server = CuemsWsServer.__new__(CuemsWsServer)
        server.NETWORK_MAP_POLL_S = 0.01
        server.notify_all_node_list_update = MagicMock(
            side_effect=lambda: asyncio.sleep(0)
        )
        return server

    def _run_watcher(self, server, cycles=6):
        async def _drive():
            task = asyncio.ensure_future(server.watch_network_map())
            for _ in range(cycles):
                await asyncio.sleep(server.NETWORK_MAP_POLL_S)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_drive())
        finally:
            loop.close()

    def test_broadcasts_when_the_map_is_rewritten(self, tmp_path):
        map_file = tmp_path / 'network_map.xml'
        map_file.write_text('<CuemsNetworkMap/>')
        server = self._server(map_file)

        with patch('cuemseditor.CuemsWsServer.ConfigManager') as MockCM:
            MockCM.return_value.conf_path.return_value = str(map_file)

            async def _drive():
                task = asyncio.ensure_future(server.watch_network_map())
                await asyncio.sleep(0.05)
                # nodeconf writes: os.replace bumps the mtime. Force it
                # forward explicitly — a same-second write would otherwise be
                # invisible on a coarse-timestamp filesystem.
                import os
                import time as _time
                map_file.write_text('<CuemsNetworkMap><node/></CuemsNetworkMap>')
                future = _time.time() + 10
                os.utime(map_file, (future, future))
                await asyncio.sleep(0.05)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(_drive())
            finally:
                loop.close()

        assert server.notify_all_node_list_update.called

    def test_quiet_map_broadcasts_nothing(self, tmp_path):
        map_file = tmp_path / 'network_map.xml'
        map_file.write_text('<CuemsNetworkMap/>')
        server = self._server(map_file)

        with patch('cuemseditor.CuemsWsServer.ConfigManager') as MockCM:
            MockCM.return_value.conf_path.return_value = str(map_file)
            self._run_watcher(server)

        server.notify_all_node_list_update.assert_not_called()

    def test_missing_map_is_not_an_error(self, tmp_path):
        """A host where nodeconf never ran has no map. That is not a failure."""
        server = self._server(tmp_path / 'absent.xml')

        with patch('cuemseditor.CuemsWsServer.ConfigManager') as MockCM:
            MockCM.return_value.conf_path.return_value = str(tmp_path / 'absent.xml')
            self._run_watcher(server)

        server.notify_all_node_list_update.assert_not_called()


# ─── nodeconf_available ──────────────────────────────────────────────────


class TestNodeconfAvailableFlag:
    """Adoption goes through /tmp/nodeconf.ipc. cuems-nodeconf ships disabled
    on most of the fleet; there, every click can only end in an error, and the
    UI has no other way to know that before trying.
    """

    def _reload(self, tmp_path, socket_exists):
        from cuemseditor.CuemsWsServer import CuemsWsServer

        server = CuemsWsServer.__new__(CuemsWsServer)
        server.mappings_dict = {}

        map_file = tmp_path / 'network_map.xml'
        map_file.write_text('<CuemsNetworkMap/>')

        with (
            patch('cuemseditor.CuemsWsServer.ConfigManager') as MockCM,
            patch('cuemseditor.CuemsWsServer.NetworkMap') as MockNM,
            patch.object(
                CuemsWsServer, 'nodeconf_available', return_value=socket_exists
            ),
        ):
            MockCM.return_value.conf_path.return_value = str(map_file)
            MockCM.return_value.network_map = {'node_list': []}
            MockNM.get_nodes_by_adoption.return_value = ([], [])
            ok = server.reload_network_map_nodes()

        return ok, server.mappings_dict

    def test_the_flag_is_sampled_per_message_not_cached(self):
        """The scenario this exists for: the operator has Settings open, then
        someone stops cuems-nodeconf. nodeconf writes nothing when it stops,
        and it skips the write entirely while the map is unchanged, so a flag
        refreshed only on map reloads would keep saying True forever.
        """
        from cuemseditor.CuemsWsServer import CuemsWsServer

        server = CuemsWsServer.__new__(CuemsWsServer)
        server.mappings_dict = {'nodes': [], 'new_nodes': []}

        with patch.object(CuemsWsServer, 'nodeconf_available', return_value=True):
            up = json.loads(server.initial_setting_message())
        # nodeconf stops. Nothing on disk changes.
        with patch.object(CuemsWsServer, 'nodeconf_available', return_value=False):
            down = json.loads(server.initial_setting_message())

        assert up['value']['nodeconf_available'] is True
        assert down['value']['nodeconf_available'] is False

    def test_watcher_broadcasts_when_nodeconf_goes_away(self, tmp_path):
        """A stopped nodeconf touches no file, so only an explicit check finds
        it — otherwise the panel keeps offering buttons that can only fail.
        """
        from cuemseditor.CuemsWsServer import CuemsWsServer

        map_file = tmp_path / 'network_map.xml'
        map_file.write_text('<CuemsNetworkMap/>')

        server = CuemsWsServer.__new__(CuemsWsServer)
        server.NETWORK_MAP_POLL_S = 0.01
        server.notify_all_node_list_update = MagicMock(
            side_effect=lambda: asyncio.sleep(0)
        )
        states = [True, False, False, False, False, False, False, False]
        server.nodeconf_available = MagicMock(side_effect=lambda: states.pop(0) if states else False)

        with patch('cuemseditor.CuemsWsServer.ConfigManager') as MockCM:
            MockCM.return_value.conf_path.return_value = str(map_file)

            async def _drive():
                task = asyncio.ensure_future(server.watch_network_map())
                await asyncio.sleep(0.1)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(_drive())
            finally:
                loop.close()

        assert server.notify_all_node_list_update.called

    def test_true_when_the_socket_is_there(self, tmp_path):
        ok, mappings = self._reload(tmp_path, socket_exists=True)
        assert ok is True
        assert mappings['nodeconf_available'] is True

    def test_false_when_nodeconf_is_disabled(self, tmp_path):
        ok, mappings = self._reload(tmp_path, socket_exists=False)
        assert ok is True
        assert mappings['nodeconf_available'] is False


# ─── node_status (liveness relay) ────────────────────────────────────────


class TestNodeStatus:
    """The engine's runtime view, relayed untouched.

    Kept distinct from each node's `online` field on purpose: `online` is
    nodeconf's discovery view, `alive` is the engine's sub-second ping/pong.
    """

    ENGINE_VALUE = {
        'alive': ['ctrl-uuid'],
        'adopted': ['ctrl-uuid', 'node01-uuid'],
        'controller': 'ctrl-uuid',
        'age_s': 0.12,
    }

    def test_relays_the_engine_answer_untouched(self, user):
        with patch.object(
            user, 'comunicate_with_engine', return_value=self.ENGINE_VALUE
        ) as mock_engine:
            user.server.event_loop.run_until_complete(user.node_status('node_status'))
        msgs = user.server.event_loop.run_until_complete(_drain(user))

        # query_mode is required: the engine answers a dict, not "OK".
        assert mock_engine.call_args[1]['query_mode'] is True
        # ...and it must ask the engine for cluster_status, whatever the client
        # called the action.
        assert mock_engine.call_args[0][2]['action'] == 'cluster_status'
        assert msgs[0] == {'type': 'node_status', 'value': self.ENGINE_VALUE}

    def test_a_dead_engine_is_an_error_not_a_silent_empty_answer(self, user):
        from cuemseditor.CuemsErrors import EngineError

        with patch.object(
            user, 'comunicate_with_engine',
            side_effect=EngineError('Timeout: Engine did not respond in 25s'),
        ):
            user.server.event_loop.run_until_complete(user.node_status('node_status'))
        msgs = user.server.event_loop.run_until_complete(_drain(user))

        assert msgs[0]['type'] == 'error'
        assert msgs[0]['action'] == 'node_status'

    def test_the_action_is_registered(self):
        import inspect

        from cuemseditor import CuemsWsUser as module

        assert '"node_status":' in inspect.getsource(module.CuemsWsUser.consumer)
