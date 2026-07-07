# cuems-editor

Part of the **CUEMS** ecosystem — see the [`cuems-RELATIONS`](https://github.com/stagesoft/cuems-RELATIONS) repo for the system index, architecture diagram, and protocol/port map.

## Role

WebSocket middleware for multi-user project editing and media management — sits between the browser frontend (`cuems-frontend`) and the engine (`cuems-engine`). Python 3.11+, systemd service `cuems-editor.service` (controller-side). PyPI name `cuemseditor`.

- **Frontend → Editor**: WebSocket `:9092`, JSON `{"action": "...", "value": ...}`.
- **Editor → Engine**: Unix IPC socket `/tmp/editor.ipc` (serialized objects) over NNG.

Main classes: `CuemsWsServer` (asyncio WS server / session multiplexer / command router), `CuemsWsUser` (per-connection session handler), `CuemsProjectManager` (owns the DB managers), `CuemsDBProject` (project CRUD, XML script I/O, filesystem management).

## Build

```bash
cd <this repo> && debuild -b -uc -us -nc
```

## Project store / library — `/opt/cuems_library/`

The authoritative path is `settings.xml <library_path>` (read by the editor as `library_path`). **There is NO `/opt/cuems/` directory** — that was a long-standing doc error; everything project-related is under `/opt/cuems_library/`.

Contents:
- `projects/` — one dir per project, **named by `unix_name`**. The project UUID and display `name` live *inside* `script.xml` and in the index DB, **not** in the dir name.
- `media/`, `trash/`.
- `project-manager.db` — SQLite index (tables `project`, `media`, `projectmedia`).

## Field notes / gotchas

- **After hand-editing `network_map.xml` or `default_mappings.xml`, restart BOTH `cuems-editor` AND `cuems-controller-engine`** (plus `cuems-node-engine` if active). Both daemons cache topology at startup via `ConfigManager(load_all=True)`; there is no on-disk reload. Restarting only the editor leaves the engine on the stale topology. Verify: editor logs `"number_of_nodes": N`.
- During the nodeconf transition, operators hand-populate `<role_id>`/`<alias>`/`<hostname>` in `network_map.xml`; restart the editor so the merged dict picks up the new fields. `cuems-frontend` falls back to a legacy `Node ${index + 1}` label when both `alias` and `role_id` are absent — partial migrations are safe.
