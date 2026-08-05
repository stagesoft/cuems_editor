<!--
SPDX-FileCopyrightText: 2026 Stagelab Coop SCCL
SPDX-License-Identifier: GPL-3.0-or-later
-->

# cuems-editor

**WebSocket backend and project management server for the CueMS browser-based editor.**

[![PyPI - Version](https://img.shields.io/pypi/v/cuemseditor.svg)](https://pypi.org/project/cuemseditor)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/cuemseditor.svg)](https://pypi.org/project/cuemseditor)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Deploy MkDocs site](https://github.com/stagesoft/cuems-editor/actions/workflows/gh-pages.yml/badge.svg)](https://github.com/stagesoft/cuems-editor/actions/workflows/gh-pages.yml)
[![Upload Python Package](https://github.com/stagesoft/cuems-editor/actions/workflows/pypi-publish.yml/badge.svg)](https://github.com/stagesoft/cuems-editor/actions/workflows/pypi-publish.yml)

!!! note "Project README"
    For installation instructions, release history, and licensing, see the
    [project README](https://github.com/stagesoft/cuems-editor#readme) on GitHub.

---

## What is cuems-editor?

`cuems-editor` is the backend service that powers the **CueMS** browser-based authoring interface. The frontend is a standard HTML/JS application that communicates exclusively over WebSocket; `cuems-editor` is the process that receives those connections, manages the project library, and bridges authoring commands to the live `cuems-engine`.

The service runs as a systemd unit (`cuems-editor.service`) inside the `/usr/lib/cuems/` virtual environment provided by the `cuems-utils` Debian package. It never forks — process supervision is delegated to systemd `Type=simple`.

| Component | Role |
|---|---|
| `CuemsWsServer` | asyncio WebSocket server; session multiplexer and command router |
| `CuemsWsUser` | Per-connection session; owns async producer/consumer queues |
| `CuemsDBManager` | Database facade; initializes SQLite schema, owns `.project` and `.media` |
| `CuemsDBProject` | Project CRUD, XML script I/O, and filesystem directory management |
| `CuemsDBMedia` | Media CRUD, thumbnail / waveform / video-index generation |
| `CuemsDBModel` | Peewee ORM models: `Project`, `Media`, `ProjectMedia` |
| `CuemsUpload` | Async binary upload handler over a dedicated WebSocket connection |
| `CuemsLibraryMaintenance` | Library directory bootstrap on first run |
| `CuemsErrors` | Shared exception hierarchy |

---

## Signal flow

```text
Browser ──WebSocket──► CuemsWsServer
                              │
                    ┌─────────┴──────────────────┐
                    ▼                             ▼
             CuemsWsUser                   CuemsUpload
          (JSON commands)              (binary upload frames)
                    │                             │
         ┌──────────┴──────────┐                 │
         ▼                     ▼                 ▼
   NNG IPC →             CuemsDBManager     CuemsDBMedia.new()
   cuems-engine          ├── CuemsDBProject      │
                         │   (project CRUD        │
                         │    + XML I/O)          │
                         └── CuemsDBMedia    ─────┘
                              (media CRUD)
                              ├── ffmpeg (thumbnails)
                              ├── audiowaveform (waveforms)
                              └── cuems-videoindexer (index sidecars)
```

The primary authoring flow for a `project_save` command:

1. Browser sends `{"command": "project_save", "data": {...}}` over the main WebSocket.
2. `CuemsWsServer` receives the frame and calls the matching handler on `CuemsWsUser`.
3. `CuemsWsUser` deserializes the JSON payload into a `CuemsScript` via `cuemsutils.XmlReaderWriter`.
4. An orphan-cleanup pass nullifies any `action_target` referencing a deleted cue UUID.
5. `CuemsDBProject.update()` validates the script against `script.xsd` and writes `cue_script.xml` to disk.
6. `CuemsWsUser` sends `{"result": "ok"}` back over WebSocket.

---

## Architecture

### WebSocket layer

[`CuemsWsServer`](api.md) — the top-level asyncio server. Owns the NNG `Communicator` (for engine IPC), the `CuemsDBManager`, and the active-user dict. Dispatches incoming JSON commands to per-user handlers.

- **`CuemsWsServer`** — constructs from `settings_dict` and `mappings_dict`; `.start(port)` enters the event loop.

[`CuemsWsUser`](api.md) — per-connection session object. Owns an `incoming` and `outgoing` asyncio queue; the consumer handler feeds the incoming queue from the socket; the producer handler drains the outgoing queue to the socket.

- **`CuemsWsUser`** — lifetime matches the WebSocket connection; holds `session_id` once authenticated.

### Database layer

[`CuemsDBManager`](api.md) — facade that initializes the SQLite database, creates tables on first run, and exposes `.project` (`CuemsDBProject`) and `.media` (`CuemsDBMedia`) accessors.

[`CuemsDBProject`](api.md) — full project lifecycle. Projects live at `library_path/projects/<unix_name>/cue_script.xml`. Uses `StringSanitizer` from `cuemsutils` for all user-supplied name strings.

- **`CuemsDBProject`** — create, read, update, delete, duplicate, trash, restore.

[`CuemsDBMedia`](api.md) — media file intake and metadata management. On upload, generates a JPEG thumbnail (`ffmpeg`), an audiowaveform JSON waveform (audio only), and a `.idx` video index sidecar (video only).

- **`CuemsDBMedia`** — inherits `StringSanitizer`; `CTimecode.milliseconds_rounded` used for `audiowaveform -e` duration argument.
- **`MediaType`** — `MOVIE`, `AUDIO`, `IMAGE`.

[`CuemsDBModel`](api.md) — Peewee ORM schema.

- **`Project`** — `uuid`, `name`, `unix_name`, `description`, `created`, `modified`, `in_trash`.
- **`Media`** — `uuid`, `name`, `unix_name`, `description`, `created`, `modified`, `duration`, `media_type`, `in_trash`.
- **`ProjectMedia`** — many-to-many join.

### Upload layer

[`CuemsUpload`](api.md) — two-phase upload protocol: JSON init frame (filename, size, MD5) followed by binary data frames assembled in a tmp directory; file is moved to the media library only after MD5 verification.

### Support

[`CuemsLibraryMaintenance`](api.md) — creates `media/`, `media/thumbnails/`, `media/waveforms/`, `projects/`, and all `trash/` subtrees on first run.

[`CuemsErrors`](api.md) — `CuemsWsServerError`, `FileIntegrityError`, `NonExistentItemError`, `NotTimeCodeError`, `EngineError`.

---

## Key design decisions

### Foreground-only execution (no daemon layer)

`python-daemon`'s `DaemonContext` double-forks and closes file descriptors at startup. This is incompatible with NNG sockets (which bind before the fork) and with asyncio's event loop (which does not survive a fork). The daemon layer was removed entirely; systemd `Type=simple` manages the process lifecycle instead. The concrete invariant: `cuems-editor` must always be started with a TTY or from systemd, never with `--daemon`.

### Schema-validated XML on every write

Project XML files are validated against `cuemsutils`'s `script.xsd` (via `XmlReaderWriter`) on every save, not just on load. This means a frontend bug that produces a structurally invalid cue tree is rejected at the editor boundary before it can corrupt a file that `cuems-engine` would later fail to load silently. The alternative — validate only on read — would allow invalid files to accumulate on disk undetected.

### Orphan-target cleanup before save

When a cue is deleted in the frontend, `ActionCue.action_target` fields in sibling cues may still reference the deleted cue's UUID. Rather than enforcing referential integrity at the schema level (which would require cascading deletes in the XSD), the editor runs a pre-save pass that collects all live cue UUIDs and nullifies any `action_target` that points outside the set. The invariant: no saved `cue_script.xml` contains a dangling `action_target`.

### Versioned filesystem moves

Renaming a project directory on name collision is delegated to `CopyMoveVersioned` from `cuemsutils`. The editor's DB `unix_name` is updated to match the actual directory name after the move — a prior bug left the DB pointing at a non-existent path after a versioned rename. The invariant: `Project.unix_name` in the DB always equals the actual directory name on disk.

### Single NNG request/response socket for engine IPC

All engine commands (`load`, `unload`, `play`, `stop`, `status`) go through a single NNG request/response socket rather than a per-command socket or a pub/sub topology. This keeps the engine IPC surface minimal and ensures the editor cannot issue a second command while a previous response is still pending. The address is read from `settings_dict['editor_ipc']` at startup.

---

## API reference

- [API](api.md) — `CuemsWsServer`, `CuemsWsUser`, `CuemsDBManager`, `CuemsDBProject`, `CuemsDBMedia`, `CuemsDBModel`, `CuemsUpload`, `CuemsLibraryMaintenance`, `CuemsErrors`
