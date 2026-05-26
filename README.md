<!--
***
SPDX-FileCopyrightText: 2026 Stagelab Coop SCCL
SPDX-License-Identifier: GPL-3.0-or-later
***
-->

# cuems-editor

**Current release: v0.1.0rc1** — see [CHANGELOG.md](./CHANGELOG.md).

**WebSocket backend and project management server for the CueMS browser-based editor.**

[![PyPI - Version](https://img.shields.io/pypi/v/cuemseditor.svg)](https://pypi.org/project/cuemseditor)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/cuemseditor.svg)](https://pypi.org/project/cuemseditor)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Deploy MkDocs site](https://github.com/stagesoft/cuems-editor/actions/workflows/gh-pages.yml/badge.svg)](https://github.com/stagesoft/cuems-editor/actions/workflows/gh-pages.yml)
[![Upload Python Package](https://github.com/stagesoft/cuems-editor/actions/workflows/pypi-publish.yml/badge.svg)](https://github.com/stagesoft/cuems-editor/actions/workflows/pypi-publish.yml)

* **Source / issues:** [stagesoft/cuems-editor](https://github.com/stagesoft/cuems-editor) on GitHub
* **API reference (HTML):** [stagesoft.github.io/cuems-editor](https://stagesoft.github.io/cuems-editor/)

`cuems-editor` is the backend service that powers the **CueMS** browser-based authoring interface. It runs as a systemd service, exposes a WebSocket API consumed by the HTML/JS frontend, manages a SQLite project and media library, and relays engine commands to `cuems-engine` over NNG IPC.

It is composed of:

* **`cuemseditor.CuemsWsServer`** — asyncio WebSocket server; session multiplexer and command router
* **`cuemseditor.CuemsWsUser`** — per-connection session handler with async producer/consumer queues
* **`cuemseditor.CuemsProjectManager`** — facade that initializes and owns the database managers
* **`cuemseditor.CuemsDBProject`** — project CRUD, XML script I/O, and filesystem management
* **`cuemseditor.CuemsDBMedia`** — media CRUD, thumbnail and waveform generation, video pre-indexing
* **`cuemseditor.CuemsDBModel`** — Peewee ORM models: `Project`, `Media`, `ProjectMedia`
* **`cuemseditor.CuemsUpload`** — async binary upload handler for the chunked WebSocket upload protocol
* **`cuemseditor.CuemsLibraryMaintenance`** — library directory bootstrap on first run
* **`cuemseditor.CuemsErrors`** — shared exception hierarchy

---

## Overview

`cuems-editor` bridges a browser frontend to the CueMS runtime:

```text
Browser (WebSocket)
       │
       ▼
CuemsWsServer ──► CuemsWsUser (per connection)
       │                │
       │         command handlers
       │                │
       │    ┌───────────┴────────────┐
       │    ▼                        ▼
       │  CuemsDBManager        NNG IPC
       │    ├── CuemsDBProject   cuems-engine
       │    └── CuemsDBMedia
       │           └── ffmpeg / audiowaveform / cuems-videoindexer
       │
       └── CuemsUpload (binary upload endpoint)
```

* **WebSocket layer** (`CuemsWsServer`, `CuemsWsUser`) handles JSON command/response framing and binary upload frames over separate connections.
* **Database layer** (`CuemsDBProject`, `CuemsDBMedia`, `CuemsDBModel`) stores project metadata and media records in SQLite using Peewee ORM.
* **File layer** manages project XML scripts (via `cuemsutils.XmlReaderWriter`), media files, thumbnails, waveforms, and video index sidecars on disk.
* **Engine bridge** relays `load`, `unload`, `play`, `stop`, and status commands to `cuems-engine` via NNG request/response.

---

## Architecture

### `CuemsWsServer`

Top-level asyncio server. Owns the NNG `Communicator`, the `CuemsDBManager`, and the active user dict. Dispatches incoming JSON commands to per-user handlers and forwards engine IPC calls. Reloads the `NetworkMap` from `cuemsutils.ConfigManager` at startup.

* **`CuemsWsServer`** — constructs the server from a `settings_dict` and `mappings_dict`; call `.start(port)` to enter the asyncio event loop.

---

### `CuemsWsUser`

Per-connection session object. Owns an asyncio `incoming` queue (fed by the consumer handler) and an `outgoing` queue (drained by the producer handler). Dispatches command strings to the appropriate project/engine method and serializes responses back to JSON.

* **`CuemsWsUser`** — bound to a single WebSocket connection; lifetime matches the connection.

---

### `CuemsProjectManager` (`CuemsDBManager`)

Facade over the two database sub-managers. Initializes the SQLite database, creates tables on first run, and exposes `.project` and `.media` accessors.

* **`CuemsDBManager`** — accepts `settings_dict`; `database.init()` is called here so all downstream models share one connection.

---

### `CuemsDBProject`

Full project lifecycle: create, read, update, delete, duplicate, trash, restore. Each project is a directory under `library_path/projects/<unix_name>/` containing a `cue_script.xml` file validated against `script.xsd`.

* **`CuemsDBProject`** — inherits `StringSanitizer`; all user-supplied name strings are sanitized before being used as filesystem paths.

---

### `CuemsDBMedia`

Media file intake and metadata management. On upload, generates:
- A JPEG thumbnail with `ffmpeg`.
- An audiowaveform JSON waveform (audio files only) with `audiowaveform`.
- A `.idx` video index sidecar (video files only) with `cuems-videoindexer`.

Extension categorization covers audio (`.mp3`, `.wav`, `.flac`, `.m4a`, `.ogg`, …), video (`.mp4`, `.mkv`, `.webm`, …), and image (`.png`, `.jpg`, `.webp`, …) types.

* **`CuemsDBMedia`** — inherits `StringSanitizer`; uses `CTimecode.milliseconds_rounded` for the `audiowaveform -e` duration argument.
* **`MediaType`** — enum: `MOVIE`, `AUDIO`, `IMAGE`.

---

### `CuemsDBModel`

Peewee ORM schema definitions backed by a single `SqliteDatabase` instance.

* **`Project`** — `uuid`, `name`, `unix_name`, `description`, `created`, `modified`, `in_trash`.
* **`Media`** — `uuid`, `name`, `unix_name`, `description`, `created`, `modified`, `duration`, `media_type`, `in_trash`.
* **`ProjectMedia`** — many-to-many join between `Project` and `Media`.

---

### `CuemsUpload`

Async binary upload protocol handler. Receives a JSON init message (filename, size, MD5) over the upload WebSocket connection, then accumulates binary frames into a tmp file, verifies the MD5, and hands the file to `CuemsDBMedia.new()`.

* **`CuemsUpload`** — bound to the upload WebSocket endpoint; stateful across a single upload session.

---

### `CuemsLibraryMaintenance`

Ensures the full library directory tree exists at startup:
`media/`, `media/thumbnails/`, `media/waveforms/`, `projects/`, and all corresponding `trash/` subtrees.

* **`CuemsLibraryMaintenance`** — call at service startup before any DB or file operation.

---

### `CuemsErrors`

Shared exception hierarchy.

* **`CuemsWsServerError`** — base exception for all editor errors.
* **`FileIntegrityError`** — raised when an uploaded file's MD5 does not match the declared value.
* **`NonExistentItemError`** — raised on DB lookups that return no result.
* **`NotTimeCodeError`** — raised when a string cannot be parsed as a valid CueMS timecode.
* **`EngineError`** — raised when `cuems-engine` returns a non-OK IPC response.

---

## Core Concepts

* **WebSocket command protocol** — the frontend sends JSON messages with a `command` key; the server dispatches to a method on `CuemsWsUser` and replies with a JSON response on the same connection.
* **Binary upload protocol** — media files are transferred over a dedicated WebSocket connection using a two-phase protocol: JSON init frame → binary data frames; verified by MD5.
* **Library path** — a single root directory (`library_path` from `settings.xml`) holds all projects, media files, thumbnails, waveforms, and the SQLite database.
* **Project XML** — every project's cue list is persisted as a `cue_script.xml` file validated against `script.xsd` from `cuemsutils`. The in-memory representation is a `CuemsScript` object from `cuemsutils.cues`.
* **NNG IPC** — the editor and engine communicate over a NNG request/response socket; the address is read from `settings_dict['editor_ipc']`.
* **Orphan cleanup** — before saving a project XML, `cuems-editor` nullifies any `action_target` / `target` field that references a deleted cue UUID, preventing dangling references in the saved file.

---

## Design Goals

* **Separation of concerns** — the WebSocket transport layer (`CuemsWsServer`, `CuemsWsUser`) is decoupled from the persistence layer (`CuemsDB*`); neither layer imports from the other directly.
* **Schema-validated persistence** — project XML files are validated against `cuemsutils`'s `script.xsd` on every write, catching malformed cue trees before they reach `cuems-engine`.
* **Atomic media intake** — uploaded files land in a tmp directory and are only moved to the media library after MD5 verification and metadata extraction succeed.
* **No daemon double-fork** — `cuems-editor` runs in the foreground and relies on systemd `Type=simple` for process supervision; `python-daemon` was removed because it was incompatible with NNG and asyncio.
* **Versioned filesystem moves** — `CopyMoveVersioned` from `cuemsutils` ensures project directory renames on collision never silently overwrite existing data.

---

## Installation

This package is designed to be deployed as a Debian package using `dh-virtualenv` into the `/usr/lib/cuems/` virtual environment provided by `cuems-utils`.

### PyPI

```bash
pip install cuemseditor
```

### Debian package

```bash
git clone --branch debian/bookworm https://github.com/stagesoft/cuems-editor.git
cd cuems-editor
dpkg-buildpackage -us -uc
sudo dpkg -i ../cuems-editor_*.deb
sudo apt-get install -f   # resolve any missing system dependencies
```

The Debian package installs:
- Package: `/usr/lib/cuems/lib/python3.11/site-packages/cuemseditor/`
- Console scripts: `/usr/lib/cuems/bin/cuems-editor`, `/usr/lib/cuems/bin/cuems-ws-server`

### systemd service

The service unit is provided by `cuems-common`. After installing the Debian package:

```bash
sudo systemctl enable cuems-editor
sudo systemctl start cuems-editor
sudo systemctl status cuems-editor
sudo journalctl -u cuems-editor -f
```

### Dependencies

* **`cuems-utils >= 0.1.0rc6`** — provides `CTimecode`, `XmlReaderWriter`, `ConfigManager`, `CommunicatorServices`, and the full cue data model.
* **`peewee >= 3.17`** — SQLite ORM.
* **`websockets >= 14.0`** — asyncio WebSocket server.
* **`ffmpeg`** (system) — thumbnail generation.
* **`audiowaveform`** (system) — waveform JSON generation.
* **`cuems-videoindexer`** (system, optional) — video index sidecar generation.

---

## Development

### Prerequisites

* Python 3.11+
* [hatch](https://hatch.pypa.io/) for environment management

### Editable install

```bash
pip install -e ".[dev]"
```

### Run tests

```bash
cd src && pytest
pytest --cov=cuemseditor
```

### Code style

```bash
ruff check .
```

---

## Release notes

See [CHANGELOG.md](./CHANGELOG.md) for the full history.

### v0.1.0rc1 — 2026-05-26

CTimecode hardening migration. Pins `cuemsutils` to `>=0.1.0rc6` to consume the `.milliseconds_rounded` / `.milliseconds_exact` precision split introduced in the 869cyndtv hardening pass. Migrates `CuemsDBMedia.generate_thumbnail`'s `audiowaveform -e` argument from the deprecated `.milliseconds` to `.milliseconds_rounded`. Also includes: orphaned action-target cleanup on project save, duplicate project XML sync, `unix_name` fix after versioned restore, `project_status` / `project_unload` WebSocket commands, video pre-indexing at upload time, and removal of `python-daemon` in favour of foreground systemd execution.

---

## Copyright notice

Copyright © 2026 Stagelab Coop SCCL. Authors: Adrià Masip (`adria@stagelab.coop`) and Ion Reguera (`ion@stagelab.coop`).

This work is part of **cuems-editor**. It is free software: you can redistribute it and/or modify it under the terms of the **GNU General Public License** as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but **without any warranty**; without even the implied warranty of **merchantability** or **fitness for a particular purpose**. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see [https://www.gnu.org/licenses/](https://www.gnu.org/licenses/).

The SPDX short form of this notice is: `SPDX-License-Identifier: GPL-3.0-or-later`.

---

## License

This project is licensed under the terms of the **GNU General Public License v3.0 or later (GPL-3.0-or-later)**.

You are free to use, modify, and redistribute this software under the conditions set by the license. Any derivative work must also be distributed under the same license terms.

See the [LICENSE](./LICENSE) file for the full license text.

---

### Summary of Terms

* **Permissions**:

  * Use for any purpose
  * Study and modify the source code
  * Redistribute original or modified versions

* **Conditions**:

  * Source code must be made available when distributing
  * Modifications must be licensed under GPL v3 or later
  * Include a copy of the license and preserve notices

* **Limitations**:

  * Provided *without warranty*
  * No liability for damages or misuse

---
