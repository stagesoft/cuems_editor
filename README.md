# CUEMS Editor

CUEMS Editor - Collaborative Universal Edit Management System

## Description

CUEMS Editor provides a WebSocket-based server for collaborative editing and project management. It includes modules for database management, media handling, and project organization.

## Package Structure

```
cuems-editor/
├── src/
│   └── cuemseditor/          # Main package
│       ├── __init__.py
│       ├── __main__.py       # Module entry point
│       ├── cli.py            # Console script entry point
│       ├── CuemsWsServer.py  # WebSocket server
│       ├── CuemsWsUser.py    # User management
│       ├── CuemsDBModel.py   # Database models
│       ├── CuemsDBProject.py # Project operations
│       ├── CuemsDBMedia.py   # Media operations
│       └── ...
├── debian/                   # Debian packaging
├── pyproject.toml           # Python package configuration
└── README.md
```

## Installation

This package is designed to be installed as a Debian package using dh-virtualenv into the `/usr/lib/cuems/` virtual environment provided by `cuems-utils`.

### Building the Debian Package

```bash
cd /home/ion/src/cuems/cuems-editor
debuild -b -uc -us -nc
```

### Installing

```bash
sudo dpkg -i ../cuems-editor_0.1.0-1_all.deb
sudo apt-get install -f  # Install any missing dependencies
```

## Dependencies

- **cuems-utils (>= 0.1.0)** - Provides the virtual environment at `/usr/lib/cuems/` with Python interpreter and dependencies (cuemsutils, peewee, websockets, etc.)
- **cuems-common (>= 1.0.0)** - Provides the systemd service file

## Usage

### Systemd Service

The service is managed by systemd using the service file from `cuems-common`.

```bash
# Start the service
sudo systemctl start cuems-editor

# Enable at boot
sudo systemctl enable cuems-editor

# Check status
sudo systemctl status cuems-editor

# View logs
sudo journalctl -u cuems-editor -f
```

### Manual Execution

```bash
# Using console script
/usr/lib/cuems/bin/cuems-ws-server

# Or the alternative name
/usr/lib/cuems/bin/cuems-editor

# Using Python module
/usr/lib/cuems/bin/python3 -m cuemseditor
```

### As a Library

```python
from cuemseditor import CuemsWsServer

# Create server instance
server = CuemsWsServer(settings_dict, mappings_dict)
server.start(9092)
```

## Components

- **CuemsWsServer**: WebSocket server implementation
- **CuemsWsUser**: User session management
- **CuemsProjectManager**: Project management functionality
- **CuemsDBModel**: Database models (using Peewee ORM)
- **CuemsDBProject**: Project database operations
- **CuemsDBMedia**: Media database operations
- **CuemsUpload**: File upload handling
- **CuemsLibraryMaintenance**: Library maintenance utilities

## Development

### Package Installation

After building, the package is installed to:
- Package: `/usr/lib/cuems/lib/python3.11/site-packages/cuemseditor/`
- Console scripts: `/usr/lib/cuems/bin/cuems-editor`, `/usr/lib/cuems/bin/cuems-ws-server`

### Console Scripts

Two console scripts are provided (both run the same entry point):
- `cuems-editor` - Main command
- `cuems-ws-server` - Alternative name for compatibility

Both are defined in `pyproject.toml`:
```toml
[project.scripts]
cuems-editor = "cuemseditor.cli:main"
cuems-ws-server = "cuemseditor.cli:main"
```

## License

See LICENSE file for details (GPL-3.0).
