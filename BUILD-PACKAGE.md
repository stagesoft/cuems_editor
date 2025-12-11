# Building cuems-editor Debian Package

## Overview

This package uses **dh-virtualenv** to install into the existing `/usr/lib/cuems/` virtual environment created by `cuems-utils`. The package is installed properly via pip, and console scripts are created for easy execution.

## Prerequisites

```bash
sudo apt-get install -y debhelper dh-virtualenv python3-all python3-setuptools python3-pip python3-dev
```

## Dependencies

- **cuems-utils (>= 0.1.0)** - Provides `/usr/lib/cuems/` virtual environment
- **cuems-common (>= 1.0.0)** - Provides systemd service file

## Build

```bash
cd /home/ion/src/cuems/cuems-editor
debuild -b -uc -us -nc
```

The package will be created as: `../cuems-editor_0.1.0-1_all.deb`

## What Gets Installed

### 1. Python Package
- **Name**: `cuemseditor`
- **Location**: `/usr/lib/cuems/lib/python3.11/site-packages/cuemseditor/`
- **Installed via**: pip (during package build)

### 2. Console Scripts
- `/usr/lib/cuems/bin/cuems-editor`
- `/usr/lib/cuems/bin/cuems-ws-server`

Both scripts run the same entry point: `cuemseditor.cli:main`

### 3. Systemd Service
- **File**: `/lib/systemd/system/cuems-editor.service` (from cuems-common)
- **Runs**: `/usr/lib/cuems/bin/cuems-ws-server`

## Installation

```bash
# Install package
sudo dpkg -i ../cuems-editor_0.1.0-1_all.deb

# Fix dependencies if needed
sudo apt-get install -f
```

## Package Structure

```
cuems-editor/
├── src/
│   └── cuemseditor/          # Python package source
│       ├── __init__.py       # Package initialization
│       ├── __main__.py       # Module entry point
│       ├── cli.py            # Console script entry point
│       ├── CuemsWsServer.py  # Main server
│       └── ...               # Other modules
├── debian/
│   ├── control               # Package metadata & dependencies
│   ├── rules                 # Build instructions (uses dh-virtualenv)
│   ├── changelog             # Version history
│   ├── compat                # Debhelper version
│   ├── postinst              # Post-installation script
│   ├── prerm                 # Pre-removal script
│   ├── postrm                # Post-removal script
│   └── source/format         # Package format
├── pyproject.toml            # Python package config & console scripts
├── README.md                 # Package documentation
└── BUILD-PACKAGE.md          # This file
```

## How It Works

### Build Phase
1. **dh-virtualenv** prepares the existing `/usr/lib/cuems/` venv
2. **pip** installs the package from `pyproject.toml`
3. Console scripts are created automatically in `/usr/lib/cuems/bin/`
4. Package is bundled into `.deb` file

### Installation Phase
1. `dpkg` installs the package
2. `postinst` script verifies installation
3. systemd daemon is reloaded

### Runtime
1. Service runs: `/usr/lib/cuems/bin/cuems-ws-server`
2. This executes: `cuemseditor.cli:main()`
3. Server starts and runs as daemon

## Usage

### Service Management

```bash
# Start
sudo systemctl start cuems-editor

# Stop
sudo systemctl stop cuems-editor

# Restart
sudo systemctl restart cuems-editor

# Status
sudo systemctl status cuems-editor

# Enable at boot
sudo systemctl enable cuems-editor

# Logs
sudo journalctl -u cuems-editor -f
```

### Manual Execution

```bash
# Using console script
/usr/lib/cuems/bin/cuems-ws-server

# Or alternative name
/usr/lib/cuems/bin/cuems-editor

# As Python module
/usr/lib/cuems/bin/python3 -m cuemseditor
```

## Updating

1. Make code changes in `src/cuemseditor/`
2. Update version in:
   - `pyproject.toml`
   - `src/cuemseditor/__init__.py`
   - `debian/changelog` (use `dch -i`)
3. Rebuild: `debuild -b -uc -us -nc`
4. Reinstall: `sudo dpkg -i ../cuems-editor_*.deb`

## Uninstall

```bash
# Remove package
sudo apt-get remove cuems-editor

# Or with dpkg
sudo dpkg -r cuems-editor

# Purge (remove config)
sudo apt-get purge cuems-editor
```

The package will:
- Stop the service if running
- Remove package from venv site-packages
- Remove console scripts
- Clean up installed files

## Troubleshooting

### Build Fails

Check dependencies:
```bash
dpkg -l | grep -E "debhelper|dh-virtualenv"
```

Check for syntax errors in `pyproject.toml`:
```bash
python3 -m pip install --dry-run .
```

### Service Won't Start

Check service status:
```bash
systemctl status cuems-editor
```

View logs:
```bash
journalctl -u cuems-editor -n 50
```

Verify installation:
```bash
/usr/lib/cuems/bin/pip3 list | grep cuemseditor
ls -la /usr/lib/cuems/bin/cuems-ws-server
```

### Import Errors

Test imports:
```bash
/usr/lib/cuems/bin/python3 -c "import cuemseditor; print(cuemseditor.__version__)"
/usr/lib/cuems/bin/python3 -c "from cuemseditor import CuemsWsServer"
```

Check dependencies:
```bash
/usr/lib/cuems/bin/pip3 list | grep -E "cuems|peewee|websockets"
```

### Console Script Not Found

Verify console scripts were installed:
```bash
ls -la /usr/lib/cuems/bin/ | grep cuems
```

Check pyproject.toml console scripts configuration:
```bash
grep -A 3 "project.scripts" pyproject.toml
```

## Advanced

### Development Installation

For development without building .deb:
```bash
cd /home/ion/src/cuems/cuems-editor
/usr/lib/cuems/bin/pip3 install -e .
```

This creates an "editable" installation - changes to source files take effect immediately.

### Testing Package Before Build

```bash
# Test installation
/usr/lib/cuems/bin/pip3 install --dry-run .

# Check console scripts
grep -A 5 "project.scripts" pyproject.toml
```

## More Information

See `DEBIAN-PACKAGING.md` for detailed packaging information and the complete packaging approach used across CUEMS packages.
