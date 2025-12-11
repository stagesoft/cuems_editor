# Debian Packaging for CUEMS Editor

## Overview

This document describes the Debian packaging setup for `cuems-editor`, which installs the Python package into the existing `/usr/lib/cuems/` virtual environment provided by the `cuems-utils` package.

## Package Structure

### Debian Files Created

```
debian/
├── changelog          # Package version history
├── compat            # Debhelper compatibility level (13)
├── control           # Package metadata and dependencies
├── rules             # Build instructions
├── postinst          # Post-installation script (installs to venv)
├── prerm             # Pre-removal script (uninstalls from venv)
├── postrm            # Post-removal script (cleanup)
└── source/
    └── format        # Source package format
```

**Note:** The systemd service file (`cuems-editor.service`) is provided by the `cuems-common` package, not by this package.

### Other Files Created

- `setup.py` - Python package setup script used by pip during installation
- `MANIFEST.in` - Specifies which files to include in Python package
- `README.md` - Package documentation

## How It Works

1. **Build Phase** (`debian/rules`):
   - Copies Python modules to `/usr/lib/cuems-editor/`
   - Copies static files

2. **Installation Phase** (`debian/postinst`):
   - Verifies `/usr/lib/cuems/` virtual environment exists (from cuems-utils)
   - Reloads systemd daemon

3. **Runtime**:
   - Service runs: `/usr/lib/cuems/bin/python3 /usr/lib/cuems-editor/ws-server.py`
   - Uses venv's Python interpreter (has cuemsutils, peewee, etc. installed)
   - Python finds modules in `/usr/lib/cuems-editor/` (same directory as script)

4. **Removal Phase** (`debian/prerm`):
   - Stops the systemd service if running

## Building the Package

```bash
cd /home/ion/src/cuems/cuems-editor
debuild -b -uc -us -nc
```

Options explained:
- `-b` : Build binary package only
- `-uc` : Do not sign .changes file
- `-us` : Do not sign source package
- `-nc` : Do not clean before build

## Installing the Package

```bash
# Install the built package
sudo dpkg -i ../cuems-editor_0.1.0-1_all.deb

# Install any missing dependencies
sudo apt-get install -f
```

## Using the Package

### Systemd Service

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
/usr/lib/cuems/bin/python3 /usr/lib/cuems-editor/run-ws-server.py
```

Or using the symlink:

```bash
cuems-ws-server
```

## Package Dependencies

The package declares these dependencies in `debian/control`:
- `cuems-utils (>= 0.1.0)` - Provides the virtual environment at `/usr/lib/cuems/` with Python interpreter and dependencies (cuemsutils, peewee, websockets, etc.)
- `cuems-common (>= 1.0.0)` - Provides the systemd service file and other common configurations
- Standard Python 3 dependencies

## File Locations After Installation

```
/usr/lib/cuems-editor/          # Python modules and application files
/usr/lib/cuems/                 # Virtual environment (from cuems-utils)
/lib/systemd/system/cuems-editor.service  # Systemd service (from cuems-common)
```

## Systemd Service Configuration

The service file is provided by `cuems-common` package and configured with:
- **Type**: `exec`
- **Restart**: `always`
- **Executable**: `/usr/lib/cuems/bin/python3`
- **Script**: `/usr/lib/cuems-editor/ws-server.py`
- **Part of**: `cuems-controller.target`
- **Dependencies**: `cuems-node.service` and `apache2.service`

The service is managed by systemd and integrated with the broader CUEMS system through `cuems-controller.target`.

## Updating the Package Version

1. Edit `debian/changelog`:
   ```bash
   dch -i  # or manually edit
   ```

2. Update version in `setup.py` if needed

3. Rebuild the package

## Troubleshooting

### Virtual environment not found
If you get "ERROR: Virtual environment not found at /usr/lib/cuems/":
- Ensure `cuems-utils` package is installed: `dpkg -l | grep cuems-utils`
- Check if venv exists: `ls -la /usr/lib/cuems/`

### Service file not found
The service file is provided by `cuems-common`:
- Ensure `cuems-common` package is installed: `dpkg -l | grep cuems-common`
- Check service file: `ls -la /lib/systemd/system/cuems-editor.service`

### Service fails to start
- Check logs: `sudo journalctl -u cuems-editor -n 50`
- Verify user exists: `id cuems`
- Check file permissions
- Verify Python path: `ls -la /usr/lib/cuems/bin/python3`

### Import errors
- Check files exist: `ls -la /usr/lib/cuems-editor/`
- Check installed packages in venv: `/usr/lib/cuems/bin/pip3 list`
- Verify cuemsutils and other dependencies are installed in venv
- Test imports: `/usr/lib/cuems/bin/python3 -c "import sys; sys.path.insert(0, '/usr/lib/cuems-editor'); import CuemsWsServer"`
