# CUEMS Package Template - dh-virtualenv with Dual Execution Modes

This document serves as a template for packaging CUEMS Python modules using dh-virtualenv, supporting both manual/development and daemon/systemd execution modes.

## 📋 Table of Contents
1. [Overview](#overview)
2. [Package Structure](#package-structure)
3. [File Templates](#file-templates)
4. [Implementation Steps](#implementation-steps)
5. [Testing Checklist](#testing-checklist)

---

## Overview

### Architecture Principles

1. **Single Virtual Environment**: All CUEMS packages install into `/usr/lib/cuems/` (created by cuems-utils)
2. **Proper pip Installation**: Packages installed via pip during build (not copied files)
3. **Console Scripts**: Professional CLI entry points in `/usr/lib/cuems/bin/`
4. **Dual Execution Modes**:
   - **Manual Mode**: Foreground execution for development/testing
   - **Daemon Mode**: Background execution with systemd integration
5. **Service Files in cuems-common**: Centralized systemd service management

### Package Pattern

```
packagename/
├── src/
│   └── packagename/          # Python package (underscores for import)
│       ├── __init__.py       # Version, exports
│       ├── __main__.py       # Module entry point
│       ├── cli.py            # Dual-mode CLI entry point
│       ├── run_manual.py     # Convenience manual-only script
│       └── ...               # Module files
├── debian/
│   ├── control               # Dependencies, dh-virtualenv
│   ├── rules                 # dh-virtualenv commands
│   ├── changelog             # Version history
│   ├── compat                # Debhelper version
│   ├── postinst              # Post-installation verification
│   ├── prerm                 # Pre-removal cleanup
│   ├── postrm                # Post-removal cleanup
│   └── source/format         # Package format
├── pyproject.toml            # Package config, console scripts
├── README.md                 # Package documentation
├── BUILD-PACKAGE.md          # Build instructions
├── USAGE.md                  # Usage guide
└── PACKAGING-TEMPLATE.md     # This file (for cuems-editor only)
```

---

## Package Structure

### Naming Convention

- **Debian Package Name**: `cuems-modulename` (hyphens)
- **Python Package Name**: `cuemsmodulename` (no separators, lowercase)
- **Import Name**: `cuemsmodulename` (e.g., `import cuemseditor`)
- **Console Script**: `cuems-modulename` (hyphens)

Example:
```
Debian: cuems-editor
Python: cuemseditor
Import: from cuemseditor import ...
Script: /usr/lib/cuems/bin/cuems-editor
```

### Directory Layout

```
src/
└── cuemsmodulename/          # Package name (no hyphens)
    ├── __init__.py           # Package initialization
    ├── __main__.py           # python -m cuemsmodulename
    ├── cli.py                # Main CLI with --daemon flag
    ├── run_manual.py         # Manual-only convenience script
    ├── MainClass.py          # Main application class
    └── ...                   # Other modules
```

---

## File Templates

### 1. `src/cuemsmodulename/__init__.py`

```python
"""
CUEMS ModuleName - Brief description

Detailed description of what this module does.
"""

__version__ = "0.1.0"
__author__ = "CUEMS Team"
__license__ = "GPL-3.0"

# Package exports
from cuemsmodulename.MainClass import MainClass

__all__ = [
    "MainClass",
]
```

### 2. `src/cuemsmodulename/__main__.py`

```python
#!/usr/bin/env python3
"""
Entry point for cuemsmodulename when run as a module:
python -m cuemsmodulename
"""

from cuemsmodulename.cli import main

if __name__ == '__main__':
    main()
```

### 3. `src/cuemsmodulename/cli.py`

```python
#!/usr/bin/env python3
"""
CLI entry point for CUEMS ModuleName

Supports two modes:
1. Manual/Development mode: Runs in foreground (default)
2. Daemon mode: Runs as system daemon (--daemon flag)
"""

import sys
import os
import argparse

# Add src directory to path for development (when not installed)
if __name__ == '__main__' and __package__ is None:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

from cuemsutils.log import Logger
from cuemsmodulename.MainClass import MainClass
from cuemsutils.daemon import run_daemon


def get_configuration():
    """Get application configuration"""
    # TODO: Implement configuration loading
    config = {
        'setting1': 'value1',
        'setting2': 'value2',
    }
    return config


def run_manual():
    """Run in manual/development mode (foreground)"""
    Logger.info("Starting CUEMS ModuleName in MANUAL mode (foreground)")
    
    config = get_configuration()
    
    # Create and start application
    app = MainClass(config)
    
    try:
        app.start()  # Your application's start method
    except KeyboardInterrupt:
        Logger.info("Received interrupt signal, stopping...")
        # app.stop() if such method exists
    except Exception as e:
        Logger.error(f"Application error: {type(e).__name__}: {e}")
        raise


def run_daemon_mode():
    """Run in daemon mode (for systemd)"""
    Logger.info("Starting CUEMS ModuleName in DAEMON mode")
    
    config = get_configuration()
    
    # Create application and run as daemon
    app = MainClass(config)
    run_daemon(app, 'cuems_modulename')


def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(
        description='CUEMS ModuleName',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run in manual/development mode (foreground)
  %(prog)s
  
  # Run as daemon (for systemd service)
  %(prog)s --daemon
        """
    )
    
    parser.add_argument(
        '--daemon',
        action='store_true',
        help='Run as daemon (for systemd service). Default: run in foreground'
    )
    
    # Add module-specific arguments here
    # parser.add_argument('--port', type=int, default=9092, help='Port number')
    
    args = parser.parse_args()
    
    if args.daemon:
        run_daemon_mode()
    else:
        run_manual()


if __name__ == '__main__':
    main()
```

### 4. `src/cuemsmodulename/run_manual.py`

```python
#!/usr/bin/env python3
"""
Quick script to run CUEMS ModuleName in manual/development mode

This is a convenience wrapper that always runs in foreground mode.
For daemon mode (systemd), use the main cli.py with --daemon flag.
"""

import sys
import os

# Add src directory to path for development (when not installed)
if __name__ == '__main__' and __package__ is None:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

from cuemsmodulename.cli import run_manual

if __name__ == '__main__':
    print("Starting CUEMS ModuleName in manual mode")
    print("Press Ctrl+C to stop")
    print("-" * 60)
    
    run_manual()
```

### 5. `pyproject.toml`

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "cuemsmodulename"
version = "0.1.0"
description = "CUEMS ModuleName - Brief description"
readme = "README.md"
requires-python = ">=3.11"
license = "GPL-3.0"
keywords = ["cuems", "modulename"]
authors = [
    { name = "CUEMS Team", email = "cuems@example.com" },
]
classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
]
dependencies = [
    "cuemsutils>=0.1.0",
    # Add other dependencies here
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-cov>=4.0",
]

[project.urls]
Homepage = "https://github.com/cuems/cuems-modulename"
Documentation = "https://github.com/cuems/cuems-modulename#readme"
Repository = "https://github.com/cuems/cuems-modulename"

[project.scripts]
cuems-modulename = "cuemsmodulename.cli:main"
cuems-modulename-manual = "cuemsmodulename.run_manual:main"

[tool.hatch.version]
path = "src/cuemsmodulename/__init__.py"

[tool.hatch.build.targets.sdist]
packages = ["src/cuemsmodulename"]

[tool.hatch.build.targets.wheel]
packages = ["src/cuemsmodulename"]

[tool.hatch.build.targets.wheel.sources]
"src" = ""
```

### 6. `debian/control`

```debcontrol
Source: cuems-modulename
Section: python
Priority: optional
Maintainer: CUEMS Team <cuems@example.com>
Build-Depends: debhelper-compat (= 13),
               dh-virtualenv (>= 1.2),
               python3-all,
               python3-setuptools,
               python3-pip,
               python3-dev
Standards-Version: 4.6.0
Homepage: https://github.com/cuems/cuems-modulename

Package: cuems-modulename
Architecture: all
Depends: ${misc:Depends},
         ${python3:Depends},
         cuems-utils (>= 0.1.0),
         cuems-common (>= 1.0.0),
         python3 (>= 3.11)
Description: CUEMS ModuleName - Brief description
 Longer description of what this module does.
 Multiple lines are supported.
 .
 This package installs into the /usr/lib/cuems/ virtual environment
 provided by cuems-utils. Console scripts are installed to
 /usr/lib/cuems/bin/.
 .
 The systemd service file is provided by cuems-common.
```

### 7. `debian/rules`

```makefile
#!/usr/bin/make -f

export DH_VIRTUALENV_INSTALL_ROOT=/usr/lib
export DH_VERBOSE = 1

%:
	dh $@ --with python-virtualenv

override_dh_virtualenv:
	dh_virtualenv --python python3 \
		--install-suffix cuems \
		--use-system-packages \
		--skip-install

override_dh_auto_install:
	# Install package using pip into existing venv
	/usr/lib/cuems/bin/pip3 install --no-deps .

override_dh_auto_clean:
	rm -rf build *.egg-info src/*.egg-info
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	dh_auto_clean

override_dh_auto_test:
	# Skip tests during package build
```

### 8. `debian/postinst`

```bash
#!/bin/bash
set -e

CUEMS_VENV="/usr/lib/cuems"

case "$1" in
    configure)
        # Verify virtual environment exists
        if [ ! -f "$CUEMS_VENV/bin/python3" ]; then
            echo "ERROR: Virtual environment not found at $CUEMS_VENV" >&2
            echo "Please ensure cuems-utils package is properly installed." >&2
            exit 1
        fi
        
        # Verify console scripts were installed
        if [ ! -f "$CUEMS_VENV/bin/cuems-modulename" ]; then
            echo "WARNING: Console script cuems-modulename not found" >&2
        fi
        
        # Reload systemd (service file from cuems-common)
        if command -v deb-systemd-helper >/dev/null 2>&1; then
            deb-systemd-helper update-state >/dev/null 2>&1 || true
        elif command -v systemctl >/dev/null 2>&1; then
            systemctl daemon-reload || true
        fi
        
        echo "cuems-modulename installed successfully."
        echo "Package: cuemsmodulename installed to $CUEMS_VENV/lib/python*/site-packages/"
        echo "Console scripts: $CUEMS_VENV/bin/cuems-modulename"
        echo ""
        echo "To start: systemctl start cuems-modulename"
        echo "To enable: systemctl enable cuems-modulename"
        ;;
        
    abort-upgrade|abort-remove|abort-deconfigure)
        ;;
        
    *)
        echo "postinst called with unknown argument \`$1'" >&2
        exit 1
        ;;
esac

#DEBHELPER#

exit 0
```

### 9. `debian/prerm`

```bash
#!/bin/bash
set -e

case "$1" in
    remove|deconfigure)
        # Stop service if running (service file from cuems-common)
        if [ -d /run/systemd/system ]; then
            if command -v systemctl >/dev/null 2>&1; then
                if systemctl is-active --quiet cuems-modulename 2>/dev/null; then
                    systemctl stop cuems-modulename || true
                fi
            fi
        fi
        ;;
        
    upgrade)
        # Don't stop service on upgrade
        ;;
        
    failed-upgrade)
        ;;
        
    *)
        echo "prerm called with unknown argument \`$1'" >&2
        exit 1
        ;;
esac

#DEBHELPER#

exit 0
```

### 10. `debian/postrm`

```bash
#!/bin/bash
set -e

case "$1" in
    purge|remove|upgrade|failed-upgrade|abort-install|abort-upgrade|disappear)
        # Reload systemd after removing service file
        if [ -d /run/systemd/system ]; then
            systemctl daemon-reload || true
        fi
        ;;
        
    *)
        echo "postrm called with unknown argument \`$1'" >&2
        exit 1
        ;;
esac

#DEBHELPER#

exit 0
```

### 11. `debian/changelog`

```
cuems-modulename (0.1.0-1) bookworm; urgency=medium

  * Initial Debian package release
  * Main module functionality
  * Integration with cuems-utils virtual environment
  * Systemd service support

 -- CUEMS Team <cuems@example.com>  Thu, 11 Dec 2025 00:00:00 +0000
```

### 12. `debian/compat`

```
13
```

### 13. `debian/source/format`

```
3.0 (native)
```

### 14. Service File in `cuems-common`

File: `cuems-common/etc/systemd/system/cuems-modulename.service`

```ini
[Unit]
Description=cuems-modulename
PartOf=cuems-controller.target
After=cuems-node.service
Requires=cuems-node.service

[Service]
Type=exec
Restart=always
ExecStart=/usr/lib/cuems/bin/cuems-modulename --daemon

[Install]
WantedBy=cuems-controller.target
```

---

## Implementation Steps

### Step 1: Create Package Structure

```bash
cd /home/ion/src/cuems/cuems-modulename

# Create directory structure
mkdir -p src/cuemsmodulename
mkdir -p debian/source
mkdir -p docs

# Move existing Python files to package
mv *.py src/cuemsmodulename/ 2>/dev/null || true

# Create debian files
touch debian/{control,rules,changelog,compat,postinst,prerm,postrm}
touch debian/source/format
chmod +x debian/{rules,postinst,prerm,postrm}
```

### Step 2: Create Package Files

1. Create `src/cuemsmodulename/__init__.py` (use template above)
2. Create `src/cuemsmodulename/__main__.py` (use template above)
3. Create `src/cuemsmodulename/cli.py` (adapt template to your needs)
4. Create `src/cuemsmodulename/run_manual.py` (use template above)
5. Create `pyproject.toml` (adapt template)

### Step 3: Create Debian Packaging

1. Fill in `debian/control` (update package name, description, dependencies)
2. Fill in `debian/rules` (usually no changes needed)
3. Fill in `debian/changelog` (update version, date, changes)
4. Fill in `debian/compat` (copy as-is)
5. Fill in `debian/source/format` (copy as-is)
6. Fill in `debian/postinst` (update script names)
7. Fill in `debian/prerm` (update service name)
8. Fill in `debian/postrm` (copy as-is)

### Step 4: Update Main Class

Ensure your main application class has:
- `__init__(config)` - Constructor
- `start()` method - For manual mode OR compatible with `run_daemon()`

If using `run_daemon()`, ensure the class follows the daemon protocol.

### Step 5: Create Service File in cuems-common

```bash
cd /home/ion/src/cuems/cuems-common/etc/systemd/system

# Create service file
cat > cuems-modulename.service << 'EOF'
[Unit]
Description=cuems-modulename
PartOf=cuems-controller.target
After=cuems-node.service
Requires=cuems-node.service

[Service]
Type=exec
Restart=always
ExecStart=/usr/lib/cuems/bin/cuems-modulename --daemon

[Install]
WantedBy=cuems-controller.target
EOF
```

Update `cuems-common/debian/install` to include the service file:
```
etc/systemd/system/cuems-modulename.service lib/systemd/system/
```

### Step 6: Create Documentation

1. Create `README.md` (package overview)
2. Create `BUILD-PACKAGE.md` (build instructions)
3. Create `USAGE.md` (usage examples)

### Step 7: Test

```bash
# Test imports during development
cd src/cuemsmodulename
python3 cli.py

# Test as module
cd ..
PYTHONPATH=src python3 -m cuemsmodulename

# Build package
cd /home/ion/src/cuems/cuems-modulename
debuild -b -uc -us -nc

# Install and test
sudo dpkg -i ../cuems-modulename_*.deb
/usr/lib/cuems/bin/cuems-modulename
sudo systemctl start cuems-modulename
sudo systemctl status cuems-modulename
```

---

## Testing Checklist

### Development Testing
- [ ] Import works: `python3 -c "import cuemsmodulename"`
- [ ] CLI runs: `python3 cli.py`
- [ ] Manual mode works: `python3 cli.py`
- [ ] Module mode works: `python3 -m cuemsmodulename`
- [ ] Version accessible: `python3 -c "import cuemsmodulename; print(cuemsmodulename.__version__)"`

### Build Testing
- [ ] Package builds: `debuild -b -uc -us -nc`
- [ ] No lintian errors: `lintian ../cuems-modulename_*.deb`
- [ ] Files in package: `dpkg -c ../cuems-modulename_*.deb`

### Installation Testing
- [ ] Package installs: `sudo dpkg -i ../cuems-modulename_*.deb`
- [ ] Dependencies resolved: `sudo apt-get install -f`
- [ ] Console script exists: `ls -la /usr/lib/cuems/bin/cuems-modulename`
- [ ] Package in site-packages: `/usr/lib/cuems/bin/pip3 list | grep cuemsmodulename`
- [ ] Import works: `/usr/lib/cuems/bin/python3 -c "import cuemsmodulename"`

### Console Script Testing
- [ ] Manual mode: `/usr/lib/cuems/bin/cuems-modulename`
- [ ] Daemon mode: `/usr/lib/cuems/bin/cuems-modulename --daemon`
- [ ] Manual script: `/usr/lib/cuems/bin/cuems-modulename-manual`
- [ ] Help text: `/usr/lib/cuems/bin/cuems-modulename --help`

### Service Testing
- [ ] Service file exists: `ls -la /lib/systemd/system/cuems-modulename.service`
- [ ] Service starts: `sudo systemctl start cuems-modulename`
- [ ] Service status: `sudo systemctl status cuems-modulename`
- [ ] Service logs: `sudo journalctl -u cuems-modulename -n 20`
- [ ] Service stops: `sudo systemctl stop cuems-modulename`
- [ ] Service restart: `sudo systemctl restart cuems-modulename`

### Integration Testing
- [ ] Works with cuems-utils dependencies
- [ ] IPC communication works (if applicable)
- [ ] Configuration loading works
- [ ] Service dependencies start correctly

---

## Quick Command Reference

```bash
# Create package structure
mkdir -p src/cuemsmodulename debian/source

# Test during development
cd src/cuemsmodulename && python3 cli.py

# Build package
debuild -b -uc -us -nc

# Install
sudo dpkg -i ../cuems-modulename_*.deb

# Test console script
/usr/lib/cuems/bin/cuems-modulename

# Test service
sudo systemctl start cuems-modulename
sudo systemctl status cuems-modulename
sudo journalctl -u cuems-modulename -f

# Uninstall
sudo apt-get remove cuems-modulename
```

---

## Common Gotchas

1. **Package name vs import name**: Use hyphens for package, no hyphens for Python
2. **Imports during development**: Add path adjustment in `__main__` checks
3. **Service file location**: In cuems-common, not in the module package
4. **Console script names**: Use hyphens, not underscores
5. **--daemon flag**: Don't forget to add it in service file
6. **Dependencies**: Declare all Python dependencies in pyproject.toml
7. **File permissions**: Make debian/{rules,postinst,prerm,postrm} executable

---

## Examples

### Apply to cuems-engine

Already has `pyproject.toml` with console scripts, just:
1. Update `debian/` files to use dh-virtualenv
2. Ensure `scripts/controller_engine.py` and `scripts/node_engine.py` have proper entry points
3. Update service files in cuems-common

### Apply to cuems-nodeconf

Needs full setup:
1. Create complete structure from this template
2. Move Python files to `src/cuemsnodeconf/`
3. Create cli.py with manual/daemon modes
4. Add service file to cuems-common

---

## Success Criteria

A properly packaged CUEMS module should:
- ✅ Install via dh-virtualenv into `/usr/lib/cuems/`
- ✅ Be importable: `from cuemsmodulename import ...`
- ✅ Have console scripts in `/usr/lib/cuems/bin/`
- ✅ Support both manual and daemon modes
- ✅ Integrate with systemd via cuems-common
- ✅ Be testable during development without installation
- ✅ Follow consistent naming and structure

---

## Reference Implementation

See **cuems-editor** package for complete working example:
- `/home/ion/src/cuems/cuems-editor/`
- All files follow this template
- Fully tested and working
