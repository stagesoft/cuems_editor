# cuems-editor Migration to dh-virtualenv

## Summary

Successfully migrated cuems-editor to use **dh-virtualenv** for proper pip-based package installation into the existing `/usr/lib/cuems/` virtual environment.

## What Changed

### ✅ Package Structure
**Before:**
```
cuems-editor/
├── CuemsWsServer.py
├── CuemsWsUser.py
├── db.py
└── ... (loose Python files)
```

**After:**
```
cuems-editor/
├── src/
│   └── cuemseditor/          # Proper Python package
│       ├── __init__.py       # Package metadata
│       ├── __main__.py       # Module entry: python -m cuemseditor
│       ├── cli.py            # Console script entry point
│       ├── CuemsWsServer.py
│       ├── CuemsWsUser.py
│       └── ...
├── pyproject.toml            # Package configuration
└── debian/                   # Updated packaging
```

### ✅ Installation Method
**Before:**
- Copy files to `/usr/lib/cuems-editor/`
- Run from source directory
- Service pointed to development paths

**After:**
- Install via pip into `/usr/lib/cuems/lib/python3.11/site-packages/cuemseditor/`
- Console scripts created: `/usr/lib/cuems/bin/cuems-editor`, `/usr/lib/cuems/bin/cuems-ws-server`
- Service uses console script: `/usr/lib/cuems/bin/cuems-ws-server`

### ✅ Debian Packaging
**Before:**
```makefile
# debian/rules
override_dh_auto_install:
    cp -r *.py debian/cuems-editor/usr/lib/cuems-editor/
```

**After:**
```makefile
# debian/rules
export DH_VIRTUALENV_INSTALL_ROOT=/usr/lib

%:
    dh $@ --with python-virtualenv

override_dh_virtualenv:
    dh_virtualenv --python python3 \
        --install-suffix cuems \
        --use-system-packages \
        --skip-install
```

### ✅ Service File (in cuems-common)
**Before:**
```ini
ExecStart=/usr/lib/cuems/bin/python3 /home/debian/src/cuems/cuems-editor/ws-server.py
```

**After:**
```ini
ExecStart=/usr/lib/cuems/bin/cuems-ws-server
```

## Package Details

### Package Information
- **Name**: `cuemseditor`
- **Version**: `0.1.0`
- **Build system**: hatchling
- **Packaging**: dh-virtualenv

### Console Scripts
Two console scripts (same entry point):
```toml
[project.scripts]
cuems-editor = "cuemseditor.cli:main"
cuems-ws-server = "cuemseditor.cli:main"
```

Installed to:
- `/usr/lib/cuems/bin/cuems-editor`
- `/usr/lib/cuems/bin/cuems-ws-server`

### Dependencies
```toml
dependencies = [
    "cuemsutils>=0.1.0",
    "peewee>=3.17.0",
    "websockets>=14.0",
]
```

## Build & Install

```bash
# Build
cd /home/ion/src/cuems/cuems-editor
debuild -b -uc -us -nc

# Install
sudo dpkg -i ../cuems-editor_0.1.0-1_all.deb
```

## Verification

### Check Installation
```bash
# Package installed
/usr/lib/cuems/bin/pip3 list | grep cuemseditor

# Console scripts exist
ls -la /usr/lib/cuems/bin/ | grep cuems

# Import works
/usr/lib/cuems/bin/python3 -c "import cuemseditor; print(cuemseditor.__version__)"

# Console script works
/usr/lib/cuems/bin/cuems-ws-server --help
```

### Service Management
```bash
# Start service
sudo systemctl start cuems-editor

# Check status
sudo systemctl status cuems-editor

# View logs
sudo journalctl -u cuems-editor -f
```

## Benefits of New Approach

1. ✅ **Proper Package Installation**: Package installed to site-packages, not loose files
2. ✅ **Console Scripts**: Professional command-line entry points
3. ✅ **Follows cuems-utils Pattern**: Consistent with existing infrastructure
4. ✅ **Clean Imports**: `from cuemseditor import CuemsWsServer` works everywhere
5. ✅ **Version Management**: Single source of truth in `__init__.py`
6. ✅ **No Source Duplication**: Source only in site-packages, not separate directory
7. ✅ **Development Mode**: Can use `pip install -e .` for development

## Files Modified

### In cuems-editor
- ✅ Created `src/cuemseditor/` package structure
- ✅ Created `pyproject.toml`
- ✅ Created `src/cuemseditor/__init__.py` (version, exports)
- ✅ Created `src/cuemseditor/__main__.py` (module entry)
- ✅ Created `src/cuemseditor/cli.py` (console script entry)
- ✅ Updated `debian/control` (dh-virtualenv dependencies)
- ✅ Updated `debian/rules` (dh-virtualenv commands)
- ✅ Updated `debian/postinst` (verification only, no pip install)
- ✅ Updated `debian/prerm` (simplified)
- ✅ Updated `README.md` (new structure)
- ✅ Updated `BUILD-PACKAGE.md` (new process)

### In cuems-common
- ✅ Updated `etc/systemd/system/cuems-editor.service` (console script path)

## Next Steps

This approach should be applied to other CUEMS packages:
1. **cuems-engine** - Already has pyproject.toml, just update debian/
2. **cuems-nodeconf** - Needs full packaging setup
3. **Other packages** - Follow this template

## Migration Template

For other packages:
1. Create `src/packagename/` structure
2. Create `pyproject.toml` with console scripts
3. Update `debian/rules` to use dh-virtualenv
4. Update service files in cuems-common to use console scripts
5. Test build and installation

See this implementation as the reference template.
