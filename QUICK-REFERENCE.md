# CUEMS Packaging Quick Reference

Quick command reference for the CUEMS dh-virtualenv packaging approach.

## 📦 For cuems-editor (Reference Implementation)

### Development
```bash
# Run in foreground (manual mode)
cd /home/ion/src/cuems/cuems-editor/src/cuemseditor
python3 cli.py
python3 cli.py --port 9093

# Test daemon mode
python3 cli.py --daemon

# Quick manual script
python3 run_manual.py
```

### Build & Install
```bash
cd /home/ion/src/cuems/cuems-editor
debuild -b -uc -us -nc
sudo dpkg -i ../cuems-editor_0.1.0-1_all.deb
sudo apt-get install -f
```

### Usage
```bash
# Manual mode (foreground)
/usr/lib/cuems/bin/cuems-ws-server
/usr/lib/cuems/bin/cuems-ws-server --port 9093
/usr/lib/cuems/bin/cuems-editor-manual

# Daemon mode (systemd)
sudo systemctl start cuems-editor
sudo systemctl status cuems-editor
sudo journalctl -u cuems-editor -f
```

---

## 🎯 Apply to Other Modules

### For cuems-engine

**Status**: Has pyproject.toml, needs debian/ update

```bash
cd /home/ion/src/cuems/python/cuems-engine

# 1. Update debian/control - add dh-virtualenv
# 2. Update debian/rules - use template from cuems-editor
# 3. Console scripts already defined in pyproject.toml:
#    - controller-engine
#    - node-engine
# 4. Update service files in cuems-common:
#    ExecStart=/usr/lib/cuems/bin/controller-engine --daemon
#    ExecStart=/usr/lib/cuems/bin/node-engine --daemon
```

### For cuems-nodeconf

**Status**: No packaging, needs full setup

```bash
cd /home/ion/src/cuems/cuems-nodeconf

# 1. Create structure
mkdir -p src/cuemsnodeconf debian/source

# 2. Move files
mv *.py src/cuemsnodeconf/

# 3. Create files from template:
#    - src/cuemsnodeconf/__init__.py
#    - src/cuemsnodeconf/__main__.py
#    - src/cuemsnodeconf/cli.py
#    - src/cuemsnodeconf/run_manual.py
#    - pyproject.toml
#    - debian/* (all files)

# 4. Add service file to cuems-common:
#    etc/systemd/system/cuems-nodeconf.service
```

---

## 📋 Template Checklist

When creating a new package:

### Directory Structure
- [ ] `src/packagename/` created
- [ ] `debian/` created
- [ ] `debian/source/` created

### Python Package Files
- [ ] `src/packagename/__init__.py` (version, exports)
- [ ] `src/packagename/__main__.py` (module entry)
- [ ] `src/packagename/cli.py` (dual-mode CLI)
- [ ] `src/packagename/run_manual.py` (manual only)
- [ ] `pyproject.toml` (package config)

### Debian Files
- [ ] `debian/control` (dependencies)
- [ ] `debian/rules` (dh-virtualenv)
- [ ] `debian/changelog` (version)
- [ ] `debian/compat` (13)
- [ ] `debian/postinst` (verification)
- [ ] `debian/prerm` (stop service)
- [ ] `debian/postrm` (reload systemd)
- [ ] `debian/source/format` (3.0 native)

### Service File (in cuems-common)
- [ ] `etc/systemd/system/cuems-packagename.service`
- [ ] Added to `debian/install`
- [ ] Uses `--daemon` flag

### Documentation
- [ ] `README.md` (overview)
- [ ] `BUILD-PACKAGE.md` (build guide)
- [ ] `USAGE.md` (usage examples)

---

## 🔧 Key Patterns

### Package Naming
```
Debian package:  cuems-modulename  (hyphens)
Python package:  cuemsmodulename   (no separators)
Import:          import cuemsmodulename
Console script:  /usr/lib/cuems/bin/cuems-modulename
Service file:    cuems-modulename.service
```

### Console Scripts (in pyproject.toml)
```toml
[project.scripts]
cuems-modulename = "cuemsmodulename.cli:main"
cuems-modulename-manual = "cuemsmodulename.run_manual:main"
```

### Service File Pattern
```ini
[Service]
ExecStart=/usr/lib/cuems/bin/cuems-modulename --daemon
```

### cli.py Entry Point
```python
def main():
    parser = argparse.ArgumentParser(...)
    parser.add_argument('--daemon', action='store_true')
    args = parser.parse_args()
    
    if args.daemon:
        run_daemon_mode()
    else:
        run_manual()
```

---

## 🧪 Testing Commands

```bash
# Development test
cd src/packagename && python3 cli.py

# Build test
debuild -b -uc -us -nc

# Package contents
dpkg -c ../packagename_*.deb

# Install test
sudo dpkg -i ../packagename_*.deb

# Console script test
/usr/lib/cuems/bin/cuems-modulename
/usr/lib/cuems/bin/cuems-modulename --daemon

# Service test
sudo systemctl start cuems-modulename
sudo systemctl status cuems-modulename
sudo journalctl -u cuems-modulename -f

# Import test
/usr/lib/cuems/bin/python3 -c "import cuemsmodulename; print(cuemsmodulename.__version__)"

# Uninstall test
sudo apt-get remove cuems-modulename
```

---

## 📚 Documentation Files

| File | Purpose |
|------|---------|
| `PACKAGING-TEMPLATE.md` | Complete template with all file examples |
| `QUICK-REFERENCE.md` | This file - quick commands |
| `MIGRATION-SUMMARY.md` | Migration details for cuems-editor |
| `BUILD-PACKAGE.md` | Build instructions |
| `USAGE.md` | Usage examples and troubleshooting |
| `README.md` | Package overview |

---

## 🎓 Learn from cuems-editor

The cuems-editor package is the **reference implementation**:

```bash
# Structure
/home/ion/src/cuems/cuems-editor/
├── src/cuemseditor/          # Package with all patterns
├── debian/                   # dh-virtualenv packaging
├── pyproject.toml            # Console scripts
└── *.md                      # Documentation

# Study these files
cat src/cuemseditor/cli.py          # Dual-mode CLI
cat src/cuemseditor/run_manual.py   # Manual-only script
cat pyproject.toml                  # Console scripts
cat debian/rules                    # dh-virtualenv
cat debian/postinst                 # Verification
```

---

## 💡 Tips

1. **Always test locally first**: `cd src/packagename && python3 cli.py`
2. **Use PYTHONPATH for module testing**: `PYTHONPATH=src python3 -m packagename`
3. **Check console scripts after install**: `ls -la /usr/lib/cuems/bin/`
4. **Service file needs --daemon**: Don't forget the flag!
5. **Follow the naming convention**: Hyphens for Debian, none for Python
6. **Test both modes**: Manual (foreground) and daemon (systemd)
7. **Keep service files in cuems-common**: Don't put them in module packages

---

## 🚀 Next Steps

1. Review `PACKAGING-TEMPLATE.md` for complete details
2. Use cuems-editor as reference implementation
3. Apply to cuems-engine (easy - has pyproject.toml)
4. Apply to cuems-nodeconf (moderate - needs full setup)
5. Update cuems-common with service files as you go
