# CUEMS Editor - Directory Structure

Clean, organized package structure following Python best practices.

## 📁 Current Structure

```
cuems-editor/
│
├── src/                          # Source code
│   └── cuemseditor/             # Python package
│       ├── __init__.py          # Package initialization & version
│       ├── __main__.py          # Module entry point
│       ├── cli.py               # Main CLI with --daemon flag
│       ├── run_manual.py        # Manual-only convenience script
│       ├── CuemsWsServer.py     # WebSocket server
│       ├── CuemsWsUser.py       # User management
│       ├── CuemsDBModel.py      # Database models
│       ├── CuemsDBProject.py    # Project operations
│       ├── CuemsDBMedia.py      # Media operations
│       ├── CuemsUpload.py       # Upload handling
│       └── ...                  # Other modules
│
├── debian/                       # Debian packaging
│   ├── control                  # Package metadata
│   ├── rules                    # Build rules (dh-virtualenv)
│   ├── compat                   # Debhelper version
│   └── source/
│       └── format               # Source format
│
├── tests/                        # Tests & development tools
│   ├── README.md                # Test documentation
│   ├── index.html               # Web UI for testing uploads
│   ├── static/                  # Static web assets
│   │   ├── scripts/criptojs/   # Client-side crypto
│   │   └── stylesheets/        # CSS
│   ├── test*.py                 # Test scripts
│   ├── ws-server.py             # Legacy standalone script
│   ├── run-ws-server.py         # Legacy runner
│   └── ...                      # Other reference files
│
├── docs/                         # Documentation (MkDocs)
│
├── apache-conf/                  # Apache configuration examples
│   └── 000-default-le-ssl.conf # SSL virtual host config
│
├── .github/                      # GitHub workflows
│   └── workflows/
│
├── .vscode/                      # VS Code settings
│
├── pyproject.toml               # Python package configuration
├── README.md                    # Package overview
├── LICENSE                      # GPL-3.0 license
├── mkdocs.yml                   # MkDocs configuration
└── .gitignore                   # Git ignore rules
```

## 🎯 Key Directories

### `src/cuemseditor/` - Production Code
Active Python package that gets installed to `/usr/lib/cuems/lib/python3.11/site-packages/`

**Console scripts created:**
- `/usr/lib/cuems/bin/cuems-editor`
- `/usr/lib/cuems/bin/cuems-ws-server`
- `/usr/lib/cuems/bin/cuems-editor-manual`

### `debian/` - Package Build
Debian packaging using **dh-virtualenv** to install into existing `/usr/lib/cuems/` venv.

### `tests/` - Testing & Development
- Web interface for testing WebSocket uploads
- Legacy scripts for reference
- Test files

**Not included in production package** - for development only.

### `docs/` - Documentation
MkDocs documentation source.

## 🧹 What Was Cleaned

Moved from root to appropriate locations:

**To `tests/`:**
- `index.html` - Web test interface
- `static/` - JavaScript and CSS for web UI
- `test*.py` - Test scripts
- `ws-server.py`, `run-ws-server.py` - Legacy scripts
- Old Python modules - Reference only

**Removed:**
- `__pycache__/` - Python cache
- Duplicate Python files
- Build artifacts

## ✨ Benefits

1. **Clean root** - Only essential config files
2. **Clear separation** - Production vs test code
3. **Standard structure** - Follows Python packaging best practices
4. **Easy to navigate** - Everything has its place
5. **Ready to package** - Proper debian/ structure
6. **Test-friendly** - Tests isolated in tests/

## 🚀 Usage

### Development
```bash
cd src/cuemseditor
python3 cli.py                  # Manual mode
python3 cli.py --daemon         # Daemon mode
```

### Testing Web Interface
```bash
cd tests
python3 -m http.server 8000
# Open http://localhost:8000/index.html
```

### Build Package
```bash
debuild -b -uc -us -nc
```

### After Installation
```bash
/usr/lib/cuems/bin/cuems-ws-server
systemctl start cuems-editor
```

## 📝 Notes

- **Debian packaging files** needed: control, rules, changelog, postinst, prerm, postrm (create as needed)
- **Documentation files** can be recreated from templates if needed
- **Service file** lives in cuems-common package
- **Static files** for testing only - not part of Python package

## 🔄 Next Steps

If you need to recreate full documentation:
1. Reference the packaging template approach
2. Create debian packaging files
3. Add BUILD-PACKAGE.md, USAGE.md, etc. as needed

Current state: **Clean and ready for development/packaging**
