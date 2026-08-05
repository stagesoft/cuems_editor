# Tests and Development Files

This directory contains test files, development scripts, and web interface for testing the CUEMS Editor WebSocket functionality.

## Contents

### Web Testing Interface
- **`index.html`** - Web UI for testing WebSocket file uploads
- **`static/`** - Static assets (JavaScript crypto libraries, CSS)
  - `static/scripts/criptojs/` - CryptoJS library for MD5 hashing
  - `static/stylesheets/main.css` - Upload UI styles

### Test Files
- **`test.py`** - Basic tests
- **`test_db.py`** - Database tests
- **`test_nng.py`** - NNG communication tests
- **`ws-command-responses.txt`** - WebSocket command examples

### Legacy Files (Reference Only)
- **`legacy/`** - Directory containing old Python source files
  - All original Python modules (CuemsWsServer.py, CuemsDBModel.py, etc.)
  - Old scripts (ws-server.py)
  - **Note**: These are duplicates kept for reference only
  - **Active source code** is in `src/cuemseditor/`

## Using the Web Interface
> #### **Note**: All bash code that use `cd` are referenced from the project root.


### 1. Start the WebSocket Server

```bash
cd src/cuemseditor/
python3 cli.py --port 9092
```

### 2. Serve the Web Interface

Option A - Simple Python HTTP Server:
```bash
cd tests/
python3 -m http.server 8000
```

Then open: http://localhost:8000/index.html

Option B - Using Apache (Production):
- Configure Apache to serve these files from `/var/www/cuems-editor/`
- See `apache-conf/000-default-le-ssl.conf` for reference

### 3. Test WebSocket Upload

1. Open `index.html` in browser
2. Update WebSocket URL if needed (default: `ws://localhost:9092/upload`)
3. Select a file and upload
4. Monitor console for MD5 hash calculation and upload progress

## File Paths in index.html

The HTML references static files with relative paths:
```html
<script src="static/scripts/criptojs/rollups/md5.js"></script>
<script src="static/scripts/criptojs/components/lib-typedarrays-min.js"></script>
<link rel="stylesheet" href="./static/stylesheets/main.css">
```

These paths work when serving from this `tests/` directory.

## Apache Integration

For production deployment with Apache:

1. Copy web files to Apache document root:
   ```bash
   sudo cp -r tests/index.html tests/static /var/www/cuems-editor/
   ```

2. Configure Apache proxy (see `apache-conf/000-default-le-ssl.conf`):
   ```apache
   ProxyPass "/ws" "ws://127.0.0.1:9092/"
   ProxyPass "/upload" "ws://127.0.0.1:9092/upload"
   ```

3. Access via: https://your-domain/cuems-editor/index.html

## Notes

- **Web interface is for testing only** - Not part of the Python package
- **Static files are client-side** - Served by web server, not WebSocket server
- **WebSocket server** - Runs separately on port 9092
- **Legacy scripts** - Old files kept for reference, use new `cli.py` instead

## Active Package

The production code is in `src/cuemseditor/`:
```bash
cd src/cuemseditor
python3 cli.py            # Run server
python3 cli.py --port 9092  # Specify port
```

Or after installation:
```bash
/usr/lib/cuems/bin/cuems-editor
systemctl start cuems-editor
```
