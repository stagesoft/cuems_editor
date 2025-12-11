# CUEMS Editor Usage Guide

## Running Modes

CUEMS Editor supports two execution modes:

### 1. Manual/Development Mode (Foreground)
For testing, development, or manual operation. Runs in the foreground and can be stopped with Ctrl+C.

### 2. Daemon Mode (Background)
For production use with systemd. Runs as a system service with automatic restart.

---

## Usage Methods

### A. Using Console Scripts (After Installation)

#### Manual Mode (Foreground)
```bash
# Default port (9092)
/usr/lib/cuems/bin/cuems-ws-server

# Custom port
/usr/lib/cuems/bin/cuems-ws-server --port 9093

# Using the dedicated manual script
/usr/lib/cuems/bin/cuems-editor-manual
/usr/lib/cuems/bin/cuems-editor-manual --port 9093
```

#### Daemon Mode (Systemd)
```bash
# Via systemd service
sudo systemctl start cuems-editor
sudo systemctl stop cuems-editor
sudo systemctl restart cuems-editor
sudo systemctl status cuems-editor

# Or manually with --daemon flag
/usr/lib/cuems/bin/cuems-ws-server --daemon
```

### B. During Development (Before Installation)

#### Method 1: Direct Python Execution
```bash
cd /home/ion/src/cuems/cuems-editor/src/cuemseditor

# Manual mode
python3 cli.py
python3 cli.py --port 9093

# Daemon mode
python3 cli.py --daemon

# Using the manual script
python3 run_manual.py
python3 run_manual.py --port 9093
```

#### Method 2: As Python Module
```bash
cd /home/ion/src/cuems/cuems-editor

# Manual mode
PYTHONPATH=src python3 -m cuemseditor.cli
PYTHONPATH=src python3 -m cuemseditor.cli --port 9093

# Daemon mode
PYTHONPATH=src python3 -m cuemseditor.cli --daemon
```

#### Method 3: Development Installation (Editable)
```bash
cd /home/ion/src/cuems/cuems-editor
/usr/lib/cuems/bin/pip3 install -e .

# Then use console scripts as normal:
/usr/lib/cuems/bin/cuems-ws-server
/usr/lib/cuems/bin/cuems-ws-server --daemon
/usr/lib/cuems/bin/cuems-editor-manual
```

---

## Command-Line Options

### cuems-ws-server / cuems-editor

```
usage: cuems-ws-server [-h] [--daemon] [--port PORT]

CUEMS Editor WebSocket Server

options:
  -h, --help   show this help message and exit
  --daemon     Run as daemon (for systemd service). Default: run in foreground
  --port PORT  WebSocket server port (manual mode only). Default: 9092
```

**Examples:**
```bash
# Manual mode on default port
cuems-ws-server

# Manual mode on custom port
cuems-ws-server --port 9093

# Daemon mode (for systemd)
cuems-ws-server --daemon
```

### cuems-editor-manual

```
usage: cuems-editor-manual [-h] [--port PORT]

Run CUEMS Editor in manual mode

options:
  -h, --help   show this help message and exit
  --port PORT  WebSocket server port. Default: 9092
```

**Examples:**
```bash
# Run on default port
cuems-editor-manual

# Run on custom port
cuems-editor-manual --port 9093
```

---

## Systemd Service Management

The service file is provided by `cuems-common` package.

### Basic Commands
```bash
# Start service
sudo systemctl start cuems-editor

# Stop service
sudo systemctl stop cuems-editor

# Restart service
sudo systemctl restart cuems-editor

# Check status
sudo systemctl status cuems-editor

# Enable at boot
sudo systemctl enable cuems-editor

# Disable at boot
sudo systemctl disable cuems-editor
```

### View Logs
```bash
# Follow logs (real-time)
sudo journalctl -u cuems-editor -f

# View recent logs
sudo journalctl -u cuems-editor -n 50

# View logs since boot
sudo journalctl -u cuems-editor -b

# View logs for specific time range
sudo journalctl -u cuems-editor --since "2025-12-11 10:00:00" --until "2025-12-11 12:00:00"
```

---

## Configuration

### Server Settings
Default settings (can be modified in `cli.py`):
- **Port**: 9092 (manual mode, customizable)
- **Library path**: `/opt/cuems_library`
- **Tmp path**: `/tmp/cuems`
- **Database**: `project-manager.db`
- **IPC socket**: `/tmp/editor.ipc`

### Project Mappings
Loaded from: `default_mappings.xml` (via ConfigManager)

---

## Troubleshooting

### Server Won't Start

1. **Check if port is in use:**
   ```bash
   sudo netstat -tlnp | grep 9092
   # or
   sudo lsof -i :9092
   ```

2. **Check permissions:**
   ```bash
   # Tmp directory
   ls -la /tmp/cuems
   
   # Library directory
   ls -la /opt/cuems_library
   ```

3. **Check logs:**
   ```bash
   sudo journalctl -u cuems-editor -n 100
   ```

### Import Errors (Development)

Make sure you're in the right directory:
```bash
cd /home/ion/src/cuems/cuems-editor/src/cuemseditor
python3 cli.py
```

Or use PYTHONPATH:
```bash
cd /home/ion/src/cuems/cuems-editor
PYTHONPATH=src python3 src/cuemseditor/cli.py
```

### Service Fails to Start

Check service status:
```bash
sudo systemctl status cuems-editor
```

Check if dependencies are running:
```bash
sudo systemctl status cuems-node
sudo systemctl status apache2
```

Verify console script exists:
```bash
ls -la /usr/lib/cuems/bin/cuems-ws-server
```

Test manually:
```bash
/usr/lib/cuems/bin/cuems-ws-server
# Should start in foreground - press Ctrl+C to stop
```

---

## Quick Reference

| Use Case | Command |
|----------|---------|
| **Development (foreground)** | `cd src/cuemseditor && python3 cli.py` |
| **Development (custom port)** | `cd src/cuemseditor && python3 cli.py --port 9093` |
| **Production (systemd)** | `sudo systemctl start cuems-editor` |
| **Manual (installed)** | `/usr/lib/cuems/bin/cuems-ws-server` |
| **Manual (installed, port)** | `/usr/lib/cuems/bin/cuems-ws-server --port 9093` |
| **Quick manual start** | `/usr/lib/cuems/bin/cuems-editor-manual` |
| **View logs** | `sudo journalctl -u cuems-editor -f` |
| **Stop service** | `sudo systemctl stop cuems-editor` |

---

## Development Workflow

1. **Make changes** to source files in `src/cuemseditor/`

2. **Test locally:**
   ```bash
   cd src/cuemseditor
   python3 cli.py --port 9093
   ```

3. **Test daemon mode:**
   ```bash
   cd src/cuemseditor
   python3 cli.py --daemon
   # Check logs in system journal
   ```

4. **Build package:**
   ```bash
   cd /home/ion/src/cuems/cuems-editor
   debuild -b -uc -us -nc
   ```

5. **Install and test:**
   ```bash
   sudo dpkg -i ../cuems-editor_*.deb
   sudo systemctl restart cuems-editor
   sudo systemctl status cuems-editor
   ```
