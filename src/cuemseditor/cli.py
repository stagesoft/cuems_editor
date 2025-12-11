#!/usr/bin/env python3
"""
CLI entry point for cuems-editor WebSocket server

Supports two modes:
1. Manual/Development mode: Runs in foreground (default)
2. Daemon mode: Runs as system daemon (--daemon flag)
"""

import sys
import os
import uuid
import json
import argparse

# Add src directory to path for development (when not installed)
if __name__ == '__main__' and __package__ is None:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

from cuemsutils.log import Logger
from cuemsutils.tools.ConfigManager import ConfigManager, ProjectMappings
from cuemseditor.CuemsWsServer import CuemsWsServer
from cuemsutils.daemon import run_daemon


def get_settings():
    """Get server settings configuration"""
    settings_dict = {}
    settings_dict['session_uuid'] = str(uuid.uuid1())
    settings_dict['library_path'] = '/opt/cuems_library'
    settings_dict['tmp_path'] = '/tmp/cuems'
    settings_dict['database_name'] = 'project-manager.db'
    settings_dict['project_folder_name'] = 'projects'
    settings_dict['script_file_name'] = 'script.xml'
    settings_dict['script_schema_name'] = 'script'
    settings_dict['media_folder_name'] = 'media'
    settings_dict['trash_folder_name'] = 'trash'
    settings_dict['thumbnail_folder_name'] = 'thumbnails'
    settings_dict['waveform_folder_name'] = 'waveforms'
    settings_dict['thumbnail_extension'] = '.png'
    settings_dict['waveform_extension'] = '.dat'
    settings_dict['thumbnail_size'] = (240, 240)
    settings_dict['editor_ipc'] = '/tmp/editor.ipc'
    return settings_dict


def get_mappings():
    """Load project mappings configuration"""
    cf_manager = ConfigManager(load_all=False)
    settings_file = cf_manager.conf_path('default_mappings.xml')
    project_mappings = ProjectMappings(settings_file)
    return project_mappings.get_dict()


def ensure_directories(settings_dict):
    """Ensure required directories exist"""
    try:
        if not os.path.exists(settings_dict['tmp_path']):
            os.makedirs(settings_dict['tmp_path'], exist_ok=True)
            Logger.info(f"Created tmp upload folder: {settings_dict['tmp_path']}")
    except Exception as e:
        Logger.error(f"Error creating tmp folder: {type(e).__name__}: {e}")
        raise


def run_manual(port=9092):
    """Run server in manual/development mode (foreground)"""
    Logger.info("Starting CUEMS Editor in MANUAL mode (foreground)")
    
    # Get configuration
    settings_dict = get_settings()
    mappings_dict = get_mappings()
    
    Logger.info(f"Project mappings: {json.dumps(mappings_dict)}")
    
    # Ensure directories
    ensure_directories(settings_dict)
    
    # Create and start server
    Logger.info(f"Starting WebSocket server on port {port}")
    server = CuemsWsServer(settings_dict, mappings_dict)
    
    try:
        server.start(port)
    except KeyboardInterrupt:
        Logger.info("Received interrupt signal, stopping server...")
        # server.stop() if such method exists
    except Exception as e:
        Logger.error(f"Server error: {type(e).__name__}: {e}")
        raise


def run_daemon_mode():
    """Run server in daemon mode (for systemd)"""
    Logger.info("Starting CUEMS Editor in DAEMON mode")
    
    # Get configuration
    settings_dict = get_settings()
    mappings_dict = get_mappings()
    
    Logger.info(f"Project mappings: {json.dumps(mappings_dict)}")
    
    # Ensure directories
    ensure_directories(settings_dict)
    
    # Create server and run as daemon
    server = CuemsWsServer(settings_dict, mappings_dict)
    run_daemon(server, 'cuems_editor')


def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(
        description='CUEMS Editor WebSocket Server',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run in manual/development mode (foreground)
  %(prog)s
  %(prog)s --port 9092
  
  # Run as daemon (for systemd service)
  %(prog)s --daemon
        """
    )
    
    parser.add_argument(
        '--daemon',
        action='store_true',
        help='Run as daemon (for systemd service). Default: run in foreground'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=9092,
        help='WebSocket server port (manual mode only). Default: 9092'
    )
    
    args = parser.parse_args()
    
    if args.daemon:
        # Daemon mode - for systemd
        run_daemon_mode()
    else:
        # Manual mode - for development/testing
        run_manual(port=args.port)


if __name__ == '__main__':
    main()
