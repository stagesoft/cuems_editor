#!/usr/bin/env python3
"""
Quick script to run CUEMS Editor in development mode.

This is a convenience wrapper with startup messages.
For production, use cli.py directly or systemd service.
"""

import sys
import os

# Add src directory to path for development (when not installed)
if __name__ == '__main__' and __package__ is None:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

from cuemseditor.cli import run_manual


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Run CUEMS Editor in development mode')
    parser.add_argument(
        '--port',
        type=int,
        default=9092,
        help='WebSocket server port. Default: 9092'
    )
    
    args = parser.parse_args()
    
    print(f"Starting CUEMS Editor on port {args.port}")
    print("Press Ctrl+C to stop")
    print("-" * 60)
    
    run_manual(port=args.port)


if __name__ == '__main__':
    main()
