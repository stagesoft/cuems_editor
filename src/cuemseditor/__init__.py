"""
CUEMS Editor - Collaborative Universal Edit Management System

WebSocket-based server for collaborative editing and project management.
"""

__version__ = "0.1.0"
__author__ = "CUEMS Team"
__license__ = "GPL-3.0"

# Package exports
from cuemseditor.CuemsWsServer import CuemsWsServer
from cuemseditor.CuemsWsUser import CuemsWsUser
from cuemseditor.CuemsErrors import *

__all__ = [
    "CuemsWsServer",
    "CuemsWsUser",
]
