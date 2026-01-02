"""
ChatterCore - A self-contained, general-purpose real-time communication module

This package provides a complete solution for real-time messaging and communication
between clients and servers using WebSockets and asyncio.
"""

from .server import ChatterServer
from .client import ChatterClient
from .message_handler import MessageHandler, Message, MessageType
from .event_manager import EventManager, EventType
from .connection_manager import ConnectionManager
from .exceptions import (
    ChatterCoreException,
    ConnectionException,
    MessageException,
    ServerException,
    ClientException,
)

__version__ = "1.0.0"
__author__ = "ChatterCore Developer"
__email__ = "developer@chattercore.com"

__all__ = [
    "ChatterServer",
    "ChatterClient", 
    "MessageHandler",
    "Message",
    "MessageType",
    "EventManager",
    "EventType",
    "ConnectionManager",
    "ChatterCoreException",
    "ConnectionException",
    "MessageException",
    "ServerException",
    "ClientException",
]
