"""
ChatterCore connection management module

This module manages WebSocket connections, client sessions, and connection lifecycle.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Set, Optional, List, Any
from dataclasses import dataclass, field
import uuid

from .exceptions import ConnectionException
from .event_manager import EventManager, EventType
from .message_handler import Message, MessageType


@dataclass
class ConnectionInfo:
    """Information about a client connection."""
    id: str
    websocket: Any  # WebSocket connection object
    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    client_info: Dict[str, Any] = field(default_factory=dict)
    channels: Set[str] = field(default_factory=set)
    authenticated: bool = False
    user_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def update_last_seen(self):
        """Update the last seen timestamp."""
        self.last_seen = datetime.now(timezone.utc)
    
    def is_stale(self, timeout_seconds: int = 300) -> bool:
        """Check if connection is stale based on last seen timestamp."""
        return (datetime.now(timezone.utc) - self.last_seen).total_seconds() > timeout_seconds


class ConnectionManager:
    """
    Manages WebSocket connections and client sessions.
    
    This class handles connection lifecycle, client tracking, channel management,
    and message broadcasting for the ChatterCore system.
    """
    
    def __init__(self, event_manager: Optional[EventManager] = None):
        self._connections: Dict[str, ConnectionInfo] = {}
        self._user_connections: Dict[str, Set[str]] = {}  # user_id -> connection_ids
        self._channels: Dict[str, Set[str]] = {}  # channel -> connection_ids
        self._event_manager = event_manager
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_interval = 60  # seconds
        self._connection_timeout = 300  # seconds
        self._max_connections = 1000
        self._stats = {
            'total_connections': 0,
            'active_connections': 0,
            'messages_sent': 0,
            'messages_failed': 0,
            'channels_created': 0,
        }
    
    async def add_connection(self, websocket: Any, client_info: Optional[Dict[str, Any]] = None) -> str:
        """
        Add a new WebSocket connection.
        
        Returns the connection ID.
        """
        if len(self._connections) >= self._max_connections:
            raise ConnectionException("Maximum number of connections reached")
        
        connection_id = str(uuid.uuid4())
        connection_info = ConnectionInfo(
            id=connection_id,
            websocket=websocket,
            client_info=client_info or {}
        )
        
        self._connections[connection_id] = connection_info
        self._stats['total_connections'] += 1
        self._stats['active_connections'] += 1
        
        # Emit connection event
        if self._event_manager:
            await self._event_manager.publish(
                EventType.CLIENT_CONNECTED,
                data={
                    'connection_id': connection_id,
                    'client_info': client_info or {},
                    'total_connections': len(self._connections)
                }
            )
        
        return connection_id
    
    async def remove_connection(self, connection_id: str) -> bool:
        """
        Remove a connection and clean up related data.
        
        Returns True if connection was removed, False if not found.
        """
        if connection_id not in self._connections:
            return False
        
        connection_info = self._connections[connection_id]
        
        # Remove from user connections
        if connection_info.user_id:
            if connection_info.user_id in self._user_connections:
                self._user_connections[connection_info.user_id].discard(connection_id)
                if not self._user_connections[connection_info.user_id]:
                    del self._user_connections[connection_info.user_id]
        
        # Remove from channels
        for channel in connection_info.channels.copy():
            await self.leave_channel(connection_id, channel)
        
        # Remove connection
        del self._connections[connection_id]
        self._stats['active_connections'] -= 1
        
        # Emit disconnection event
        if self._event_manager:
            await self._event_manager.publish(
                EventType.CLIENT_DISCONNECTED,
                data={
                    'connection_id': connection_id,
                    'user_id': connection_info.user_id,
                    'total_connections': len(self._connections)
                }
            )
        
        return True
    
    def get_connection(self, connection_id: str) -> Optional[ConnectionInfo]:
        """Get connection information by ID."""
        return self._connections.get(connection_id)
    
    def get_connections_for_user(self, user_id: str) -> List[ConnectionInfo]:
        """Get all connections for a specific user."""
        connection_ids = self._user_connections.get(user_id, set())
        return [self._connections[conn_id] for conn_id in connection_ids 
                if conn_id in self._connections]
    
    def get_active_connections(self) -> List[ConnectionInfo]:
        """Get all active connections."""
        return list(self._connections.values())
    
    def get_connection_count(self) -> int:
        """Get the current number of active connections."""
        return len(self._connections)
    
    async def authenticate_connection(self, connection_id: str, user_id: str, 
                                    metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Authenticate a connection with a user ID.
        
        Returns True if successful, False if connection not found.
        """
        if connection_id not in self._connections:
            return False
        
        connection_info = self._connections[connection_id]
        connection_info.authenticated = True
        connection_info.user_id = user_id
        if metadata:
            connection_info.metadata.update(metadata)
        
        # Track user connections
        if user_id not in self._user_connections:
            self._user_connections[user_id] = set()
        self._user_connections[user_id].add(connection_id)
        
        return True
    
    async def join_channel(self, connection_id: str, channel: str) -> bool:
        """
        Add a connection to a channel.
        
        Returns True if successful, False if connection not found.
        """
        if connection_id not in self._connections:
            return False
        
        connection_info = self._connections[connection_id]
        connection_info.channels.add(channel)
        
        # Track channel membership
        if channel not in self._channels:
            self._channels[channel] = set()
            self._stats['channels_created'] += 1
        self._channels[channel].add(connection_id)
        
        # Emit channel join event
        if self._event_manager:
            await self._event_manager.publish(
                EventType.CHANNEL_JOINED,
                data={
                    'connection_id': connection_id,
                    'channel': channel,
                    'user_id': connection_info.user_id
                }
            )
        
        return True
    
    async def leave_channel(self, connection_id: str, channel: str) -> bool:
        """
        Remove a connection from a channel.
        
        Returns True if successful, False if connection not found.
        """
        if connection_id not in self._connections:
            return False
        
        connection_info = self._connections[connection_id]
        connection_info.channels.discard(channel)
        
        # Remove from channel tracking
        if channel in self._channels:
            self._channels[channel].discard(connection_id)
            if not self._channels[channel]:
                del self._channels[channel]
        
        # Emit channel leave event
        if self._event_manager:
            await self._event_manager.publish(
                EventType.CHANNEL_LEFT,
                data={
                    'connection_id': connection_id,
                    'channel': channel,
                    'user_id': connection_info.user_id
                }
            )
        
        return True
    
    async def send_to_connection(self, connection_id: str, message: Message) -> bool:
        """
        Send a message to a specific connection.
        
        Returns True if successful, False if failed or connection not found.
        """
        if connection_id not in self._connections:
            return False
        
        connection_info = self._connections[connection_id]
        connection_info.update_last_seen()
        
        try:
            await connection_info.websocket.send(message.to_json())
            self._stats['messages_sent'] += 1
            
            # Emit message sent event
            if self._event_manager:
                await self._event_manager.publish(
                    EventType.MESSAGE_SENT,
                    data={
                        'connection_id': connection_id,
                        'message_id': message.id,
                        'message_type': message.type
                    }
                )
            
            return True
            
        except Exception as e:
            self._stats['messages_failed'] += 1
            
            # Emit message failed event
            if self._event_manager:
                await self._event_manager.publish(
                    EventType.MESSAGE_FAILED,
                    data={
                        'connection_id': connection_id,
                        'message_id': message.id,
                        'error': str(e)
                    }
                )
            
            return False
    
    async def send_to_user(self, user_id: str, message: Message) -> int:
        """
        Send a message to all connections of a specific user.
        
        Returns the number of connections the message was sent to.
        """
        connections = self.get_connections_for_user(user_id)
        sent_count = 0
        
        for connection in connections:
            if await self.send_to_connection(connection.id, message):
                sent_count += 1
        
        return sent_count
    
    async def send_to_channel(self, channel: str, message: Message, 
                            exclude_connection: Optional[str] = None) -> int:
        """
        Send a message to all connections in a channel.
        
        Returns the number of connections the message was sent to.
        """
        if channel not in self._channels:
            return 0
        
        connection_ids = self._channels[channel].copy()
        if exclude_connection:
            connection_ids.discard(exclude_connection)
        
        sent_count = 0
        for connection_id in connection_ids:
            if await self.send_to_connection(connection_id, message):
                sent_count += 1
        
        return sent_count
    
    async def broadcast(self, message: Message, exclude_connection: Optional[str] = None) -> int:
        """
        Broadcast a message to all active connections.
        
        Returns the number of connections the message was sent to.
        """
        connection_ids = list(self._connections.keys())
        if exclude_connection:
            connection_ids = [cid for cid in connection_ids if cid != exclude_connection]
        
        sent_count = 0
        for connection_id in connection_ids:
            if await self.send_to_connection(connection_id, message):
                sent_count += 1
        
        return sent_count
    
    def get_channel_members(self, channel: str) -> List[ConnectionInfo]:
        """Get all connection info for members of a channel."""
        if channel not in self._channels:
            return []
        
        return [self._connections[conn_id] for conn_id in self._channels[channel]
                if conn_id in self._connections]
    
    def get_channels(self) -> List[str]:
        """Get list of all active channels."""
        return list(self._channels.keys())
    
    async def start_cleanup_task(self) -> None:
        """Start the background cleanup task for stale connections."""
        if self._cleanup_task is not None:
            return
        
        self._cleanup_task = asyncio.create_task(self._cleanup_stale_connections())
    
    async def stop_cleanup_task(self) -> None:
        """Stop the background cleanup task."""
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
    
    async def _cleanup_stale_connections(self) -> None:
        """Background task to clean up stale connections."""
        while True:
            try:
                await asyncio.sleep(self._cleanup_interval)
                
                stale_connections = []
                for connection_id, connection_info in self._connections.items():
                    if connection_info.is_stale(self._connection_timeout):
                        stale_connections.append(connection_id)
                
                for connection_id in stale_connections:
                    await self.remove_connection(connection_id)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._event_manager:
                    await self._event_manager.publish(
                        EventType.SERVER_ERROR,
                        data={'error': f'Connection cleanup error: {e}'}
                    )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection manager statistics."""
        stats = self._stats.copy()
        stats.update({
            'active_connections': len(self._connections),
            'active_channels': len(self._channels),
            'authenticated_users': len(self._user_connections)
        })
        return stats
