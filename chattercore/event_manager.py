"""
ChatterCore event management module

This module provides event-driven architecture support for the ChatterCore system.
"""

import asyncio
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Callable, Any, Optional, Union
from dataclasses import dataclass, field

from .exceptions import ChatterCoreException


class EventType(str, Enum):
    """Enumeration of supported event types."""
    # Connection events
    CLIENT_CONNECTED = "client_connected"
    CLIENT_DISCONNECTED = "client_disconnected"
    CONNECTION_LOST = "connection_lost"
    CONNECTION_RESTORED = "connection_restored"
    
    # Message events
    MESSAGE_RECEIVED = "message_received"
    MESSAGE_SENT = "message_sent"
    MESSAGE_FAILED = "message_failed"
    MESSAGE_PROCESSED = "message_processed"
    
    # Server events
    SERVER_STARTED = "server_started"
    SERVER_STOPPED = "server_stopped"
    SERVER_ERROR = "server_error"
    
    # Client events
    CLIENT_JOINED = "client_joined"
    CLIENT_LEFT = "client_left"
    CLIENT_ERROR = "client_error"
    
    # Channel events
    CHANNEL_CREATED = "channel_created"
    CHANNEL_DELETED = "channel_deleted"
    CHANNEL_JOINED = "channel_joined"
    CHANNEL_LEFT = "channel_left"
    
    # Custom events
    CUSTOM = "custom"


@dataclass
class Event:
    """
    Event object containing event data and metadata.
    """
    type: EventType
    data: Dict[str, Any] = field(default_factory=dict)
    source: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    event_id: Optional[str] = None
    correlation_id: Optional[str] = None
    
    def __post_init__(self):
        if self.event_id is None:
            import uuid
            self.event_id = str(uuid.uuid4())


class EventManager:
    """
    Central event management system for ChatterCore.
    
    This class provides event subscription, publishing, and management
    capabilities for the ChatterCore system.
    """
    
    def __init__(self):
        self._listeners: Dict[EventType, List[Callable]] = {}
        self._once_listeners: Dict[EventType, List[Callable]] = {}
        self._middleware: List[Callable] = []
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
        self._stats = {
            'events_published': 0,
            'events_processed': 0,
            'events_failed': 0,
            'listeners_count': 0
        }
    
    def subscribe(self, event_type: EventType, listener: Callable) -> None:
        """Subscribe to an event type with a listener function."""
        if event_type not in self._listeners:
            self._listeners[event_type] = []
        self._listeners[event_type].append(listener)
        self._stats['listeners_count'] += 1
    
    def unsubscribe(self, event_type: EventType, listener: Callable) -> None:
        """Unsubscribe a listener from an event type."""
        if event_type in self._listeners:
            try:
                self._listeners[event_type].remove(listener)
                self._stats['listeners_count'] -= 1
            except ValueError:
                pass
    
    def subscribe_once(self, event_type: EventType, listener: Callable) -> None:
        """Subscribe to an event type with a one-time listener."""
        if event_type not in self._once_listeners:
            self._once_listeners[event_type] = []
        self._once_listeners[event_type].append(listener)
    
    def add_middleware(self, middleware: Callable) -> None:
        """Add middleware to process all events."""
        self._middleware.append(middleware)
    
    async def publish(self, event_type: EventType, data: Optional[Dict[str, Any]] = None, 
                     source: Optional[str] = None, correlation_id: Optional[str] = None) -> None:
        """Publish an event to all subscribers."""
        event = Event(
            type=event_type,
            data=data or {},
            source=source,
            correlation_id=correlation_id
        )
        
        await self._event_queue.put(event)
        self._stats['events_published'] += 1
    
    async def emit(self, event_type: EventType, **kwargs) -> None:
        """Emit an event with keyword arguments as data."""
        await self.publish(event_type, data=kwargs)
    
    async def start(self) -> None:
        """Start the event processor."""
        if self._running:
            return
        
        self._running = True
        self._processor_task = asyncio.create_task(self._process_events())
    
    async def stop(self) -> None:
        """Stop the event processor."""
        if not self._running:
            return
        
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
    
    async def _process_events(self) -> None:
        """Process events from the queue."""
        while self._running:
            try:
                # Wait for events with timeout to allow for graceful shutdown
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
                await self._handle_event(event)
                self._stats['events_processed'] += 1
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self._stats['events_failed'] += 1
                await self._handle_error(f"Error processing event: {e}")
    
    async def _handle_event(self, event: Event) -> None:
        """Handle a single event by calling all listeners."""
        try:
            # Apply middleware
            for middleware in self._middleware:
                event = await self._call_async_or_sync(middleware, event)
                if event is None:
                    return
            
            # Call regular listeners
            listeners = self._listeners.get(event.type, [])
            for listener in listeners:
                try:
                    await self._call_async_or_sync(listener, event)
                except Exception as e:
                    await self._handle_error(f"Error in event listener: {e}")
            
            # Call one-time listeners
            once_listeners = self._once_listeners.get(event.type, [])
            if once_listeners:
                for listener in once_listeners:
                    try:
                        await self._call_async_or_sync(listener, event)
                    except Exception as e:
                        await self._handle_error(f"Error in one-time event listener: {e}")
                # Clear one-time listeners after calling them
                del self._once_listeners[event.type]
        
        except Exception as e:
            await self._handle_error(f"Error handling event {event.type}: {e}")
    
    async def _call_async_or_sync(self, func: Callable, *args, **kwargs) -> Any:
        """Call function whether it's async or sync."""
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            return func(*args, **kwargs)
    
    async def _handle_error(self, error_message: str) -> None:
        """Handle internal errors."""
        # Emit error event if we have listeners for it
        if EventType.SERVER_ERROR in self._listeners:
            error_event = Event(
                type=EventType.SERVER_ERROR,
                data={'error': error_message},
                source='EventManager'
            )
            # Process error event directly to avoid recursion
            listeners = self._listeners.get(EventType.SERVER_ERROR, [])
            for listener in listeners:
                try:
                    await self._call_async_or_sync(listener, error_event)
                except Exception:
                    pass  # Avoid infinite recursion
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event manager statistics."""
        return self._stats.copy()
    
    def get_listener_count(self, event_type: Optional[EventType] = None) -> int:
        """Get the number of listeners for an event type or total."""
        if event_type is None:
            return sum(len(listeners) for listeners in self._listeners.values())
        return len(self._listeners.get(event_type, []))
    
    def clear_listeners(self, event_type: Optional[EventType] = None) -> None:
        """Clear listeners for a specific event type or all listeners."""
        if event_type is None:
            self._listeners.clear()
            self._once_listeners.clear()
            self._stats['listeners_count'] = 0
        else:
            if event_type in self._listeners:
                count = len(self._listeners[event_type])
                del self._listeners[event_type]
                self._stats['listeners_count'] -= count
            if event_type in self._once_listeners:
                del self._once_listeners[event_type]
