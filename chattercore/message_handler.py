"""
ChatterCore message handling module

This module defines the message structure and handling logic for the ChatterCore system.
"""

import json
import uuid
import asyncio
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional, Union, List, Callable, Awaitable
from pydantic import BaseModel, Field, field_validator

from .exceptions import MessageException, ValidationException


class MessageType(str, Enum):
    """Enumeration of supported message types."""
    TEXT = "text"
    SYSTEM = "system"
    BROADCAST = "broadcast"
    DIRECT = "direct"
    STATUS = "status"
    ERROR = "error"
    HEARTBEAT = "heartbeat"
    AUTH = "auth"
    JOIN = "join"
    LEAVE = "leave"
    CUSTOM = "custom"


class MessagePriority(str, Enum):
    """Message priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class Message(BaseModel):
    """
    Core message structure for ChatterCore communication.
    
    This class defines the standard message format used throughout
    the ChatterCore system for all client-server communication.
    """
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: MessageType = MessageType.TEXT
    content: Union[str, Dict[str, Any]] = ""
    sender_id: Optional[str] = None
    recipient_id: Optional[str] = None
    channel: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    priority: MessagePriority = MessagePriority.NORMAL
    metadata: Dict[str, Any] = Field(default_factory=dict)
    expires_at: Optional[datetime] = None
    reply_to: Optional[str] = None
    
    @field_validator('content')
    @classmethod
    def validate_content(cls, v):
        """Ensure content is not empty for non-system messages."""
        if isinstance(v, str) and len(v.strip()) == 0:
            return v  # Allow empty content for system messages
        return v
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v):
        """Ensure timestamp is in UTC."""
        if v.tzinfo is not None:
            v = v.replace(tzinfo=None)
        return v
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary format."""
        return self.model_dump()
    
    def to_json(self) -> str:
        """Convert message to JSON string."""
        return self.model_dump_json()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Create message from dictionary."""
        try:
            return cls(**data)
        except Exception as e:
            raise ValidationException(f"Failed to create message from dict: {e}")
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        """Create message from JSON string."""
        try:
            data = json.loads(json_str)
            return cls.from_dict(data)
        except json.JSONDecodeError as e:
            raise ValidationException(f"Invalid JSON format: {e}")
        except Exception as e:
            raise ValidationException(f"Failed to create message from JSON: {e}")
    
    def is_expired(self) -> bool:
        """Check if message has expired."""
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at


class CallbackHandler:
    """
    Handles callback-based message responses.
    
    Supports both waiting for responses and registering callback functions
    that are called when responses are received.
    """
    
    def __init__(self):
        # For waiting responses: message_id -> Future
        self._pending_responses: Dict[str, asyncio.Future] = {}
        # For callback responses: message_id -> callback function
        self._response_callbacks: Dict[str, Callable] = {}
        # Timeout tasks for cleanup
        self._timeout_tasks: Dict[str, asyncio.Task] = {}
        
    async def wait_for_response(self, message_id: str, timeout: float = 10.0) -> Optional[Message]:
        """
        Wait for a response to a specific message.
        
        Args:
            message_id: The ID of the message to wait for a response to
            timeout: Maximum time to wait in seconds
            
        Returns:
            The response message or None if timeout
            
        Raises:
            TimeoutError: If no response is received within timeout
        """
        if message_id in self._pending_responses:
            raise MessageException(f"Already waiting for response to message {message_id}")
        
        # Create future for this response
        future = asyncio.Future()
        self._pending_responses[message_id] = future
        
        # Set up timeout task
        async def timeout_cleanup():
            await asyncio.sleep(timeout)
            if message_id in self._pending_responses:
                future_to_cancel = self._pending_responses.pop(message_id)
                if not future_to_cancel.done():
                    future_to_cancel.set_exception(asyncio.TimeoutError(f"Response timeout for message {message_id}"))
        
        timeout_task = asyncio.create_task(timeout_cleanup())
        self._timeout_tasks[message_id] = timeout_task
        
        try:
            response = await future
            return response
        finally:
            # Cleanup
            self._pending_responses.pop(message_id, None)
            timeout_task.cancel()
            self._timeout_tasks.pop(message_id, None)
    
    def register_callback(self, message_id: str, callback: Callable[[Message], None], timeout: float = 30.0) -> None:
        """
        Register a callback to be called when a response is received.
        
        Args:
            message_id: The ID of the message to wait for a response to
            callback: Function to call when response is received
            timeout: Maximum time to wait before cleanup
        """
        if message_id in self._response_callbacks:
            raise MessageException(f"Callback already registered for message {message_id}")
        
        self._response_callbacks[message_id] = callback
        
        # Set up timeout cleanup
        async def timeout_cleanup():
            await asyncio.sleep(timeout)
            if message_id in self._response_callbacks:
                self._response_callbacks.pop(message_id)
                self._timeout_tasks.pop(message_id, None)
        
        timeout_task = asyncio.create_task(timeout_cleanup())
        self._timeout_tasks[message_id] = timeout_task
    
    async def handle_response(self, message: Message) -> bool:
        """
        Handle an incoming response message.
        
        Args:
            message: The response message
            
        Returns:
            True if the response was handled, False otherwise
        """
        if not message.reply_to:
            return False
        
        reply_to = message.reply_to
        handled = False
        
        # Check for waiting response
        if reply_to in self._pending_responses:
            future = self._pending_responses.pop(reply_to)
            if not future.done():
                future.set_result(message)
                handled = True
            
            # Cancel timeout task
            if reply_to in self._timeout_tasks:
                self._timeout_tasks[reply_to].cancel()
                self._timeout_tasks.pop(reply_to)
        
        # Check for callback
        if reply_to in self._response_callbacks:
            callback = self._response_callbacks.pop(reply_to)
            try:
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(message))
                else:
                    callback(message)
                handled = True
            except Exception as e:
                # Log callback error but don't raise
                print(f"Error in response callback for {reply_to}: {e}")
            
            # Cancel timeout task
            if reply_to in self._timeout_tasks:
                self._timeout_tasks[reply_to].cancel()
                self._timeout_tasks.pop(reply_to)
        
        return handled
    
    def cancel_all(self):
        """Cancel all pending responses and callbacks."""
        # Cancel all futures
        for future in self._pending_responses.values():
            if not future.done():
                future.cancel()
        self._pending_responses.clear()
        
        # Clear callbacks
        self._response_callbacks.clear()
        
        # Cancel all timeout tasks
        for task in self._timeout_tasks.values():
            task.cancel()
        self._timeout_tasks.clear()
    
    def get_stats(self) -> Dict[str, int]:
        """Get callback handler statistics."""
        return {
            'pending_responses': len(self._pending_responses),
            'active_callbacks': len(self._response_callbacks),
            'timeout_tasks': len(self._timeout_tasks)
        }


class MessageHandler:
    """
    Central message handling and routing system.
    
    This class manages message processing, validation, routing,
    and custom message type handling.
    """
    
    def __init__(self):
        self._handlers: Dict[MessageType, List[Callable]] = {}
        self._middleware: List[Callable] = []
        self._filters: Dict[str, Callable] = {}
        self.callback_handler = CallbackHandler()
    
    async def wait_for_response(self, message_id: str, timeout: float = 30.0) -> Optional[Message]:
        """Wait for a response to a specific message ID."""
        return await self.callback_handler.wait_for_response(message_id, timeout)
    
    def register_callback(self, message_id: str, callback: Callable, timeout: float = 30.0) -> None:
        """Register a callback function for a specific message ID."""
        self.callback_handler.register_callback(message_id, callback, timeout)
    
    def register_handler(self, message_type: MessageType, handler: Callable) -> None:
        """Register a handler for a specific message type."""
        if message_type not in self._handlers:
            self._handlers[message_type] = []
        self._handlers[message_type].append(handler)
    
    def unregister_handler(self, message_type: MessageType, handler: Callable) -> None:
        """Remove a handler for a specific message type."""
        if message_type in self._handlers:
            try:
                self._handlers[message_type].remove(handler)
            except ValueError:
                pass
    
    def add_middleware(self, middleware: Callable) -> None:
        """Add middleware function to process all messages."""
        self._middleware.append(middleware)
    
    def add_filter(self, name: str, filter_func: Callable) -> None:
        """Add a named filter function."""
        self._filters[name] = filter_func
    
    async def process_message(self, message: Message, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Process a message through middleware, filters, callbacks, and handlers.
        
        Returns True if message was processed successfully, False otherwise.
        """
        try:
            # Check if message is expired
            if message.is_expired():
                return False
            
            # First, check if this is a response to a pending message
            if message.reply_to and await self.callback_handler.handle_response(message):
                # Message was handled by callback system
                return True
            
            # Apply middleware
            for middleware in self._middleware:
                message = await self._call_async_or_sync(middleware, message, context)
                if message is None:
                    return False
            
            # Apply filters
            for filter_func in self._filters.values():
                if not await self._call_async_or_sync(filter_func, message, context):
                    return False
            
            # Route to handlers
            handlers = self._handlers.get(message.type, [])
            if not handlers:
                # Try custom handler if no specific handler found
                handlers = self._handlers.get(MessageType.CUSTOM, [])
            
            for handler in handlers:
                await self._call_async_or_sync(handler, message, context)
            
            return True
            
        except Exception as e:
            raise MessageException(f"Failed to process message {message.id}: {e}")
    
    async def _call_async_or_sync(self, func, *args, **kwargs):
        """Call function whether it's async or sync."""
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            return func(*args, **kwargs)
    
    def create_message(
        self,
        content: Union[str, Dict[str, Any]],
        message_type: MessageType = MessageType.TEXT,
        sender_id: Optional[str] = None,
        recipient_id: Optional[str] = None,
        channel: Optional[str] = None,
        priority: MessagePriority = MessagePriority.NORMAL,
        metadata: Optional[Dict[str, Any]] = None,
        reply_to: Optional[str] = None
    ) -> Message:
        """Create a new message with the specified parameters."""
        return Message(
            type=message_type,
            content=content,
            sender_id=sender_id,
            recipient_id=recipient_id,
            channel=channel,
            priority=priority,
            metadata=metadata or {},
            reply_to=reply_to
        )
    
    def create_system_message(self, content: str, metadata: Optional[Dict[str, Any]] = None, reply_to: Optional[str] = None) -> Message:
        """Create a system message."""
        return self.create_message(
            content=content,
            message_type=MessageType.SYSTEM,
            metadata=metadata,
            reply_to=reply_to
        )
    
    def create_error_message(self, error: str, details: Optional[Dict[str, Any]] = None, reply_to: Optional[str] = None) -> Message:
        """Create an error message."""
        metadata = {"error_details": details} if details else {}
        return self.create_message(
            content=error,
            message_type=MessageType.ERROR,
            priority=MessagePriority.HIGH,
            metadata=metadata,
            reply_to=reply_to
        )
