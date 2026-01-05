"""
ChatterCore message handling module

This module defines the message structure and handling logic for the ChatterCore system.
"""

import json
import uuid
import asyncio
import time
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional, Union, List, Callable, Awaitable, Type
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


class MessageContext:
    """
    Rich context for message handlers.
    
    Provides structured context with routing awareness and metadata support.
    """
    
    def __init__(
        self,
        connection_id: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        connection: Optional[Any] = None
    ):
        self.connection_id = connection_id
        self.user_id = user_id
        self.session_id = session_id
        self.connection = connection
        self.timestamp = time.time()
        
        # Routing information (populated for routed messages)
        self.sender: Optional[str] = None
        self.recipient: Optional[str] = None
        self.route_hops: List[str] = []
        
        # Custom metadata
        self.metadata: Dict[str, Any] = {}
    
    def add_hop(self, agent: str):
        """Track routing through system."""
        self.route_hops.append(agent)
    
    def is_routed(self) -> bool:
        """Check if this is a routed message."""
        return self.sender is not None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary."""
        return {
            'connection_id': self.connection_id,
            'user_id': self.user_id,
            'session_id': self.session_id,
            'timestamp': self.timestamp,
            'sender': self.sender,
            'recipient': self.recipient,
            'route_hops': self.route_hops,
            'metadata': self.metadata
        }


class RoutedMessage:
    """A message with explicit routing information."""
    
    def __init__(
        self,
        content: Union[str, Dict[str, Any]],
        sender: str,
        recipient: str,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.content = content
        self.sender = sender
        self.recipient = recipient
        self.metadata = metadata or {}
        self.timestamp = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            'content': self.content,
            'sender': self.sender,
            'recipient': self.recipient,
            'metadata': self.metadata,
            'timestamp': self.timestamp
        }


class Message(BaseModel):
    """
    Core message structure for ChatterCore communication.
    
    This class defines the standard message format used throughout
    the ChatterCore system for all client-server communication.
    
    Content Type Contract:
    - TEXT: content must be str
    - CUSTOM: content must be dict (auto-parsed from JSON)
    - SYSTEM/ERROR/STATUS: content is str
    - Other types: content can be str or dict
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
    def validate_content(cls, v, info):
        """Validate and normalize content based on message type."""
        message_type = info.data.get('type', MessageType.TEXT)
        
        # CUSTOM messages must be dicts - auto-parse JSON strings
        if message_type == MessageType.CUSTOM:
            if isinstance(v, str):
                try:
                    v = json.loads(v)
                except (json.JSONDecodeError, ValueError) as e:
                    raise ValueError(f"CUSTOM message content must be valid JSON dict, got invalid JSON string: {e}")
            if not isinstance(v, dict):
                raise ValueError(f"CUSTOM message content must be dict, got {type(v).__name__}")
            return v
        
        # TEXT messages must be strings
        if message_type == MessageType.TEXT:
            if not isinstance(v, str):
                raise ValueError(f"TEXT message content must be string, got {type(v).__name__}")
        
        # Allow empty content for system messages
        if isinstance(v, str) and len(v.strip()) == 0:
            return v
        
        return v
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v):
        """Ensure timestamp is in UTC."""
        if v.tzinfo is not None:
            v = v.replace(tzinfo=None)
        return v
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary format for transport."""
        data = self.model_dump()
        # For CUSTOM messages, ensure content is serialized for wire transport
        if self.type == MessageType.CUSTOM and isinstance(self.content, dict):
            data['content'] = json.dumps(self.content)
        return data
    
    def to_json(self) -> str:
        """Convert message to JSON string for transport."""
        # Use to_dict to ensure proper serialization
        return json.dumps(self.to_dict(), default=str)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Create message from dictionary with automatic content deserialization."""
        try:
            # Auto-parse CUSTOM message content if it's a JSON string
            if data.get('type') == MessageType.CUSTOM:
                content = data.get('content')
                if isinstance(content, str):
                    try:
                        data['content'] = json.loads(content)
                    except (json.JSONDecodeError, ValueError):
                        # Let the validator handle the error
                        pass
            return cls(**data)
        except Exception as e:
            raise ValidationException(f"Failed to create message from dict: {e}")
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        """Create message from JSON string with automatic content deserialization."""
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
    
    def deliver_response(self, message_id: str, response: Message) -> bool:
        """
        Deliver a response to a waiting message.
        
        This is called to manually deliver responses for testing or
        when responses arrive through non-standard channels.
        
        Args:
            message_id: The ID of the message being responded to
            response: The response message
            
        Returns:
            True if a waiting handler was found and notified, False otherwise
        """
        handled = False
        
        # Check for waiting response
        if message_id in self._pending_responses:
            future = self._pending_responses.pop(message_id)
            if not future.done():
                future.set_result(response)
                handled = True
            
            # Cancel timeout task
            if message_id in self._timeout_tasks:
                self._timeout_tasks[message_id].cancel()
                self._timeout_tasks.pop(message_id)
        
        # Check for callback
        if message_id in self._response_callbacks:
            callback = self._response_callbacks.pop(message_id)
            try:
                if asyncio.iscoroutinefunction(callback):
                    # Wrap async callback in error-handling task
                    async def safe_callback():
                        try:
                            await callback(response)
                        except Exception as e:
                            print(f"Error in response callback for {message_id}: {e}")
                    asyncio.create_task(safe_callback())
                else:
                    callback(response)
                handled = True
            except Exception as e:
                # Log callback error but don't raise
                print(f"Error in response callback for {message_id}: {e}")
            
            # Cancel timeout task
            if message_id in self._timeout_tasks:
                self._timeout_tasks[message_id].cancel()
                self._timeout_tasks.pop(message_id)
        
        return handled
    
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
        
        return self.deliver_response(message.reply_to, message)
    
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
    and custom message type handling with built-in request-response support.
    """
    
    def __init__(self):
        self._handlers: Dict[MessageType, List[Callable]] = {}
        self._middleware: List[Callable] = []
        self._filters: Dict[str, Callable] = {}
        self.callback_handler = CallbackHandler()
        self._logger = logging.getLogger(__name__)
        
        # Request-response tracking
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._request_senders: Dict[str, str] = {}  # request_id -> sender_id
    
    async def send_request(
        self,
        content: Dict[str, Any],
        recipient: str,
        sender_id: str,
        timeout: float = 30.0,
        send_callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        Send a request and wait for response.
        
        Automatically generates request_id and manages future.
        Returns response content or raises TimeoutError.
        
        Args:
            content: Request content (dict)
            recipient: Recipient ID or alias
            sender_id: Sender ID
            timeout: Response timeout in seconds
            send_callback: Async callback to actually send the message
            
        Returns:
            Response content dict
            
        Raises:
            TimeoutError: If no response within timeout
            MessageException: If send fails
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # Generate unique request ID
        request_id = f"{sender_id}_{uuid.uuid4().hex[:8]}"
        
        # Add request_id to content
        content['request_id'] = request_id
        
        # Create future
        future = asyncio.Future()
        self._pending_requests[request_id] = future
        self._request_senders[request_id] = sender_id
        
        try:
            # Send request using provided callback
            if send_callback:
                await send_callback(content, MessageType.CUSTOM, recipient)
            else:
                raise MessageException("No send_callback provided for send_request")
            
            logger.debug(f"Request {request_id} sent to {recipient}, waiting for response...")
            
            # Wait for response
            response = await asyncio.wait_for(future, timeout=timeout)
            logger.debug(f"Request {request_id} received response")
            return response
            
        except asyncio.TimeoutError:
            self._pending_requests.pop(request_id, None)
            self._request_senders.pop(request_id, None)
            raise TimeoutError(f"Request {request_id} timed out after {timeout}s")
        except Exception as e:
            self._pending_requests.pop(request_id, None)
            self._request_senders.pop(request_id, None)
            raise MessageException(f"Failed to send request: {e}")
    
    async def send_response(self, request_id: str, content: Dict[str, Any]) -> None:
        """
        Handle incoming response to a previous request.
        
        This should be called when receiving a response message.
        
        Args:
            request_id: Original request ID
            content: Response content
        """
        if request_id in self._pending_requests:
            future = self._pending_requests.pop(request_id)
            self._request_senders.pop(request_id, None)
            if not future.done():
                future.set_result(content)
    
    def is_pending_request(self, request_id: str) -> bool:
        """Check if a request_id is pending."""
        return request_id in self._pending_requests
    
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
    
    async def process_message(self, message: Message, context: Optional[Union[Dict[str, Any], MessageContext]] = None) -> bool:
        """
        Process a message through middleware, filters, callbacks, and handlers.
        
        Returns True if message was processed successfully, False otherwise.
        """
        try:
            # Check if message is expired
            if message.is_expired():
                return False
            
            # Convert dict context to MessageContext if needed
            if isinstance(context, dict):
                msg_context = MessageContext(
                    connection_id=context.get('connection_id'),
                    user_id=context.get('user_id'),
                    session_id=context.get('session_id'),
                    connection=context.get('connection')
                )
                # Copy any additional metadata
                for key, value in context.items():
                    if key not in ['connection_id', 'user_id', 'session_id', 'connection']:
                        msg_context.metadata[key] = value
            else:
                msg_context = context or MessageContext()
            
            # Check if this is a response to a pending request
            if message.type == MessageType.CUSTOM and isinstance(message.content, dict):
                request_id = message.content.get('request_id')
                if request_id and self.is_pending_request(request_id):
                    # This is a response - complete the future
                    await self.send_response(request_id, message.content)
                    return True
            
            # First, check if this is a response to a pending message (legacy callback system)
            if message.reply_to and await self.callback_handler.handle_response(message):
                # Message was handled by callback system
                return True
            
            # Apply middleware
            for middleware in self._middleware:
                message = await self._call_async_or_sync(middleware, message, msg_context)
                if message is None:
                    return False
            
            # Apply filters
            for filter_func in self._filters.values():
                if not await self._call_async_or_sync(filter_func, message, msg_context):
                    return False
            
            # Route to handlers
            handlers = self._handlers.get(message.type, [])
            if not handlers:
                # Try custom handler if no specific handler found
                handlers = self._handlers.get(MessageType.CUSTOM, [])
            
            for handler in handlers:
                await self._call_async_or_sync(handler, message, msg_context)
            
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
