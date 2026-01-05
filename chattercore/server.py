"""
ChatterCore server module

This module provides the main server implementation for the ChatterCore system.
"""

import asyncio
import json
import logging
import uuid
import fnmatch
from typing import Dict, Any, Optional, Callable, Union, Type
import websockets
from websockets import ConnectionClosed
from pydantic import BaseModel, ValidationError

from .connection_manager import ConnectionManager
from .event_manager import EventManager, EventType
from .message_handler import MessageHandler, Message, MessageType
from .exceptions import ServerException, MessageException


class ChatterServer:
    """
    Main WebSocket server for the ChatterCore system.
    
    This class provides a complete WebSocket server with connection management,
    message handling, event processing, and extensible architecture.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8765,
        max_connections: int = 1000,
        message_size_limit: int = 1024 * 1024,  # 1MB
        ping_interval: int = 20,
        ping_timeout: int = 20,
        close_timeout: int = 10,
        auto_route_messages: bool = True
    ):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self.message_size_limit = message_size_limit
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.close_timeout = close_timeout
        self.auto_route_messages = auto_route_messages
        
        # Core components
        self.event_manager = EventManager()
        self.connection_manager = ConnectionManager(self.event_manager)
        self.message_handler = MessageHandler()
        
        # Server state
        self._server: Optional[Any] = None  # WebSocket server instance
        self._running = False
        self._logger = logging.getLogger(__name__)
        
        # Authentication and middleware
        self._auth_handler: Optional[Callable] = None
        self._connection_middleware: list = []
        self._message_middleware: list = []
        
        # Phase 3: Schema validation
        self._schemas: Dict[str, Type[BaseModel]] = {}
        self._strict_validation = False
        
        # Phase 3: Logging and tracing
        self._tracing_enabled = False
        
        # Phase 3: Metrics and monitoring
        self._metrics_enabled = False
        self._message_metrics = {
            'total_received': 0,
            'total_sent': 0,
            'by_type': {},
            'errors': 0,
            'processing_times': []
        }
        self._performance_monitoring_enabled = False
        self._error_tracking_enabled = False
        self._error_log = []
        
        # Setup default message handlers
        self._setup_default_handlers()
    
    def _setup_default_handlers(self):
        """Setup default message handlers."""
        # Handle system messages
        self.message_handler.register_handler(
            MessageType.SYSTEM,
            self._handle_system_message
        )
        
        # Handle heartbeat messages
        self.message_handler.register_handler(
            MessageType.HEARTBEAT,
            self._handle_heartbeat
        )
        
        # Handle join/leave messages
        self.message_handler.register_handler(
            MessageType.JOIN,
            self._handle_join_message
        )
        
        self.message_handler.register_handler(
            MessageType.LEAVE,
            self._handle_leave_message
        )
        
        # Handle authentication messages
        self.message_handler.register_handler(
            MessageType.AUTH,
            self._handle_auth_message
        )
    
    async def start(self) -> None:
        """Start the ChatterServer."""
        if self._running:
            raise ServerException("Server is already running")
        
        try:
            # Start core components
            await self.event_manager.start()
            await self.connection_manager.start_cleanup_task()
            
            # Start WebSocket server
            self._server = await websockets.serve(
                self._handle_client,
                self.host,
                self.port,
                max_size=self.message_size_limit,
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
                close_timeout=self.close_timeout,
                max_queue=None  # No limit on message queue
            )
            
            self._running = True
            self._logger.info(f"ChatterServer started on {self.host}:{self.port}")
            
            # Emit server started event
            await self.event_manager.publish(
                EventType.SERVER_STARTED,
                data={
                    'host': self.host,
                    'port': self.port,
                    'max_connections': self.max_connections
                }
            )
            
        except Exception as e:
            self._logger.error(f"Failed to start server: {e}")
            raise ServerException(f"Failed to start server: {e}")
    
    async def stop(self) -> None:
        """Stop the ChatterServer."""
        if not self._running:
            return
        
        try:
            self._running = False
            
            # Stop WebSocket server
            if self._server:
                self._server.close()
                await self._server.wait_closed()
            
            # Stop core components
            await self.connection_manager.stop_cleanup_task()
            await self.event_manager.stop()
            
            self._logger.info("ChatterServer stopped")
            
            # Emit server stopped event
            await self.event_manager.publish(
                EventType.SERVER_STOPPED,
                data={'graceful_shutdown': True}
            )
            
        except Exception as e:
            self._logger.error(f"Error stopping server: {e}")
            raise ServerException(f"Error stopping server: {e}")
    
    async def _handle_client(self, websocket, path: str = ""):
        """Handle a new client connection."""
        connection_id = None
        
        try:
            # Check connection limit
            if self.connection_manager.get_connection_count() >= self.max_connections:
                await websocket.close(code=1013, reason="Server at capacity")
                return
            
            # Add connection
            connection_id = await self.connection_manager.add_connection(
                websocket,
                client_info={'path': path, 'remote_address': websocket.remote_address}
            )
            
            self._logger.info(f"New client connected: {connection_id}")
            
            # Apply connection middleware
            for middleware in self._connection_middleware:
                if not await self._call_async_or_sync(middleware, connection_id, websocket):
                    await websocket.close(code=1002, reason="Connection rejected by middleware")
                    return
            
            # Handle messages
            async for raw_message in websocket:
                try:
                    await self._process_client_message(connection_id, raw_message)
                except MessageException as e:
                    self._logger.warning(f"Message error from {connection_id}: {e}")
                    error_msg = self.message_handler.create_error_message(str(e))
                    await self.connection_manager.send_to_connection(connection_id, error_msg)
                except Exception as e:
                    self._logger.error(f"Unexpected error processing message from {connection_id}: {e}")
                    break
        
        except ConnectionClosed:
            self._logger.info(f"Client disconnected: {connection_id}")
        except Exception as e:
            self._logger.error(f"Error handling client {connection_id}: {e}")
        finally:
            # Clean up connection
            if connection_id:
                await self.connection_manager.remove_connection(connection_id)
    
    async def _process_client_message(self, connection_id: str, raw_message: str):
        """Process a message from a client."""
        try:
            # Parse message
            message = Message.from_json(raw_message)
            message.sender_id = connection_id
            
            # Trace incoming message
            self._trace_message("receive", message, connection_id)
            
            # Validate message against schemas
            if not self._validate_message(message):
                # Validation failed in lenient mode - send error but continue
                error_msg = self.message_handler.create_error_message(
                    "Message validation failed"
                )
                await self.connection_manager.send_to_connection(connection_id, error_msg)
                return
            
            # Apply message middleware
            for middleware in self._message_middleware:
                message = await self._call_async_or_sync(middleware, message, connection_id)
                if message is None:
                    return
            
            # Update connection last seen
            connection = self.connection_manager.get_connection(connection_id)
            if connection:
                connection.update_last_seen()
            
            # Track message received
            self._track_message_received(message)
            
            # Process message through handler with timing
            import time
            start_time = time.time()
            context = {'connection_id': connection_id, 'connection': connection}
            
            try:
                await self.message_handler.process_message(message, context)
            except Exception as e:
                self._track_error(e, f"Processing message {message.id}")
                raise
            finally:
                # Track processing time
                if self._performance_monitoring_enabled:
                    duration = time.time() - start_time
                    self._track_processing_time(duration)
            
            # Route message to other clients if auto-routing is enabled
            if self.auto_route_messages:
                await self._route_message(message, connection_id)
            
            # Emit message received event
            await self.event_manager.publish(
                EventType.MESSAGE_RECEIVED,
                data={
                    'connection_id': connection_id,
                    'message_id': message.id,
                    'message_type': message.type,
                    'content_length': len(str(message.content))
                }
            )
            
        except MessageException as e:
            # Schema validation or other message error in strict mode
            raise
        except Exception as e:
            raise MessageException(f"Failed to process message: {e}")
    
    async def _route_message(self, message: Message, sender_id: str):
        """
        Route message to appropriate recipients based on message properties.
        
        This method implements smart routing logic:
        - Direct messages: send to specific recipient
        - Channel messages: send to all in channel (excluding sender)
        - Broadcast messages: send to all connections (excluding sender)
        - Custom/Text messages: broadcast to all (excluding sender) by default
        """
        try:
            # Direct message - send to specific recipient
            if message.recipient_id:
                self._logger.debug(f"Routing direct message from {sender_id} to {message.recipient_id}")
                await self.connection_manager.send_to_connection(message.recipient_id, message)
                return
            
            # Channel message - send to all in channel (excluding sender)
            if message.channel:
                self._logger.debug(f"Routing channel message from {sender_id} to channel {message.channel}")
                await self.connection_manager.send_to_channel(
                    message.channel, message, exclude_connection=sender_id
                )
                return
            
            # Broadcast message - send to all (excluding sender)
            if message.type == MessageType.BROADCAST:
                self._logger.debug(f"Broadcasting message from {sender_id} to all clients")
                await self.connection_manager.broadcast(message, exclude_connection=sender_id)
                return
            
            # Custom and Text messages - broadcast by default (excluding sender)
            # These are the most common user messages
            if message.type in [MessageType.CUSTOM, MessageType.TEXT]:
                self._logger.debug(f"Routing {message.type} message from {sender_id} to all clients")
                await self.connection_manager.broadcast(message, exclude_connection=sender_id)
                return
            
            # System, Error, Status, Heartbeat, Auth messages are NOT auto-routed
            # These are typically server-to-client only
            self._logger.debug(f"Message type {message.type} not auto-routed")
            
        except Exception as e:
            self._logger.error(f"Failed to route message: {e}")
    
    async def _handle_system_message(self, message: Message, context: Optional[Dict[str, Any]] = None):
        """Handle system messages."""
        # System messages are typically handled internally
        self._logger.debug(f"System message: {message.content}")
    
    async def _handle_heartbeat(self, message: Message, context: Optional[Dict[str, Any]] = None):
        """Handle heartbeat messages."""
        from .message_handler import MessageContext
        
        if not context:
            return
        
        # Handle both dict and MessageContext
        if isinstance(context, MessageContext):
            connection_id = context.connection_id
        elif isinstance(context, dict):
            connection_id = context.get('connection_id')
        else:
            return
        
        if not connection_id:
            return
        
        # Send heartbeat response
        response = self.message_handler.create_message(
            content="pong",
            message_type=MessageType.HEARTBEAT
        )
        await self.connection_manager.send_to_connection(
            connection_id, 
            response
        )
    
    async def _handle_join_message(self, message: Message, context: Optional[Dict[str, Any]] = None):
        """Handle channel join messages."""
        from .message_handler import MessageContext
        
        if not context:
            return
        
        # Handle both dict and MessageContext
        if isinstance(context, MessageContext):
            connection_id = context.connection_id
        elif isinstance(context, dict):
            connection_id = context.get('connection_id')
        else:
            return
        
        if not connection_id:
            return
        
        # Extract channel from message content
        if isinstance(message.content, dict) and 'channel' in message.content:
            channel = message.content['channel']
            success = await self.connection_manager.join_channel(connection_id, channel)
            
            if success:
                response = self.message_handler.create_system_message(
                    f"Joined channel: {channel}",
                    reply_to=message.id
                )
            else:
                response = self.message_handler.create_error_message(
                    f"Failed to join channel: {channel}",
                    reply_to=message.id
                )
            
            await self.connection_manager.send_to_connection(connection_id, response)
    
    async def _handle_leave_message(self, message: Message, context: Optional[Dict[str, Any]] = None):
        """Handle channel leave messages."""
        from .message_handler import MessageContext
        
        if not context:
            return
        
        # Handle both dict and MessageContext
        if isinstance(context, MessageContext):
            connection_id = context.connection_id
        elif isinstance(context, dict):
            connection_id = context.get('connection_id')
        else:
            return
        
        if not connection_id:
            return
        
        # Extract channel from message content
        if isinstance(message.content, dict) and 'channel' in message.content:
            channel = message.content['channel']
            success = await self.connection_manager.leave_channel(connection_id, channel)
            
            if success:
                response = self.message_handler.create_system_message(
                    f"Left channel: {channel}",
                    reply_to=message.id
                )
            else:
                response = self.message_handler.create_error_message(
                    f"Failed to leave channel: {channel}",
                    reply_to=message.id
                )
            
            await self.connection_manager.send_to_connection(connection_id, response)
    
    async def _handle_auth_message(self, message: Message, context: Optional[Dict[str, Any]] = None):
        """Handle authentication messages."""
        from .message_handler import MessageContext
        
        if not context:
            return
        
        # Handle both dict and MessageContext
        if isinstance(context, MessageContext):
            connection_id = context.connection_id
        elif isinstance(context, dict):
            connection_id = context.get('connection_id')
        else:
            return
        
        if not connection_id:
            return
        
        # Use custom auth handler if available
        if self._auth_handler:
            try:
                auth_result = await self._call_async_or_sync(
                    self._auth_handler, 
                    message, 
                    connection_id
                )
                
                if auth_result and isinstance(auth_result, dict):
                    user_id = auth_result.get('user_id')
                    metadata = auth_result.get('metadata', {})
                    
                    if user_id:
                        await self.connection_manager.authenticate_connection(
                            connection_id, 
                            user_id, 
                            metadata
                        )
                        
                        response = self.message_handler.create_system_message(
                            "Authentication successful"
                        )
                    else:
                        response = self.message_handler.create_error_message(
                            "Authentication failed"
                        )
                else:
                    response = self.message_handler.create_error_message(
                        "Authentication failed"
                    )
                
                await self.connection_manager.send_to_connection(connection_id, response)
                
            except Exception as e:
                self._logger.error(f"Authentication error: {e}")
                response = self.message_handler.create_error_message(
                    "Authentication error"
                )
                await self.connection_manager.send_to_connection(connection_id, response)
    
    async def _call_async_or_sync(self, func: Callable, *args, **kwargs):
        """Call function whether it's async or sync."""
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            return func(*args, **kwargs)
    
    # Public API methods
    def enable_tracing(self, enabled: bool = True):
        """Enable or disable detailed message tracing."""
        self._tracing_enabled = enabled
        if enabled:
            self._logger.info("Message tracing enabled")
        else:
            self._logger.info("Message tracing disabled")
    
    def register_schema(self, message_pattern: str, schema: Type[BaseModel]):
        """
        Register a Pydantic schema for message validation.
        
        Args:
            message_pattern: Pattern to match (e.g., "memory.*" or "operation:recall")
                           Can use wildcards like * and ?
            schema: Pydantic model to validate against
        """
        self._schemas[message_pattern] = schema
        self._logger.info(f"Registered schema for pattern: {message_pattern}")
    
    def enable_strict_validation(self, strict: bool = True):
        """
        Enable or disable strict validation mode.
        
        In strict mode, validation failures raise exceptions.
        In lenient mode, validation failures are logged but don't raise.
        """
        self._strict_validation = strict
        if strict:
            self._logger.info("Strict validation mode enabled")
        else:
            self._logger.info("Lenient validation mode enabled")
    
    def _validate_message(self, message: Message) -> bool:
        """
        Validate a message against registered schemas.
        
        Returns True if validation passes or no schema matches.
        Returns False or raises exception if validation fails.
        """
        if message.type != MessageType.CUSTOM:
            return True
        
        if not isinstance(message.content, dict):
            return True
        
        # Check all schema patterns
        for pattern, schema in self._schemas.items():
            if self._matches_pattern(message.content, pattern):
                try:
                    # Validate message content against schema
                    schema.model_validate(message.content)
                    self._logger.debug(f"Message validated against schema: {pattern}")
                    return True
                except ValidationError as e:
                    error_msg = f"Schema validation failed for pattern '{pattern}': {e}"
                    self._logger.error(error_msg)
                    
                    if self._strict_validation:
                        raise MessageException(error_msg)
                    return False
        
        return True
    
    def _matches_pattern(self, content: Dict[str, Any], pattern: str) -> bool:
        """
        Check if message content matches a pattern.
        
        Supports wildcards and key-based matching.
        """
        # Simple pattern matching for now
        # Pattern can be: "operation:*", "memory.*", etc.
        
        if ':' in pattern:
            # Key:value pattern matching
            key, value_pattern = pattern.split(':', 1)
            if key in content:
                content_value = str(content[key])
                return fnmatch.fnmatch(content_value, value_pattern)
        elif '.' in pattern:
            # Dot notation for nested keys
            keys = pattern.split('.')
            current = content
            for key in keys[:-1]:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return False
            # Check last key with wildcard
            last_key = keys[-1]
            if isinstance(current, dict):
                return any(fnmatch.fnmatch(k, last_key) for k in current.keys())
        else:
            # Simple key match
            return pattern in content
        
        return False
    
    def _trace_message(self, direction: str, message: Message, connection_id: str = None):
        """Log message trace information if tracing is enabled."""
        if not self._tracing_enabled:
            return
        
        trace_id = message.id[:8]  # Use first 8 chars of message ID as trace ID
        content_preview = str(message.content)[:200] if message.content else ""
        
        if direction == "send":
            self._logger.debug(
                f"[TRACE-{trace_id}] Sending {message.type} to {connection_id}"
            )
        elif direction == "receive":
            self._logger.debug(
                f"[TRACE-{trace_id}] Received {message.type} from {connection_id}"
            )
        
        self._logger.debug(f"[TRACE-{trace_id}] Content: {content_preview}")
    
    def enable_metrics(self, enabled: bool = True):
        """Enable or disable message metrics collection."""
        self._metrics_enabled = enabled
        if enabled:
            self._logger.info("Message metrics collection enabled")
        else:
            self._logger.info("Message metrics collection disabled")
    
    def enable_performance_monitoring(self, enabled: bool = True):
        """Enable or disable performance monitoring."""
        self._performance_monitoring_enabled = enabled
        if enabled:
            self._logger.info("Performance monitoring enabled")
        else:
            self._logger.info("Performance monitoring disabled")
    
    def enable_error_tracking(self, enabled: bool = True):
        """Enable or disable error tracking."""
        self._error_tracking_enabled = enabled
        if enabled:
            self._logger.info("Error tracking enabled")
            self._error_log = []
        else:
            self._logger.info("Error tracking disabled")
    
    def get_message_metrics(self) -> Dict[str, Any]:
        """Get current message metrics."""
        return self._message_metrics.copy()
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics."""
        if not self._message_metrics['processing_times']:
            return {
                'avg_processing_time': 0,
                'min_processing_time': 0,
                'max_processing_time': 0,
                'total_messages': 0
            }
        
        times = self._message_metrics['processing_times']
        return {
            'avg_processing_time': sum(times) / len(times),
            'min_processing_time': min(times),
            'max_processing_time': max(times),
            'total_messages': len(times)
        }
    
    def get_error_log(self) -> list:
        """Get error tracking log."""
        return self._error_log.copy()
    
    def _track_message_received(self, message: Message):
        """Track metrics for received message."""
        if not self._metrics_enabled:
            return
        
        self._message_metrics['total_received'] += 1
        msg_type = str(message.type)
        if msg_type not in self._message_metrics['by_type']:
            self._message_metrics['by_type'][msg_type] = {'received': 0, 'sent': 0}
        self._message_metrics['by_type'][msg_type]['received'] += 1
    
    def _track_message_sent(self, message: Message):
        """Track metrics for sent message."""
        if not self._metrics_enabled:
            return
        
        self._message_metrics['total_sent'] += 1
        msg_type = str(message.type)
        if msg_type not in self._message_metrics['by_type']:
            self._message_metrics['by_type'][msg_type] = {'received': 0, 'sent': 0}
        self._message_metrics['by_type'][msg_type]['sent'] += 1
    
    def _track_processing_time(self, duration: float):
        """Track message processing time."""
        if not self._performance_monitoring_enabled:
            return
        
        self._message_metrics['processing_times'].append(duration)
        # Keep only last 1000 samples to avoid unbounded growth
        if len(self._message_metrics['processing_times']) > 1000:
            self._message_metrics['processing_times'] = self._message_metrics['processing_times'][-1000:]
    
    def _track_error(self, error: Exception, context: str = ""):
        """Track error for error tracking."""
        if not self._error_tracking_enabled:
            return
        
        import time
        self._message_metrics['errors'] += 1
        error_entry = {
            'timestamp': time.time(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'context': context
        }
        self._error_log.append(error_entry)
        # Keep only last 100 errors
        if len(self._error_log) > 100:
            self._error_log = self._error_log[-100:]
    
    def set_auth_handler(self, auth_handler: Callable):
        """Set custom authentication handler."""
        self._auth_handler = auth_handler
    
    def add_connection_middleware(self, middleware: Callable):
        """Add connection middleware."""
        self._connection_middleware.append(middleware)
    
    def add_message_middleware(self, middleware: Callable):
        """Add message middleware."""
        self._message_middleware.append(middleware)
    
    def register_message_handler(self, message_type: MessageType, handler: Callable):
        """Register a custom message handler."""
        self.message_handler.register_handler(message_type, handler)
    
    def subscribe_to_event(self, event_type: EventType, listener: Callable):
        """Subscribe to server events."""
        self.event_manager.subscribe(event_type, listener)
    
    async def broadcast_message(self, content: str, message_type: MessageType = MessageType.BROADCAST):
        """Broadcast a message to all connected clients."""
        message = self.message_handler.create_message(content, message_type)
        self._track_message_sent(message)
        return await self.connection_manager.broadcast(message)
    
    async def send_to_channel(self, channel: str, content: Union[str, Dict[str, Any]], 
                            message_type: MessageType = MessageType.TEXT):
        """Send a message to all clients in a channel."""
        message = self.message_handler.create_message(content, message_type)
        self._track_message_sent(message)
        return await self.connection_manager.send_to_channel(channel, message)
    
    async def send_to_user(
        self,
        user_id: str,
        content: Union[str, Dict[str, Any]],
        message_type: MessageType = MessageType.DIRECT,
        routing: Optional[Dict[str, Any]] = None
    ):
        """
        Send a message to all connections of a specific user.
        
        Automatically serializes content based on message type:
        - CUSTOM messages: dict -> JSON string for transport
        - TEXT messages: must be string
        - Other types: flexible
        
        Args:
            user_id: Target user ID
            content: Message content (str or dict)
            message_type: Type of message
            routing: Optional routing metadata (sender, recipient, etc.)
        """
        # Validate content type based on message type
        if message_type == MessageType.CUSTOM:
            if isinstance(content, dict):
                # Valid - will be serialized for transport
                pass
            elif isinstance(content, str):
                # Validate it's valid JSON
                try:
                    json.loads(content)
                except json.JSONDecodeError:
                    raise ValueError("CUSTOM message must be dict or valid JSON string")
            else:
                raise ValueError(f"CUSTOM message must be dict or JSON string, got {type(content)}")
        elif message_type == MessageType.TEXT:
            if not isinstance(content, str):
                raise ValueError(f"TEXT message must be string, got {type(content)}")
        
        # Create message
        message = self.message_handler.create_message(
            content, 
            message_type,
            metadata=routing or {}
        )
        
        # Trace outgoing message
        self._trace_message("send", message, user_id)
        
        return await self.connection_manager.send_to_user(user_id, message)
    
    async def route_message(
        self,
        content: Union[str, Dict[str, Any]],
        sender: str,
        recipient: str,
        message_type: MessageType = MessageType.CUSTOM,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Route a message to the specified recipient with routing metadata.
        
        Args:
            content: Message content
            sender: Sender ID/alias
            recipient: Recipient ID/alias
            message_type: Type of message
            metadata: Additional routing metadata
        """
        from .message_handler import RoutedMessage
        
        # Create routed message
        routed_msg = RoutedMessage(
            content=content,
            sender=sender,
            recipient=recipient,
            metadata=metadata or {}
        )
        
        # Prepare routing envelope
        envelope = {
            'sender': routed_msg.sender,
            'recipient': routed_msg.recipient,
            'timestamp': routed_msg.timestamp,
            'metadata': routed_msg.metadata
        }
        
        # Send to recipient with routing context
        await self.send_to_user(
            recipient,
            routed_msg.content,
            message_type,
            routing=envelope
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics."""
        return {
            'server': {
                'running': self._running,
                'host': self.host,
                'port': self.port,
                'max_connections': self.max_connections
            },
            'connections': self.connection_manager.get_stats(),
            'events': self.event_manager.get_stats()
        }
    
    @property
    def is_running(self) -> bool:
        """Check if server is running."""
        return self._running


async def main():
    """Main function for running the server."""
    import signal
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and start server
    server = ChatterServer()
    
    # Graceful shutdown handler
    def signal_handler():
        asyncio.create_task(server.stop())
    
    # Register signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await server.start()
        print(f"ChatterServer running on {server.host}:{server.port}")
        print("Press Ctrl+C to stop the server")
        
        # Keep server running
        while server.is_running:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\\nShutting down server...")
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
