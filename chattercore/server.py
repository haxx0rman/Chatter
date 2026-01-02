"""
ChatterCore server module

This module provides the main server implementation for the ChatterCore system.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, Callable
import websockets
from websockets import ConnectionClosed

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
        close_timeout: int = 10
    ):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self.message_size_limit = message_size_limit
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.close_timeout = close_timeout
        
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
            
            # Apply message middleware
            for middleware in self._message_middleware:
                message = await self._call_async_or_sync(middleware, message, connection_id)
                if message is None:
                    return
            
            # Update connection last seen
            connection = self.connection_manager.get_connection(connection_id)
            if connection:
                connection.update_last_seen()
            
            # Process message through handler
            context = {'connection_id': connection_id, 'connection': connection}
            await self.message_handler.process_message(message, context)
            
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
            
        except Exception as e:
            raise MessageException(f"Failed to process message: {e}")
    
    async def _handle_system_message(self, message: Message, context: Optional[Dict[str, Any]] = None):
        """Handle system messages."""
        # System messages are typically handled internally
        self._logger.debug(f"System message: {message.content}")
    
    async def _handle_heartbeat(self, message: Message, context: Optional[Dict[str, Any]] = None):
        """Handle heartbeat messages."""
        if context and 'connection_id' in context:
            # Send heartbeat response
            response = self.message_handler.create_message(
                content="pong",
                message_type=MessageType.HEARTBEAT
            )
            await self.connection_manager.send_to_connection(
                context['connection_id'], 
                response
            )
    
    async def _handle_join_message(self, message: Message, context: Optional[Dict[str, Any]] = None):
        """Handle channel join messages."""
        if not context or 'connection_id' not in context:
            return
        
        connection_id = context['connection_id']
        
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
        if not context or 'connection_id' not in context:
            return
        
        connection_id = context['connection_id']
        
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
        if not context or 'connection_id' not in context:
            return
        
        connection_id = context['connection_id']
        
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
        return await self.connection_manager.broadcast(message)
    
    async def send_to_channel(self, channel: str, content: str, 
                            message_type: MessageType = MessageType.TEXT):
        """Send a message to all clients in a channel."""
        message = self.message_handler.create_message(content, message_type)
        return await self.connection_manager.send_to_channel(channel, message)
    
    async def send_to_user(self, user_id: str, content: str,
                         message_type: MessageType = MessageType.DIRECT):
        """Send a message to all connections of a specific user."""
        message = self.message_handler.create_message(content, message_type)
        return await self.connection_manager.send_to_user(user_id, message)
    
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
