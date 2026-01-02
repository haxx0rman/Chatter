"""
ChatterCore client module

This module provides the client implementation for connecting to ChatterCore servers.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, Callable, List, Union
from enum import Enum
import websockets
from websockets import ConnectionClosed

from .event_manager import EventManager, EventType
from .message_handler import MessageHandler, Message, MessageType
from .exceptions import ClientException, ConnectionException


class ClientState(str, Enum):
    """Client connection states."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    AUTHENTICATING = "authenticating"
    AUTHENTICATED = "authenticated"
    CLOSING = "closing"


class ChatterClient:
    """
    WebSocket client for connecting to ChatterCore servers.
    
    This class provides a robust client with auto-reconnection, message handling,
    and event-driven architecture for communicating with ChatterCore servers.
    """
    
    def __init__(
        self,
        uri: str,
        auto_reconnect: bool = True,
        reconnect_interval: int = 5,
        max_reconnect_attempts: int = 10,
        heartbeat_interval: int = 30,
        message_timeout: int = 10,
        connect_timeout: int = 10
    ):
        self.uri = uri
        self.auto_reconnect = auto_reconnect
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        self.heartbeat_interval = heartbeat_interval
        self.message_timeout = message_timeout
        self.connect_timeout = connect_timeout
        
        # Core components
        self.event_manager = EventManager()
        self.message_handler = MessageHandler()
        
        # Client state
        self._websocket: Optional[Any] = None  # WebSocket connection
        self._state = ClientState.DISCONNECTED
        self._reconnect_count = 0
        self._logger = logging.getLogger(__name__)
        
        # Tasks
        self._listen_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        
        # Message handling
        self._pending_messages: Dict[str, asyncio.Future] = {}
        self._response_handlers: Dict[MessageType, List[Callable]] = {}
        
        # Authentication
        self._auth_token: Optional[str] = None
        self._user_id: Optional[str] = None
        self._authenticated = False
        
        # Setup default handlers
        self._setup_default_handlers()
    
    def _setup_default_handlers(self):
        """Setup default message handlers."""
        self.message_handler.register_handler(
            MessageType.SYSTEM,
            self._handle_system_message
        )
        
        self.message_handler.register_handler(
            MessageType.HEARTBEAT,
            self._handle_heartbeat
        )
        
        self.message_handler.register_handler(
            MessageType.ERROR,
            self._handle_error_message
        )
        
        self.message_handler.register_handler(
            MessageType.AUTH,
            self._handle_auth_response
        )
    
    async def connect(self) -> bool:
        """
        Connect to the ChatterCore server.
        
        Returns True if connection successful, False otherwise.
        """
        if self._state in (ClientState.CONNECTED, ClientState.CONNECTING):
            return True
        
        try:
            self._state = ClientState.CONNECTING
            self._logger.info(f"Connecting to {self.uri}")
            
            # Start event manager
            await self.event_manager.start()
            
            # Connect to WebSocket
            self._websocket = await asyncio.wait_for(
                websockets.connect(self.uri),
                timeout=self.connect_timeout
            )
            
            self._state = ClientState.CONNECTED
            self._reconnect_count = 0
            
            # Start tasks
            self._listen_task = asyncio.create_task(self._listen_for_messages())
            if self.heartbeat_interval > 0:
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            
            self._logger.info("Connected to server")
            
            # Emit connection event
            await self.event_manager.publish(
                EventType.CLIENT_CONNECTED,
                data={'uri': self.uri, 'reconnect_count': self._reconnect_count}
            )
            
            return True
            
        except Exception as e:
            self._logger.error(f"Failed to connect: {e}")
            self._state = ClientState.DISCONNECTED
            
            # Start reconnection if enabled
            if self.auto_reconnect and self._reconnect_count < self.max_reconnect_attempts:
                await self._start_reconnect()
            
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from the server."""
        if self._state == ClientState.DISCONNECTED:
            return
        
        self._state = ClientState.CLOSING
        self._logger.info("Disconnecting from server")
        
        # Cancel tasks
        await self._cancel_tasks()
        
        # Close WebSocket
        if self._websocket and hasattr(self._websocket, 'close'):
            try:
                await self._websocket.close()
            except Exception:
                pass  # Ignore close errors
        
        # Stop event manager
        await self.event_manager.stop()
        
        self._state = ClientState.DISCONNECTED
        self._websocket = None
        self._authenticated = False
        
        # Emit disconnection event
        await self.event_manager.publish(
            EventType.CLIENT_DISCONNECTED,
            data={'graceful': True}
        )
        
        self._logger.info("Disconnected from server")
    
    async def send_message(
        self,
        content: Union[str, Dict[str, Any]],
        message_type: MessageType = MessageType.TEXT,
        recipient_id: Optional[str] = None,
        channel: Optional[str] = None,
        wait_for_response: bool = False,
        callback: Optional[Callable] = None,
        timeout: Optional[int] = None
    ) -> Optional[Message]:
        """
        Send a message to the server.
        
        Args:
            content: Message content
            message_type: Type of message
            recipient_id: Target recipient ID
            channel: Channel to send to
            wait_for_response: If True, wait synchronously for response
            callback: If provided, call this function when response is received
            timeout: Response timeout (uses message_timeout if not specified)
        
        Returns:
            Response message if wait_for_response=True, otherwise None
        """
        # Validate parameters first
        if wait_for_response and callback:
            raise ClientException("Cannot use both wait_for_response and callback - choose one")
        
        if not self.is_connected:
            raise ClientException("Not connected to server")
        
        # Create message
        message = self.message_handler.create_message(
            content=content,
            message_type=message_type,
            sender_id=self._user_id,
            recipient_id=recipient_id,
            channel=channel
        )
        
        # Set up response handling
        response_timeout = timeout or self.message_timeout
        response_future = None
        
        if wait_for_response:
            # Use callback handler for synchronous waiting
            response_future = asyncio.create_task(
                self.message_handler.wait_for_response(message.id, response_timeout)
            )
        elif callback:
            # Register callback for asynchronous response
            self.message_handler.register_callback(message.id, callback, response_timeout)
        
        try:
            # Send message
            await self._websocket.send(message.to_json())
            
            # Emit message sent event
            await self.event_manager.publish(
                EventType.MESSAGE_SENT,
                data={
                    'message_id': message.id,
                    'message_type': message_type,
                    'recipient_id': recipient_id,
                    'channel': channel
                }
            )
            
            # Wait for response if requested
            if wait_for_response and response_future:
                try:
                    response = await response_future
                    return response
                except asyncio.TimeoutError:
                    raise ClientException(f"Response timeout for message {message.id}")
            
            return message
            
        except Exception as e:
            # Emit message failed event
            await self.event_manager.publish(
                EventType.MESSAGE_FAILED,
                data={'message_id': message.id, 'error': str(e)}
            )
            
            raise ClientException(f"Failed to send message: {e}")
    
    async def authenticate(self, auth_data: Dict[str, Any]) -> bool:
        """
        Authenticate with the server.
        
        Returns True if authentication successful, False otherwise.
        """
        if not self.is_connected:
            raise ClientException("Not connected to server")
        
        self._state = ClientState.AUTHENTICATING
        
        try:
            # Send authentication message
            response = await self.send_message(
                content=auth_data,
                message_type=MessageType.AUTH,
                wait_for_response=True,
                timeout=self.message_timeout
            )
            
            if response and response.type == MessageType.SYSTEM:
                if isinstance(response.content, str) and "successful" in response.content.lower():
                    self._authenticated = True
                    self._state = ClientState.AUTHENTICATED
                    self._auth_token = auth_data.get('token')
                    self._user_id = auth_data.get('user_id')
                    
                    self._logger.info("Authentication successful")
                    return True
            
            self._state = ClientState.CONNECTED
            self._logger.warning("Authentication failed")
            return False
            
        except Exception as e:
            self._state = ClientState.CONNECTED
            self._logger.error(f"Authentication error: {e}")
            return False
    
    async def join_channel(self, channel: str) -> bool:
        """Join a channel."""
        try:
            response = await self.send_message(
                content={'channel': channel},
                message_type=MessageType.JOIN,
                wait_for_response=True
            )
            
            return response is not None and response.type == MessageType.SYSTEM
            
        except Exception as e:
            self._logger.error(f"Failed to join channel {channel}: {e}")
            return False
    
    async def leave_channel(self, channel: str) -> bool:
        """Leave a channel."""
        try:
            response = await self.send_message(
                content={'channel': channel},
                message_type=MessageType.LEAVE,
                wait_for_response=True
            )
            
            return response is not None and response.type == MessageType.SYSTEM
            
        except Exception as e:
            self._logger.error(f"Failed to leave channel {channel}: {e}")
            return False
    
    async def _listen_for_messages(self):
        """Listen for incoming messages from the server."""
        try:
            async for raw_message in self._websocket:
                try:
                    await self._process_server_message(raw_message)
                except Exception as e:
                    self._logger.error(f"Error processing message: {e}")
                    
        except ConnectionClosed:
            self._logger.info("Connection closed by server")
            await self._handle_connection_lost()
        except Exception as e:
            self._logger.error(f"Error in message listener: {e}")
            await self._handle_connection_lost()
    
    async def _process_server_message(self, raw_message: str):
        """Process an incoming message from the server."""
        try:
            message = Message.from_json(raw_message)
            
            # Check for pending response
            if message.reply_to and message.reply_to in self._pending_messages:
                future = self._pending_messages.pop(message.reply_to)
                if not future.done():
                    future.set_result(message)
                return
            
            # Process through message handler
            await self.message_handler.process_message(message, {'client': self})
            
            # Emit message received event
            await self.event_manager.publish(
                EventType.MESSAGE_RECEIVED,
                data={
                    'message_id': message.id,
                    'message_type': message.type,
                    'sender_id': message.sender_id
                }
            )
            
        except Exception as e:
            self._logger.error(f"Failed to process server message: {e}")
    
    async def _handle_system_message(self, message: Message, context: Optional[Dict[str, Any]] = None):
        """Handle system messages from server."""
        self._logger.info(f"System message: {message.content}")
    
    async def _handle_heartbeat(self, message: Message, context: Optional[Dict[str, Any]] = None):
        """Handle heartbeat messages."""
        if isinstance(message.content, str) and message.content == "ping":
            # Respond to server ping
            response = self.message_handler.create_message(
                content="pong",
                message_type=MessageType.HEARTBEAT,
                reply_to=message.id
            )
            await self._websocket.send(response.to_json())
    
    async def _handle_error_message(self, message: Message, context: Optional[Dict[str, Any]] = None):
        """Handle error messages from server."""
        self._logger.error(f"Server error: {message.content}")
        
        # Emit error event
        await self.event_manager.publish(
            EventType.CLIENT_ERROR,
            data={'error': message.content, 'message_id': message.id}
        )
    
    async def _handle_auth_response(self, message: Message, context: Optional[Dict[str, Any]] = None):
        """Handle authentication response."""
        # This is handled in the authenticate method
        pass
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeat messages to server."""
        while self._state in (ClientState.CONNECTED, ClientState.AUTHENTICATED):
            try:
                await asyncio.sleep(self.heartbeat_interval)
                
                if self.is_connected:
                    heartbeat = self.message_handler.create_message(
                        content="ping",
                        message_type=MessageType.HEARTBEAT
                    )
                    await self._websocket.send(heartbeat.to_json())
                    
            except Exception as e:
                self._logger.error(f"Heartbeat error: {e}")
                break
    
    async def _handle_connection_lost(self):
        """Handle lost connection."""
        if self._state == ClientState.CLOSING:
            return
        
        self._state = ClientState.DISCONNECTED
        self._authenticated = False
        
        # Cancel tasks
        await self._cancel_tasks()
        
        # Emit connection lost event
        await self.event_manager.publish(
            EventType.CONNECTION_LOST,
            data={'reconnect_count': self._reconnect_count}
        )
        
        # Start reconnection if enabled
        if self.auto_reconnect and self._reconnect_count < self.max_reconnect_attempts:
            await self._start_reconnect()
    
    async def _start_reconnect(self):
        """Start reconnection process."""
        if self._reconnect_task and not self._reconnect_task.done():
            return
        
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())
    
    async def _reconnect_loop(self):
        """Reconnection loop."""
        while (self._state == ClientState.DISCONNECTED and 
               self._reconnect_count < self.max_reconnect_attempts):
            
            self._reconnect_count += 1
            self._state = ClientState.RECONNECTING
            
            self._logger.info(f"Reconnection attempt {self._reconnect_count}/{self.max_reconnect_attempts}")
            
            try:
                await asyncio.sleep(self.reconnect_interval)
                success = await self.connect()
                
                if success:
                    # Emit connection restored event
                    await self.event_manager.publish(
                        EventType.CONNECTION_RESTORED,
                        data={'reconnect_count': self._reconnect_count}
                    )
                    return
                    
            except Exception as e:
                self._logger.error(f"Reconnection failed: {e}")
        
        # Max reconnection attempts reached
        if self._reconnect_count >= self.max_reconnect_attempts:
            self._logger.error("Max reconnection attempts reached")
            self._state = ClientState.DISCONNECTED
    
    async def _cancel_tasks(self):
        """Cancel all running tasks."""
        tasks = [self._listen_task, self._heartbeat_task, self._reconnect_task]
        
        for task in tasks:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self._listen_task = None
        self._heartbeat_task = None
        self._reconnect_task = None
    
    # Public API methods
    def register_message_handler(self, message_type: MessageType, handler: Callable):
        """Register a custom message handler."""
        self.message_handler.register_handler(message_type, handler)
    
    def subscribe_to_event(self, event_type: EventType, listener: Callable):
        """Subscribe to client events."""
        self.event_manager.subscribe(event_type, listener)
    
    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return (self._websocket is not None and 
                hasattr(self._websocket, 'protocol') and
                self._state in (ClientState.CONNECTED, ClientState.AUTHENTICATED))
    
    @property
    def is_authenticated(self) -> bool:
        """Check if client is authenticated."""
        return self._authenticated and self._state == ClientState.AUTHENTICATED
    
    @property
    def state(self) -> ClientState:
        """Get current client state."""
        return self._state
    
    @property
    def user_id(self) -> Optional[str]:
        """Get authenticated user ID."""
        return self._user_id
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        return {
            'state': self._state.value,
            'connected': self.is_connected,
            'authenticated': self.is_authenticated,
            'user_id': self._user_id,
            'reconnect_count': self._reconnect_count,
            'uri': self.uri,
            'events': self.event_manager.get_stats()
        }


async def main():
    """Main function for running a test client."""
    import signal
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create client
    client = ChatterClient("ws://localhost:8765")
    
    # Connect to server
    connected = await client.connect()
    if not connected:
        print("Failed to connect to server")
        return
    
    print("Connected to server. Type messages (or 'quit' to exit):")
    
    try:
        while client.is_connected:
            try:
                # Simple interactive message sending
                message = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, input, "> "),
                    timeout=1.0
                )
                
                if message.strip().lower() == 'quit':
                    break
                    
                if message.strip():
                    await client.send_message(message.strip())
                    
            except asyncio.TimeoutError:
                continue
            except KeyboardInterrupt:
                break
                
    finally:
        await client.disconnect()
        print("Disconnected from server")


if __name__ == "__main__":
    asyncio.run(main())
