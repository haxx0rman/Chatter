"""
Chat room example using ChatterCore
"""

import asyncio
import logging
from datetime import datetime
from chattercore import ChatterServer, ChatterClient, EventType, MessageType


logging.basicConfig(level=logging.INFO)


class ChatRoom:
    """A simple chat room implementation using ChatterCore."""
    
    def __init__(self, name="general"):
        self.name = name
        self.server = ChatterServer(host="localhost", port=8765)
        self.users = {}  # connection_id -> user_info
        self.setup_handlers()
    
    def setup_handlers(self):
        """Setup message handlers and event listeners."""
        
        # Handle chat messages
        async def handle_chat_message(message, context):
            connection_id = context.get('connection_id')
            user_info = self.users.get(connection_id, {})
            username = user_info.get('username', f'User_{connection_id[:8]}')
            
            # Format chat message
            timestamp = datetime.now().strftime("%H:%M:%S")
            formatted_message = f"[{timestamp}] {username}: {message.content}"
            
            # Broadcast to all users in the room
            await self.server.send_to_channel(
                self.name, 
                formatted_message,
                exclude_connection=connection_id
            )
            
            print(formatted_message)
        
        # Handle user joining
        async def handle_join(message, context):
            connection_id = context.get('connection_id')
            
            if isinstance(message.content, dict):
                username = message.content.get('username', f'User_{connection_id[:8]}')
                self.users[connection_id] = {'username': username}
                
                # Join the chat room channel
                await self.server.connection_manager.join_channel(connection_id, self.name)
                
                # Announce user joined
                join_message = f"*** {username} joined the chat room ***"
                await self.server.send_to_channel(self.name, join_message)
                print(join_message)
        
        # Handle user leaving
        async def on_client_disconnected(event):
            connection_id = event.data.get('connection_id')
            user_info = self.users.pop(connection_id, {})
            username = user_info.get('username', f'User_{connection_id[:8]}')
            
            # Announce user left
            leave_message = f"*** {username} left the chat room ***"
            await self.server.send_to_channel(self.name, leave_message)
            print(leave_message)
        
        # Register handlers
        self.server.register_message_handler(MessageType.TEXT, handle_chat_message)
        self.server.register_message_handler(MessageType.JOIN, handle_join)
        self.server.subscribe_to_event(EventType.CLIENT_DISCONNECTED, on_client_disconnected)
    
    async def start(self):
        """Start the chat room server."""
        await self.server.start()
        print(f"Chat room '{self.name}' started on {self.server.host}:{self.server.port}")
        print("Users can connect and start chatting!")
        
        try:
            while self.server.is_running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            print("\\nShutting down chat room...")
        finally:
            await self.server.stop()


class ChatClient:
    """A simple chat client for the chat room."""
    
    def __init__(self, username, server_uri="ws://localhost:8765"):
        self.username = username
        self.client = ChatterClient(server_uri)
        self.setup_handlers()
    
    def setup_handlers(self):
        """Setup message handlers."""
        
        async def on_message_received(event):
            # Print received messages (these are the chat messages)
            pass  # Messages are handled by the text message handler
        
        async def handle_text_message(message, context):
            # Display chat messages
            print(message.content)
        
        # Register handlers  
        self.client.register_message_handler(MessageType.TEXT, handle_text_message)
        self.client.subscribe_to_event(EventType.MESSAGE_RECEIVED, on_message_received)
    
    async def start(self):
        """Start the chat client."""
        # Connect to server
        connected = await self.client.connect()
        if not connected:
            print("Failed to connect to chat room")
            return
        
        print(f"Connected to chat room as {self.username}")
        
        # Join the chat room
        await self.client.send_message(
            content={'username': self.username},
            message_type=MessageType.JOIN
        )
        
        print("Type messages to chat (or 'quit' to exit):")
        
        try:
            while self.client.is_connected:
                try:
                    # Get user input
                    message = await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(None, input, ""),
                        timeout=0.1
                    )
                    
                    if message.strip().lower() == 'quit':
                        break
                        
                    if message.strip():
                        await self.client.send_message(message.strip())
                        
                except asyncio.TimeoutError:
                    continue
                except KeyboardInterrupt:
                    break
                    
        finally:
            await self.client.disconnect()
            print(f"\\n{self.username} left the chat room")


async def run_chat_room():
    """Run the chat room server."""
    chat_room = ChatRoom("general")
    await chat_room.start()


async def run_chat_client(username):
    """Run a chat client."""
    client = ChatClient(username)
    await client.start()


if __name__ == "__main__":
    print("ChatterCore Chat Room")
    print("1. Start chat room server")
    print("2. Join chat room as client")
    
    choice = input("Enter choice (1-2): ").strip()
    
    if choice == "1":
        asyncio.run(run_chat_room())
    elif choice == "2":
        username = input("Enter your username: ").strip() or "Anonymous"
        asyncio.run(run_chat_client(username))
    else:
        print("Invalid choice")
