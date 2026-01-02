"""
Example usage of ChatterCore server and client
"""

import asyncio
import logging
from chattercore import ChatterServer, ChatterClient, EventType, MessageType


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


async def example_server():
    """Example server with custom message handlers and event listeners."""
    
    # Create server
    server = ChatterServer(host="localhost", port=8765)
    
    # Custom message handler for text messages
    async def handle_text_message(message, context):
        print(f"Received text message: {message.content}")
        
        # Echo the message back to all clients in the same channel
        if message.channel:
            await server.send_to_channel(
                message.channel, 
                f"Echo: {message.content}",
                MessageType.TEXT
            )
        else:
            # Broadcast to all clients
            await server.broadcast_message(f"Broadcast: {message.content}")
    
    # Custom authentication handler
    async def auth_handler(message, connection_id):
        if isinstance(message.content, dict):
            username = message.content.get('username')
            password = message.content.get('password')
            
            # Simple authentication (in real apps, use proper auth)
            if username and password == 'secret':
                return {
                    'user_id': username,
                    'metadata': {'auth_time': message.timestamp}
                }
        return None
    
    # Event listeners
    async def on_client_connected(event):
        print(f"Client connected: {event.data['connection_id']}")
        print(f"Total connections: {event.data['total_connections']}")
    
    async def on_client_disconnected(event):
        print(f"Client disconnected: {event.data['connection_id']}")
        print(f"Total connections: {event.data['total_connections']}")
    
    # Register handlers and listeners
    server.register_message_handler(MessageType.TEXT, handle_text_message)
    server.set_auth_handler(auth_handler)
    server.subscribe_to_event(EventType.CLIENT_CONNECTED, on_client_connected)
    server.subscribe_to_event(EventType.CLIENT_DISCONNECTED, on_client_disconnected)
    
    # Start server
    await server.start()
    print(f"Server running on {server.host}:{server.port}")
    print("Press Ctrl+C to stop")
    
    try:
        # Keep server running
        while server.is_running:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\\nShutting down server...")
    finally:
        await server.stop()


async def example_client():
    """Example client with authentication and channel operations."""
    
    # Create client
    client = ChatterClient("ws://localhost:8765")
    
    # Event listeners
    async def on_message_received(event):
        print(f"Received message: {event.data}")
    
    async def on_connection_lost(event):
        print("Connection lost! Client will try to reconnect...")
    
    async def on_connection_restored(event):
        print(f"Connection restored after {event.data['reconnect_count']} attempts")
    
    # Subscribe to events
    client.subscribe_to_event(EventType.MESSAGE_RECEIVED, on_message_received)
    client.subscribe_to_event(EventType.CONNECTION_LOST, on_connection_lost)
    client.subscribe_to_event(EventType.CONNECTION_RESTORED, on_connection_restored)
    
    # Connect to server
    connected = await client.connect()
    if not connected:
        print("Failed to connect to server")
        return
    
    print("Connected to server")
    
    # Authenticate
    auth_success = await client.authenticate({
        'username': 'testuser',
        'password': 'secret'
    })
    
    if auth_success:
        print("Authentication successful")
    else:
        print("Authentication failed")
    
    # Join a channel
    await client.join_channel("general")
    print("Joined channel: general")
    
    # Send some messages
    await client.send_message("Hello, ChatterCore!", channel="general")
    await client.send_message("This is a test message", channel="general")
    
    # Wait a bit to receive responses
    await asyncio.sleep(2)
    
    # Leave channel and disconnect
    await client.leave_channel("general")
    await client.disconnect()
    print("Disconnected from server")


async def run_server_and_client():
    """Run server and client together for demonstration."""
    
    # Start server in background
    server_task = asyncio.create_task(example_server())
    
    # Give server time to start
    await asyncio.sleep(1)
    
    # Run client
    try:
        await example_client()
    finally:
        # Stop server
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    print("ChatterCore Example")
    print("1. Server only")
    print("2. Client only") 
    print("3. Both server and client")
    
    choice = input("Enter choice (1-3): ").strip()
    
    if choice == "1":
        asyncio.run(example_server())
    elif choice == "2":
        asyncio.run(example_client())
    elif choice == "3":
        asyncio.run(run_server_and_client())
    else:
        print("Invalid choice")
