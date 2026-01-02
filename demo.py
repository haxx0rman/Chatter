#!/usr/bin/env python3
"""
ChatterCore Demonstration Script

This script demonstrates the key features of the ChatterCore system
by running a simple server and client interaction.
"""

import asyncio
import logging
import signal
from chattercore import ChatterServer, ChatterClient, EventType, MessageType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def demo_server_client():
    """Demonstrate basic server-client functionality."""
    print("=" * 60)
    print("ChatterCore System Demonstration")
    print("=" * 60)
    
    # Create and configure server
    server = ChatterServer(host="localhost", port=8765)
    
    # Event listeners for monitoring
    async def on_client_connected(event):
        logger.info(f"📱 Client connected: {event.data['connection_id']}")
        logger.info(f"📊 Total connections: {event.data['total_connections']}")
    
    async def on_client_disconnected(event):
        logger.info(f"📱 Client disconnected: {event.data['connection_id']}")
        logger.info(f"📊 Total connections: {event.data['total_connections']}")
    
    async def on_message_received(event):
        logger.info(f"📨 Message received: {event.data['message_id']} "
                   f"(type: {event.data['message_type']})")
    
    # Custom message handler for text messages
    async def handle_text_message(message, context):
        logger.info(f"💬 Processing text message: {message.content}")
        connection_id = context.get('connection_id')
        
        # Echo the message back
        response = f"Echo: {message.content}"
        await server.send_to_user(connection_id, response, MessageType.TEXT)
    
    # Register event listeners and handlers
    server.subscribe_to_event(EventType.CLIENT_CONNECTED, on_client_connected)
    server.subscribe_to_event(EventType.CLIENT_DISCONNECTED, on_client_disconnected)
    server.subscribe_to_event(EventType.MESSAGE_RECEIVED, on_message_received)
    server.register_message_handler(MessageType.TEXT, handle_text_message)
    
    # Start server
    logger.info("🚀 Starting ChatterCore server...")
    await server.start()
    logger.info(f"✅ Server running on {server.host}:{server.port}")
    
    try:
        # Create client
        client = ChatterClient("ws://localhost:8765")
        
        # Client event listeners
        async def on_client_message(message, context):
            logger.info(f"📥 Client received: {message.content}")
        
        client.register_message_handler(MessageType.TEXT, on_client_message)
        
        # Connect client
        logger.info("🔌 Connecting client to server...")
        connected = await client.connect()
        
        if connected:
            logger.info("✅ Client connected successfully")
            
            # Demonstrate messaging
            logger.info("💬 Sending test messages...")
            
            await client.send_message("Hello, ChatterCore!")
            await asyncio.sleep(0.5)
            
            await client.send_message("This is a test message")
            await asyncio.sleep(0.5)
            
            # Demonstrate channel operations
            logger.info("🏠 Testing channel operations...")
            join_result = await client.join_channel("demo-channel")
            logger.info(f"Channel join result: {join_result}")
            
            if join_result:
                leave_result = await client.leave_channel("demo-channel")
                logger.info(f"Channel leave result: {leave_result}")
            
            # Demonstrate heartbeat
            logger.info("💓 Testing heartbeat...")
            await client.send_message("ping", MessageType.HEARTBEAT)
            await asyncio.sleep(0.5)
            
            # Show server stats
            stats = server.get_stats()
            logger.info("📊 Server Statistics:")
            logger.info(f"   - Running: {stats['server']['running']}")
            logger.info(f"   - Active connections: {stats['connections']['active_connections']}")
            logger.info(f"   - Messages sent: {stats['connections']['messages_sent']}")
            logger.info(f"   - Events processed: {stats['events']['events_processed']}")
            
            # Show client stats
            client_stats = client.get_stats()
            logger.info("📊 Client Statistics:")
            logger.info(f"   - State: {client_stats['state']}")
            logger.info(f"   - Connected: {client_stats['connected']}")
            logger.info(f"   - Reconnect count: {client_stats['reconnect_count']}")
            
            logger.info("✅ Demonstration completed successfully!")
            
            # Disconnect client
            await client.disconnect()
            logger.info("🔌 Client disconnected")
        
        else:
            logger.error("❌ Failed to connect client")
    
    except Exception as e:
        logger.error(f"❌ Error during demonstration: {e}")
    
    finally:
        # Stop server
        logger.info("🛑 Stopping server...")
        await server.stop()
        logger.info("✅ Server stopped")
    
    print("=" * 60)
    print("Demonstration Complete!")
    print("=" * 60)


async def demo_multiple_clients():
    """Demonstrate multiple clients connecting to the same server."""
    print("=" * 60)
    print("Multiple Clients Demonstration")
    print("=" * 60)
    
    server = ChatterServer(host="localhost", port=8766)
    await server.start()
    logger.info("✅ Server started for multiple clients demo")
    
    try:
        # Create multiple clients
        clients = []
        client_names = ["Alice", "Bob", "Charlie"]
        
        for name in client_names:
            client = ChatterClient("ws://localhost:8766")
            clients.append((name, client))
        
        # Connect all clients
        logger.info("🔌 Connecting multiple clients...")
        for name, client in clients:
            connected = await client.connect()
            if connected:
                logger.info(f"✅ {name} connected")
                # Send a greeting message
                await client.send_message(f"Hello from {name}!")
            else:
                logger.error(f"❌ {name} failed to connect")
        
        # Let messages process
        await asyncio.sleep(1)
        
        # Disconnect all clients
        logger.info("🔌 Disconnecting clients...")
        for name, client in clients:
            await client.disconnect()
            logger.info(f"👋 {name} disconnected")
        
        # Show final stats
        stats = server.get_stats()
        logger.info(f"📊 Final stats - Total connections handled: {stats['connections']['total_connections']}")
    
    finally:
        await server.stop()
        logger.info("✅ Multiple clients demo completed")


async def main():
    """Main demonstration function."""
    # Setup graceful shutdown
    shutdown_event = asyncio.Event()
    
    def signal_handler():
        logger.info("🛑 Shutdown requested...")
        shutdown_event.set()
    
    # Register signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        # Run basic demo
        await demo_server_client()
        
        # Add a small delay between demos
        await asyncio.sleep(1)
        
        # Run multiple clients demo
        await demo_multiple_clients()
        
        print("\\n🎉 All demonstrations completed successfully!")
        print("\\nChatterCore Features Demonstrated:")
        print("  ✅ Server-Client Communication")
        print("  ✅ Message Handling & Routing")
        print("  ✅ Event-Driven Architecture")
        print("  ✅ Channel Operations")
        print("  ✅ Heartbeat System")
        print("  ✅ Statistics & Monitoring")
        print("  ✅ Multiple Client Support")
        print("  ✅ Graceful Connection Management")
        
    except KeyboardInterrupt:
        logger.info("👋 Demo interrupted by user")
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        raise


if __name__ == "__main__":
    print("🚀 Starting ChatterCore Demonstration...")
    print("Press Ctrl+C to stop")
    asyncio.run(main())
