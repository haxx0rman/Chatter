#!/usr/bin/env python3
"""
ChatterCore Callback System Demo

This demo shows both routes of the callback system:
1. Synchronous waiting for responses
2. Asynchronous callback functions
"""

import asyncio
import logging
import time
from typing import Dict, Any

from chattercore import ChatterServer, ChatterClient, MessageType, Message

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def response_callback(message: Message):
    """
    Example callback function for handling asynchronous responses.
    """
    print(f"📞 Callback received response: {message.content}")
    print(f"   Reply to: {message.reply_to}")
    print(f"   Timestamp: {message.timestamp}")


async def demo_callback_system():
    """
    Demonstrate both callback routes with a comprehensive example.
    """
    print("🚀 Starting ChatterCore Callback System Demo")
    
    # Create and start server
    server = ChatterServer(port=8765)
    
    # Register an echo handler that responds with a delay
    async def delayed_echo_handler(message: Message, context: Dict[str, Any]):
        """Handler that responds after a delay to demonstrate callbacks."""
        if message.content.startswith("echo:"):
            # Simulate processing time
            await asyncio.sleep(1)
            
            # Create response
            response_content = f"Echo response: {message.content[5:].strip()}"
            response = server.message_handler.create_message(
                content=response_content,
                message_type=MessageType.TEXT,
                sender_id="server",
                recipient_id=message.sender_id,
                reply_to=message.id
            )
            
            # Send response back to client
            if message.sender_id:
                connection_info = server.connection_manager.get_connection(message.sender_id)
                if connection_info:
                    await connection_info.websocket.send(response.to_json())
    
    server.message_handler.register_handler(MessageType.TEXT, delayed_echo_handler)
    
    # Start server
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)  # Let server start
    
    try:
        # Create clients
        client1 = ChatterClient("ws://localhost:8765", auto_reconnect=False)
        client2 = ChatterClient("ws://localhost:8765", auto_reconnect=False)
        
        await client1.connect()
        await client2.connect()
        await asyncio.sleep(0.5)  # Let clients connect
        
        print("\n" + "="*60)
        print("ROUTE 1: SYNCHRONOUS WAITING FOR RESPONSES")
        print("="*60)
        
        # Route 1: Synchronous waiting
        print("\n📤 Client1 sending message with wait_for_response=True...")
        start_time = time.time()
        
        try:
            response = await client1.send_message(
                "echo: Hello from synchronous route!",
                wait_for_response=True,
                timeout=5
            )
            
            end_time = time.time()
            print(f"✅ Received synchronous response in {end_time - start_time:.2f}s:")
            print(f"   Content: {response.content}")
            print(f"   Message ID: {response.id}")
            print(f"   Reply to: {response.reply_to}")
            
        except Exception as e:
            print(f"❌ Synchronous response failed: {e}")
        
        print("\n" + "="*60)
        print("ROUTE 2: ASYNCHRONOUS CALLBACK FUNCTIONS")
        print("="*60)
        
        # Route 2: Asynchronous callbacks
        print("\n📤 Client2 sending message with callback function...")
        
        # Track callback completion
        callback_received = asyncio.Event()
        original_callback = response_callback
        
        async def tracked_callback(message: Message):
            await original_callback(message)
            callback_received.set()
        
        await client2.send_message(
            "echo: Hello from asynchronous callback route!",
            callback=tracked_callback,
            timeout=5
        )
        
        print("📨 Message sent, callback registered. Waiting for response...")
        
        # Wait for callback to be called
        try:
            await asyncio.wait_for(callback_received.wait(), timeout=3)
            print("✅ Callback was called successfully!")
        except asyncio.TimeoutError:
            print("❌ Callback timeout - no response received")
        
        print("\n" + "="*60)
        print("COMPARISON: MULTIPLE MESSAGES WITH DIFFERENT ROUTES")
        print("="*60)
        
        # Send multiple messages to demonstrate both routes working together
        print("\n🔀 Sending multiple messages using both routes...")
        
        # Create multiple callback events to track
        callback_events = []
        for i in range(3):
            event = asyncio.Event()
            callback_events.append(event)
            
            async def make_tracked_callback(idx, evt):
                async def tracked_cb(msg: Message):
                    print(f"📞 Async callback {idx}: {msg.content}")
                    evt.set()
                return tracked_cb
            
            callback = await make_tracked_callback(i, event)
            
            # Send with callback
            await client2.send_message(
                f"echo: Async message {i}",
                callback=callback
            )
        
        # Send with synchronous waiting
        sync_tasks = []
        for i in range(2):
            task = asyncio.create_task(
                client1.send_message(
                    f"echo: Sync message {i}",
                    wait_for_response=True,
                    timeout=5
                )
            )
            sync_tasks.append(task)
        
        # Wait for all callbacks
        print("⏳ Waiting for async callbacks...")
        for i, event in enumerate(callback_events):
            try:
                await asyncio.wait_for(event.wait(), timeout=3)
                print(f"✅ Async callback {i} completed")
            except asyncio.TimeoutError:
                print(f"❌ Async callback {i} timed out")
        
        # Wait for synchronous responses
        print("⏳ Waiting for sync responses...")
        for i, task in enumerate(sync_tasks):
            try:
                response = await task
                print(f"✅ Sync response {i}: {response.content}")
            except Exception as e:
                print(f"❌ Sync response {i} failed: {e}")
        
        print("\n" + "="*60)
        print("CALLBACK HANDLER STATISTICS")
        print("="*60)
        
        # Show callback handler stats
        stats1 = client1.message_handler.callback_handler.get_stats()
        stats2 = client2.message_handler.callback_handler.get_stats()
        
        print(f"\nClient1 callback stats: {stats1}")
        print(f"Client2 callback stats: {stats2}")
        
    except Exception as e:
        print(f"❌ Demo error: {e}")
        raise
    
    finally:
        # Cleanup
        print("\n🧹 Cleaning up...")
        
        try:
            await client1.disconnect()
            await client2.disconnect()
        except Exception:
            pass
        
        try:
            await server.stop()
            server_task.cancel()
        except Exception:
            pass
        
        print("✅ Demo completed!")


async def main():
    """
    Main demo function.
    """
    try:
        await demo_callback_system()
    except KeyboardInterrupt:
        print("\n⛔ Demo interrupted by user")
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
