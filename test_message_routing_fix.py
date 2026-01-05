#!/usr/bin/env python3
"""Test script to verify message routing fix for ChatterCore."""
import asyncio
import logging
from chattercore import ChatterServer, ChatterClient, MessageType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def test_basic_message_routing():
    """Test basic message routing between clients."""
    print("\n=== Test 1: Basic Message Routing ===")
    receiver_messages = []
    
    # Start server with auto_route_messages=True (default)
    server = ChatterServer(host="localhost", port=8765)
    await server.start()
    
    # Create receiver client with message handler
    receiver = ChatterClient("ws://localhost:8765")
    
    async def on_custom_message(message, context):
        print(f"🎉 RECEIVER GOT MESSAGE: {message.content}")
        receiver_messages.append(message.content)
    
    receiver.register_message_handler(MessageType.CUSTOM, on_custom_message)
    await receiver.connect()
    
    # Create sender client
    sender = ChatterClient("ws://localhost:8765")
    await sender.connect()
    
    await asyncio.sleep(1)
    
    # Send a CUSTOM message
    print("📤 Sending CUSTOM message from sender...")
    await sender.send_message(
        {"test": "hello", "number": 42},
        MessageType.CUSTOM
    )
    
    await asyncio.sleep(2)
    
    # Check results
    success = False
    if receiver_messages:
        print(f"✅ Test 1 PASSED: Message received! Content: {receiver_messages[0]}")
        success = True
    else:
        print("❌ Test 1 FAILED: Message NOT received by other client")
    
    # Cleanup
    await sender.disconnect()
    await receiver.disconnect()
    await server.stop()
    
    return success


async def test_text_message_routing():
    """Test TEXT message routing between clients."""
    print("\n=== Test 2: TEXT Message Routing ===")
    receiver_messages = []
    
    # Start server
    server = ChatterServer(host="localhost", port=8766)
    await server.start()
    
    # Create receiver client
    receiver = ChatterClient("ws://localhost:8766")
    
    async def on_text_message(message, context):
        print(f"🎉 RECEIVER GOT TEXT: {message.content}")
        receiver_messages.append(message.content)
    
    receiver.register_message_handler(MessageType.TEXT, on_text_message)
    await receiver.connect()
    
    # Create sender client
    sender = ChatterClient("ws://localhost:8766")
    await sender.connect()
    
    await asyncio.sleep(1)
    
    # Send a TEXT message
    print("📤 Sending TEXT message from sender...")
    await sender.send_message("Hello, World!", MessageType.TEXT)
    
    await asyncio.sleep(2)
    
    # Check results
    success = False
    if receiver_messages:
        print(f"✅ Test 2 PASSED: Message received! Content: {receiver_messages[0]}")
        success = True
    else:
        print("❌ Test 2 FAILED: Message NOT received by other client")
    
    # Cleanup
    await sender.disconnect()
    await receiver.disconnect()
    await server.stop()
    
    return success


async def test_multiple_receivers():
    """Test broadcasting to multiple receivers."""
    print("\n=== Test 3: Multiple Receivers ===")
    receiver1_messages = []
    receiver2_messages = []
    
    # Start server
    server = ChatterServer(host="localhost", port=8767)
    await server.start()
    
    # Create receiver clients
    receiver1 = ChatterClient("ws://localhost:8767")
    receiver2 = ChatterClient("ws://localhost:8767")
    
    async def on_receiver1_message(message, context):
        print(f"🎉 RECEIVER 1 GOT: {message.content}")
        receiver1_messages.append(message.content)
    
    async def on_receiver2_message(message, context):
        print(f"🎉 RECEIVER 2 GOT: {message.content}")
        receiver2_messages.append(message.content)
    
    receiver1.register_message_handler(MessageType.CUSTOM, on_receiver1_message)
    receiver2.register_message_handler(MessageType.CUSTOM, on_receiver2_message)
    
    await receiver1.connect()
    await receiver2.connect()
    
    # Create sender client
    sender = ChatterClient("ws://localhost:8767")
    await sender.connect()
    
    await asyncio.sleep(1)
    
    # Send message
    print("📤 Sending message to multiple receivers...")
    await sender.send_message({"broadcast": "test"}, MessageType.CUSTOM)
    
    await asyncio.sleep(2)
    
    # Check results
    success = False
    if receiver1_messages and receiver2_messages:
        print(f"✅ Test 3 PASSED: Both receivers got the message!")
        success = True
    elif receiver1_messages or receiver2_messages:
        print("⚠️ Test 3 PARTIAL: Only one receiver got the message")
    else:
        print("❌ Test 3 FAILED: No receivers got the message")
    
    # Cleanup
    await sender.disconnect()
    await receiver1.disconnect()
    await receiver2.disconnect()
    await server.stop()
    
    return success


async def test_sender_exclusion():
    """Test that sender doesn't receive their own message."""
    print("\n=== Test 4: Sender Exclusion ===")
    sender_messages = []
    receiver_messages = []
    
    # Start server
    server = ChatterServer(host="localhost", port=8768)
    await server.start()
    
    # Create clients
    sender = ChatterClient("ws://localhost:8768")
    receiver = ChatterClient("ws://localhost:8768")
    
    async def on_sender_message(message, context):
        print(f"⚠️ SENDER received own message: {message.content}")
        sender_messages.append(message.content)
    
    async def on_receiver_message(message, context):
        print(f"🎉 RECEIVER got message: {message.content}")
        receiver_messages.append(message.content)
    
    sender.register_message_handler(MessageType.CUSTOM, on_sender_message)
    receiver.register_message_handler(MessageType.CUSTOM, on_receiver_message)
    
    await sender.connect()
    await receiver.connect()
    
    await asyncio.sleep(1)
    
    # Send message from sender
    print("📤 Sending message from sender...")
    await sender.send_message({"test": "exclusion"}, MessageType.CUSTOM)
    
    await asyncio.sleep(2)
    
    # Check results
    success = False
    if receiver_messages and not sender_messages:
        print(f"✅ Test 4 PASSED: Receiver got message, sender excluded!")
        success = True
    elif sender_messages:
        print("❌ Test 4 FAILED: Sender received own message (should be excluded)")
    else:
        print("❌ Test 4 FAILED: No messages received")
    
    # Cleanup
    await sender.disconnect()
    await receiver.disconnect()
    await server.stop()
    
    return success


async def test_auto_route_disabled():
    """Test that auto_route_messages=False prevents routing."""
    print("\n=== Test 5: Auto-Route Disabled ===")
    receiver_messages = []
    
    # Start server with auto_route_messages=False
    server = ChatterServer(host="localhost", port=8769, auto_route_messages=False)
    await server.start()
    
    # Create clients
    receiver = ChatterClient("ws://localhost:8769")
    
    async def on_message(message, context):
        print(f"⚠️ RECEIVER got message (should not happen): {message.content}")
        receiver_messages.append(message.content)
    
    receiver.register_message_handler(MessageType.CUSTOM, on_message)
    await receiver.connect()
    
    sender = ChatterClient("ws://localhost:8769")
    await sender.connect()
    
    await asyncio.sleep(1)
    
    # Send message
    print("📤 Sending message with auto-route disabled...")
    await sender.send_message({"test": "no route"}, MessageType.CUSTOM)
    
    await asyncio.sleep(2)
    
    # Check results
    success = False
    if not receiver_messages:
        print(f"✅ Test 5 PASSED: Message not routed (as expected)")
        success = True
    else:
        print("❌ Test 5 FAILED: Message was routed despite auto_route_messages=False")
    
    # Cleanup
    await sender.disconnect()
    await receiver.disconnect()
    await server.stop()
    
    return success


async def main():
    """Run all tests."""
    print("=" * 60)
    print("ChatterCore Message Routing Fix - Test Suite")
    print("=" * 60)
    
    results = []
    
    try:
        results.append(await test_basic_message_routing())
        await asyncio.sleep(1)
        
        results.append(await test_text_message_routing())
        await asyncio.sleep(1)
        
        results.append(await test_multiple_receivers())
        await asyncio.sleep(1)
        
        results.append(await test_sender_exclusion())
        await asyncio.sleep(1)
        
        results.append(await test_auto_route_disabled())
        
    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("✅ All tests passed!")
        return True
    else:
        print(f"❌ {total - passed} test(s) failed")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
