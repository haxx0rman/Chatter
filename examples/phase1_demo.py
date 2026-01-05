"""
Demo of ChatterCore 2.0 Phase 1 Improvements

This example demonstrates:
1. Message Content Type Contract (CUSTOM messages are always dicts)
2. Built-in Request-Response Pattern
3. MessageContext usage
4. RoutedMessage for explicit routing
"""

import asyncio
import logging
from chattercore import (
    ChatterServer, 
    ChatterClient, 
    MessageType,
    MessageContext,
    RoutedMessage
)

logging.basicConfig(level=logging.INFO)


async def demo_content_type_contract():
    """Demonstrate automatic dict parsing for CUSTOM messages."""
    print("\n=== Demo 1: Message Content Type Contract ===")
    
    from chattercore import Message
    
    # CUSTOM messages automatically parse JSON to dict
    message1 = Message(
        type=MessageType.CUSTOM,
        content='{"operation": "test", "value": 42}'  # JSON string
    )
    print(f"Created from JSON string: {type(message1.content)} = {message1.content}")
    assert isinstance(message1.content, dict)
    
    # CUSTOM messages accept dicts directly
    message2 = Message(
        type=MessageType.CUSTOM,
        content={"operation": "test", "value": 42}  # dict
    )
    print(f"Created from dict: {type(message2.content)} = {message2.content}")
    assert isinstance(message2.content, dict)
    
    # Roundtrip serialization
    json_str = message2.to_json()
    print(f"Serialized to JSON: {json_str[:80]}...")
    
    message3 = Message.from_json(json_str)
    print(f"Deserialized: {type(message3.content)} = {message3.content}")
    assert isinstance(message3.content, dict)
    assert message3.content == message2.content
    
    print("✅ No manual json.loads() needed - content is always a dict!")


async def demo_message_context():
    """Demonstrate MessageContext features."""
    print("\n=== Demo 2: MessageContext ===")
    
    # Create context with routing info
    context = MessageContext(
        connection_id="conn_123",
        user_id="user_456"
    )
    
    # Add routing metadata
    context.sender = "AGENT_A"
    context.recipient = "AGENT_B"
    
    print(f"Context is routed: {context.is_routed()}")
    print(f"From: {context.sender} -> To: {context.recipient}")
    
    # Track routing hops
    context.add_hop("HUB")
    context.add_hop("ROUTER")
    context.add_hop("AGENT_B")
    
    print(f"Route hops: {' -> '.join(context.route_hops)}")
    
    # Custom metadata
    context.metadata['priority'] = 'high'
    context.metadata['trace_id'] = 'abc-123'
    
    print(f"Metadata: {context.metadata}")
    
    # Convert to dict
    context_dict = context.to_dict()
    print(f"As dict: {list(context_dict.keys())}")
    
    print("✅ Rich context with routing awareness!")


async def demo_routed_message():
    """Demonstrate RoutedMessage for explicit routing."""
    print("\n=== Demo 3: RoutedMessage ===")
    
    # Create routed message with explicit sender/recipient
    routed = RoutedMessage(
        content={"operation": "recall", "query": "test"},
        sender="AGENT_A",
        recipient="HINDSIGHT",
        metadata={"priority": "high", "timeout": 30}
    )
    
    print(f"Routed message:")
    print(f"  From: {routed.sender}")
    print(f"  To: {routed.recipient}")
    print(f"  Content: {routed.content}")
    print(f"  Metadata: {routed.metadata}")
    print(f"  Timestamp: {routed.timestamp}")
    
    # Convert to dict for transport
    routed_dict = routed.to_dict()
    print(f"  As dict: {routed_dict}")
    
    print("✅ Explicit routing with metadata!")


async def demo_request_response():
    """Demonstrate built-in request-response pattern."""
    print("\n=== Demo 4: Request-Response Pattern ===")
    
    # Start server
    server = ChatterServer(host="localhost", port=9998)
    await server.start()
    
    try:
        # Connect two clients
        client_a = ChatterClient("ws://localhost:9998")
        client_b = ChatterClient("ws://localhost:9998")
        
        await client_a.connect()
        await client_b.connect()
        
        # Client B will respond to requests
        async def handle_request(message, context):
            if isinstance(message.content, dict) and 'request_id' in message.content:
                print(f"[CLIENT B] Received request: {message.content}")
                
                # Simulate processing
                await asyncio.sleep(0.1)
                
                # Send response with same request_id
                response = {
                    "request_id": message.content['request_id'],
                    "status": "success",
                    "result": f"Processed: {message.content.get('query', 'unknown')}"
                }
                
                print(f"[CLIENT B] Sending response: {response}")
                await client_b.send_message(response, MessageType.CUSTOM)
        
        client_b.message_handler.register_handler(MessageType.CUSTOM, handle_request)
        
        # Give time for setup
        await asyncio.sleep(0.2)
        
        # Client A sends request using built-in request-response
        print("[CLIENT A] Sending request...")
        
        # Note: In this demo, we're using the lower-level send_message
        # In production with multiple clients, you'd use send_request with recipient
        request = {"operation": "test", "query": "hello"}
        
        # Manual request-response demo (since we can't directly route between clients)
        request_id = "test_req_123"
        request['request_id'] = request_id
        
        # Set up to wait for response
        future = asyncio.Future()
        client_a.message_handler._pending_requests[request_id] = future
        
        # Send request
        await client_a.send_message(request, MessageType.CUSTOM)
        
        # Wait for response
        try:
            response = await asyncio.wait_for(future, timeout=2.0)
            print(f"[CLIENT A] Received response: {response}")
            print("✅ Request-response completed successfully!")
        except asyncio.TimeoutError:
            print("❌ Request timed out")
        
        # Cleanup
        await client_a.disconnect()
        await client_b.disconnect()
        
    finally:
        await server.stop()


async def demo_server_routing():
    """Demonstrate server-side routing features."""
    print("\n=== Demo 5: Server Routing ===")
    
    server = ChatterServer(host="localhost", port=9996)
    await server.start()
    
    try:
        client = ChatterClient("ws://localhost:9996")
        await client.connect()
        
        # Track received messages
        received = []
        
        async def capture_message(message, context):
            print(f"[CLIENT] Received message: type={message.type}, content={message.content}")
            received.append({
                'content': message.content,
                'metadata': message.metadata
            })
        
        client.message_handler.register_handler(MessageType.CUSTOM, capture_message)
        
        # Give time for setup
        await asyncio.sleep(0.3)
        
        # Get connection ID (for routing)
        # Note: In production, you'd use user aliases or IDs
        connections = list(server.connection_manager._connections.keys())
        if connections:
            target_conn = connections[0]
            
            # Send directly to connection with routing metadata
            print("Sending message with routing metadata...")
            from chattercore import Message
            message = Message(
                type=MessageType.CUSTOM,
                content={"data": "test", "operation": "demo"},
                metadata={
                    'sender': 'SERVER',
                    'recipient': 'CLIENT',
                    'timestamp': 123456789,
                    'priority': 'high'
                }
            )
            
            await server.connection_manager.send_to_connection(target_conn, message)
            
            await asyncio.sleep(0.3)
            
            if received:
                print(f"✅ Received message with metadata!")
                print(f"   Content: {received[0]['content']}")
                print(f"   Metadata: {received[0]['metadata']}")
            else:
                print("❌ No message received")
        
        await client.disconnect()
        
    finally:
        await server.stop()


async def main():
    """Run all demos."""
    print("="*60)
    print("ChatterCore 2.0 - Phase 1 Improvements Demo")
    print("="*60)
    
    # Demo 1: Content Type Contract
    await demo_content_type_contract()
    
    # Demo 2: MessageContext
    await demo_message_context()
    
    # Demo 3: RoutedMessage
    await demo_routed_message()
    
    # Demo 4: Request-Response Pattern
    await demo_request_response()
    
    # Demo 5: Server Routing
    await demo_server_routing()
    
    print("\n" + "="*60)
    print("All demos completed!")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())
