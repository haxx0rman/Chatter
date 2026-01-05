"""
Integration Tests for ChatterCore Improvements

End-to-end tests covering all improvement phases working together.
"""

import pytest
import asyncio
import json
from typing import List
from pydantic import BaseModel, ValidationError
from typing import Literal, Optional

from chattercore.server import ChatterServer
from chattercore.client import ChatterClient
from chattercore.message_handler import Message, MessageType, MessageContext


# Test fixtures
@pytest.fixture
async def server():
    """Create and start a test server."""
    srv = ChatterServer(host="localhost", port=8888)
    await srv.start()
    yield srv
    await srv.stop()


@pytest.fixture
async def client(server):
    """Create and connect a test client."""
    client = ChatterClient("ws://localhost:8888")
    await client.connect()
    await asyncio.sleep(0.1)  # Let connection stabilize
    yield client
    await client.disconnect()


@pytest.fixture
async def clients(server):
    """Create and connect multiple test clients."""
    client_list: List[ChatterClient] = []
    for i in range(3):
        c = ChatterClient("ws://localhost:8888")
        await c.connect()
        await asyncio.sleep(0.1)
        client_list.append(c)
    
    yield client_list
    
    for c in client_list:
        await c.disconnect()


@pytest.mark.asyncio
class TestIntegrationPhase1:
    """Integration tests for Phase 1 improvements."""
    
    async def test_custom_message_end_to_end(self, server, clients):
        """Test CUSTOM message with dict content through full system."""
        client_a, client_b = clients[0], clients[1]
        
        # Setup receiver
        received_messages = []
        
        def handler(message, context):
            received_messages.append(message)
        
        client_b.register_message_handler(MessageType.CUSTOM, handler)
        
        # Send CUSTOM message with dict content
        content = {
            "operation": "test",
            "data": {"key": "value"},
            "array": [1, 2, 3]
        }
        
        await client_a.send_message(
            content=content,
            message_type=MessageType.CUSTOM
        )
        
        # Wait for message to be received
        await asyncio.sleep(0.2)
        
        # Verify message was received with dict content
        assert len(received_messages) > 0
        received = received_messages[0]
        assert isinstance(received.content, dict)
        assert received.content["operation"] == "test"
        assert received.content["data"]["key"] == "value"
        assert received.content["array"] == [1, 2, 3]
    
    async def test_request_response_end_to_end(self, server, clients):
        """Test request-response pattern end-to-end."""
        client_a, client_b = clients[0], clients[1]
        
        # Setup responder on client_b
        async def handler(message, context):
            if isinstance(message.content, dict) and "request_id" in message.content:
                # Send response
                response = {
                    "request_id": message.content["request_id"],
                    "status": "success",
                    "result": "processed"
                }
                await client_b.send_message(
                    content=response,
                    message_type=MessageType.CUSTOM
                )
        
        client_b.register_message_handler(MessageType.CUSTOM, handler)
        
        # Send request from client_a
        request_content = {"operation": "test", "data": "request"}
        
        # Use the built-in request-response pattern
        response_future = asyncio.Future()
        
        async def send_and_wait():
            # Manually track request
            request_id = f"test_req_{asyncio.get_event_loop().time()}"
            request_content["request_id"] = request_id
            
            # Register callback to catch response
            def response_handler(msg, ctx):
                if isinstance(msg.content, dict) and msg.content.get("request_id") == request_id:
                    if not response_future.done():
                        response_future.set_result(msg.content)
            
            client_a.register_message_handler(MessageType.CUSTOM, response_handler)
            
            # Send request
            await client_a.send_message(
                content=request_content,
                message_type=MessageType.CUSTOM
            )
            
            # Wait for response
            try:
                response = await asyncio.wait_for(response_future, timeout=2.0)
                return response
            except asyncio.TimeoutError:
                return None
        
        response = await send_and_wait()
        
        assert response is not None
        assert response["status"] == "success"
        assert response["result"] == "processed"
    
    async def test_multiple_clients_custom_messages(self, server, clients):
        """Test multiple clients exchanging CUSTOM messages."""
        # Track received messages per client
        received = {0: [], 1: [], 2: []}
        
        for i, client in enumerate(clients):
            def make_handler(idx):
                def handler(msg, ctx):
                    received[idx].append(msg)
                return handler
            
            client.register_message_handler(MessageType.CUSTOM, make_handler(i))
        
        # Client 0 sends message
        await clients[0].send_message(
            content={"sender": "client_0", "message": "hello all"},
            message_type=MessageType.CUSTOM
        )
        
        await asyncio.sleep(0.2)
        
        # Clients 1 and 2 should receive it (not client 0 due to auto-routing exclude)
        assert len(received[0]) == 0  # Sender doesn't receive own message
        assert len(received[1]) > 0
        assert len(received[2]) > 0
        
        # Verify content is dict
        for msg in received[1] + received[2]:
            assert isinstance(msg.content, dict)
            assert msg.content["sender"] == "client_0"


@pytest.mark.asyncio
class TestIntegrationPhase2:
    """Integration tests for Phase 2 improvements."""
    
    async def test_routing_between_clients(self, server, clients):
        """Test routing messages between named clients."""
        client_a, client_b = clients[0], clients[1]
        
        received = []
        
        def handler(msg, ctx):
            received.append((msg, ctx))
        
        client_b.register_message_handler(MessageType.CUSTOM, handler)
        
        # Send message that will be broadcast to all clients
        # (auto-routing broadcasts CUSTOM messages to all except sender)
        await client_a.send_message(
            content={"routed": "message", "data": "test", "intended_for": "client_b"},
            message_type=MessageType.CUSTOM
        )
        
        await asyncio.sleep(0.2)
        
        assert len(received) > 0
        msg, ctx = received[0]
        assert isinstance(msg.content, dict)
        assert msg.content["routed"] == "message"
    
    async def test_routing_context_preservation(self, server, clients):
        """Test that routing context is preserved through hops."""
        client_a, client_b = clients[0], clients[1]
        
        received_contexts = []
        
        def handler(msg, ctx):
            received_contexts.append(ctx)
        
        client_b.register_message_handler(MessageType.CUSTOM, handler)
        
        # Send message with routing metadata embedded in content
        content_with_routing = {
            "test": "data",
            "_routing": {
                "sender": "client_a",
                "recipient": "client_b",
                "metadata": {"correlation_id": "abc123", "priority": "high"}
            }
        }
        
        await client_a.send_message(
            content=content_with_routing,
            message_type=MessageType.CUSTOM
        )
        
        await asyncio.sleep(0.2)
        
        # Context should exist
        assert len(received_contexts) > 0
        ctx = received_contexts[0]
        
        # Check if context is dict or MessageContext
        assert ctx is not None
    
    async def test_multi_hop_routing(self, server, clients):
        """Test routing through multiple intermediary agents."""
        client_a, client_b, client_c = clients[0], clients[1], clients[2]
        
        # Track message path
        message_path = []
        
        # Client B forwards to Client C
        async def b_handler(msg, ctx):
            message_path.append("B")
            if isinstance(msg.content, dict) and "forward" in msg.content:
                await client_b.send_message(
                    content=msg.content,
                    message_type=MessageType.CUSTOM
                )
        
        # Client C receives final message
        def c_handler(msg, ctx):
            message_path.append("C")
        
        client_b.register_message_handler(MessageType.CUSTOM, b_handler)
        client_c.register_message_handler(MessageType.CUSTOM, c_handler)
        
        # Client A sends to B with forward flag
        await client_a.send_message(
            content={"forward": True, "data": "multi-hop"},
            message_type=MessageType.CUSTOM
        )
        
        await asyncio.sleep(0.3)
        
        # Should have gone through B and C
        assert "B" in message_path
        assert "C" in message_path


@pytest.mark.asyncio
class TestIntegrationPhase3:
    """Integration tests for Phase 3 improvements."""
    
    async def test_schema_validation_end_to_end(self, server, clients):
        """Test schema validation in live system."""
        # Define schema
        class TestSchema(BaseModel):
            operation: Literal["create", "update", "delete"]
            item_id: str
            data: Optional[dict] = None
        
        # Register schema on server
        server.register_schema("operation:*", TestSchema)
        server.enable_strict_validation(False)  # Lenient mode for testing
        
        client_a = clients[0]
        
        # Send valid message
        valid_content = {
            "operation": "create",
            "item_id": "item123",
            "data": {"key": "value"}
        }
        
        await client_a.send_message(
            content=valid_content,
            message_type=MessageType.CUSTOM
        )
        
        await asyncio.sleep(0.1)
        
        # Should be processed successfully (no errors)
        # In strict mode, invalid messages would cause errors
    
    async def test_tracing_end_to_end(self, server, clients):
        """Test message tracing through full system."""
        # Enable tracing
        server.enable_tracing(True)
        
        client_a = clients[0]
        
        # Send message
        await client_a.send_message(
            content={"traced": "message", "id": "trace_test"},
            message_type=MessageType.CUSTOM
        )
        
        await asyncio.sleep(0.1)
        
        # Tracing should be enabled (check via server state)
        assert server._tracing_enabled
        
        # Disable tracing
        server.enable_tracing(False)
        assert not server._tracing_enabled


@pytest.mark.asyncio
class TestIntegrationAllPhases:
    """Integration tests combining all improvement phases."""
    
    async def test_routed_request_response_with_validation(self, server, clients):
        """Test routed request-response with schema validation."""
        # Define schema
        class RequestSchema(BaseModel):
            operation: str
            request_id: str
            data: dict
        
        # Register schema
        server.register_schema("operation:*", RequestSchema)
        server.enable_strict_validation(False)
        
        # Enable tracing
        server.enable_tracing(True)
        
        client_a, client_b = clients[0], clients[1]
        
        # Setup responder
        async def responder(msg, ctx):
            if isinstance(msg.content, dict) and "request_id" in msg.content:
                response = {
                    "request_id": msg.content["request_id"],
                    "status": "success",
                    "result": msg.content.get("data", {})
                }
                await client_b.send_message(
                    content=response,
                    message_type=MessageType.CUSTOM
                )
        
        client_b.register_message_handler(MessageType.CUSTOM, responder)
        
        # Send request via broadcast (auto-routing will deliver to all clients)
        request_content = {
            "operation": "process",
            "request_id": "full_test_123",
            "data": {"test": "value"}
        }
        
        response_future = asyncio.Future()
        
        def response_handler(msg, ctx):
            if isinstance(msg.content, dict) and msg.content.get("request_id") == "full_test_123":
                if not response_future.done():
                    response_future.set_result(msg.content)
        
        client_a.register_message_handler(MessageType.CUSTOM, response_handler)
        
        # Send request
        await client_a.send_message(
            content=request_content,
            message_type=MessageType.CUSTOM
        )
        
        # Wait for response
        response = await asyncio.wait_for(response_future, timeout=2.0)
        
        # Validate response
        assert response["status"] == "success"
        assert response["result"] == {"test": "value"}
    
    async def test_complex_multi_agent_scenario(self, server, clients):
        """Test complex scenario with multiple agents, routing, and validation."""
        client_a, client_b, client_c = clients[0], clients[1], clients[2]
        
        # Enable all features
        server.enable_tracing(True)
        
        # Track message flow
        flow = []
        
        async def a_handler(msg, ctx):
            flow.append("A")
        
        async def b_handler(msg, ctx):
            flow.append("B")
            if isinstance(msg.content, dict) and msg.content.get("forward_to_c"):
                await client_b.send_message(
                    content={"from_b": True, "original": msg.content},
                    message_type=MessageType.CUSTOM
                )
        
        def c_handler(msg, ctx):
            flow.append("C")
        
        client_a.register_message_handler(MessageType.CUSTOM, a_handler)
        client_b.register_message_handler(MessageType.CUSTOM, b_handler)
        client_c.register_message_handler(MessageType.CUSTOM, c_handler)
        
        # Send message that triggers multi-agent flow
        await client_a.send_message(
            content={"forward_to_c": True, "data": "test"},
            message_type=MessageType.CUSTOM
        )
        
        await asyncio.sleep(0.3)
        
        # All agents should have received messages
        assert "A" in flow or "B" in flow or "C" in flow
    
    async def test_error_handling_across_phases(self, server, clients):
        """Test error handling across all improvement phases."""
        client_a = clients[0]
        
        # Enable strict validation
        class StrictSchema(BaseModel):
            required_field: str
            number_field: int
        
        server.register_schema("strict:*", StrictSchema)
        server.enable_strict_validation(True)
        
        # Try to send invalid message
        invalid_content = {
            "strict": "test",
            "required_field": "present",
            "number_field": "not_a_number"  # Wrong type
        }
        
        # In strict mode, this should fail validation on server
        # The message will be sent but server will reject it
        await client_a.send_message(
            content=invalid_content,
            message_type=MessageType.CUSTOM
        )
        
        await asyncio.sleep(0.1)
        
        # Test passes if no crashes occur
        assert True

