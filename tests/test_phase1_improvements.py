"""
Tests for Phase 1 improvements: Message Content Type Contract and Request-Response Pattern
"""

import pytest
import asyncio
import json
from chattercore import (
    MessageHandler, 
    Message, 
    MessageType, 
    MessageContext,
    RoutedMessage,
    ChatterServer,
    ChatterClient
)


class TestMessageContentTypeContract:
    """Test the message content type contract for automatic serialization."""
    
    def test_custom_message_dict_content(self):
        """CUSTOM messages with dict content should be preserved."""
        content = {"operation": "test", "data": "value"}
        message = Message(type=MessageType.CUSTOM, content=content)
        
        assert isinstance(message.content, dict)
        assert message.content == content
    
    def test_custom_message_json_string_content(self):
        """CUSTOM messages with JSON string content should be parsed to dict."""
        json_str = '{"operation": "test", "data": "value"}'
        message = Message(type=MessageType.CUSTOM, content=json_str)
        
        assert isinstance(message.content, dict)
        assert message.content["operation"] == "test"
    
    def test_custom_message_invalid_json_raises(self):
        """CUSTOM messages with invalid JSON should raise error."""
        with pytest.raises(ValueError, match="CUSTOM message content must be valid JSON"):
            Message(type=MessageType.CUSTOM, content="not json")
    
    def test_custom_message_non_dict_raises(self):
        """CUSTOM messages with non-dict content should raise error."""
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            Message(type=MessageType.CUSTOM, content=123)
    
    def test_text_message_string_content(self):
        """TEXT messages must have string content."""
        message = Message(type=MessageType.TEXT, content="Hello")
        assert isinstance(message.content, str)
        assert message.content == "Hello"
    
    def test_text_message_non_string_raises(self):
        """TEXT messages with non-string content should raise error."""
        with pytest.raises(ValueError, match="TEXT message content must be string"):
            Message(type=MessageType.TEXT, content={"key": "value"})
    
    def test_custom_message_to_dict_serializes(self):
        """CUSTOM message to_dict should serialize content for transport."""
        content = {"operation": "test", "data": "value"}
        message = Message(type=MessageType.CUSTOM, content=content)
        
        result = message.to_dict()
        # Content should be JSON string in transport format
        assert isinstance(result['content'], str)
        assert json.loads(result['content']) == content
    
    def test_custom_message_from_dict_deserializes(self):
        """CUSTOM message from_dict should deserialize content."""
        json_content = '{"operation": "test", "data": "value"}'
        data = {
            'type': 'custom',
            'content': json_content,
            'sender_id': 'test'
        }
        
        message = Message.from_dict(data)
        assert isinstance(message.content, dict)
        assert message.content["operation"] == "test"
    
    def test_custom_message_roundtrip(self):
        """CUSTOM message should survive serialization roundtrip."""
        original_content = {"operation": "recall", "query": "test", "agent_id": "AGENT_A"}
        message1 = Message(type=MessageType.CUSTOM, content=original_content)
        
        # Serialize for transport
        json_str = message1.to_json()
        
        # Deserialize on receiving end
        message2 = Message.from_json(json_str)
        
        # Content should be dict on both ends
        assert isinstance(message1.content, dict)
        assert isinstance(message2.content, dict)
        assert message2.content == original_content


class TestMessageContext:
    """Test the enhanced MessageContext class."""
    
    def test_create_message_context(self):
        """Create a MessageContext with basic parameters."""
        context = MessageContext(
            connection_id="conn123",
            user_id="user456"
        )
        
        assert context.connection_id == "conn123"
        assert context.user_id == "user456"
        assert isinstance(context.timestamp, float)
        assert context.metadata == {}
    
    def test_message_context_routing_info(self):
        """MessageContext should track routing information."""
        context = MessageContext()
        context.sender = "AGENT_A"
        context.recipient = "AGENT_B"
        
        assert context.is_routed()
        assert context.sender == "AGENT_A"
        assert context.recipient == "AGENT_B"
    
    def test_message_context_hop_tracking(self):
        """MessageContext should track routing hops."""
        context = MessageContext()
        context.add_hop("HUB")
        context.add_hop("AGENT_A")
        context.add_hop("AGENT_B")
        
        assert len(context.route_hops) == 3
        assert context.route_hops == ["HUB", "AGENT_A", "AGENT_B"]
    
    def test_message_context_metadata(self):
        """MessageContext should support custom metadata."""
        context = MessageContext()
        context.metadata['priority'] = 'high'
        context.metadata['request_id'] = '12345'
        
        assert context.metadata['priority'] == 'high'
        assert context.metadata['request_id'] == '12345'
    
    def test_message_context_to_dict(self):
        """MessageContext should convert to dict."""
        context = MessageContext(connection_id="conn123")
        context.sender = "AGENT_A"
        context.add_hop("HUB")
        
        result = context.to_dict()
        assert result['connection_id'] == "conn123"
        assert result['sender'] == "AGENT_A"
        assert "HUB" in result['route_hops']


class TestRoutedMessage:
    """Test the RoutedMessage class."""
    
    def test_create_routed_message(self):
        """Create a RoutedMessage with routing info."""
        routed = RoutedMessage(
            content={"data": "test"},
            sender="AGENT_A",
            recipient="AGENT_B"
        )
        
        assert routed.sender == "AGENT_A"
        assert routed.recipient == "AGENT_B"
        assert routed.content == {"data": "test"}
        assert isinstance(routed.timestamp, float)
    
    def test_routed_message_with_metadata(self):
        """RoutedMessage should support metadata."""
        routed = RoutedMessage(
            content={"data": "test"},
            sender="AGENT_A",
            recipient="AGENT_B",
            metadata={"priority": "high"}
        )
        
        assert routed.metadata["priority"] == "high"
    
    def test_routed_message_to_dict(self):
        """RoutedMessage should convert to dict."""
        routed = RoutedMessage(
            content={"data": "test"},
            sender="AGENT_A",
            recipient="AGENT_B"
        )
        
        result = routed.to_dict()
        assert result['sender'] == "AGENT_A"
        assert result['recipient'] == "AGENT_B"
        assert result['content'] == {"data": "test"}
        assert 'timestamp' in result


class TestRequestResponsePattern:
    """Test the built-in request-response pattern."""
    
    @pytest.mark.asyncio
    async def test_send_request_basic(self):
        """Test basic request-response pattern."""
        handler = MessageHandler()
        
        # Simulate sending
        sent_messages = []
        async def send_callback(content, msg_type, recipient):
            sent_messages.append({
                'content': content,
                'type': msg_type,
                'recipient': recipient
            })
        
        # Start request in background
        async def send_req():
            return await handler.send_request(
                content={"operation": "test"},
                recipient="TARGET",
                sender_id="SENDER",
                timeout=1.0,
                send_callback=send_callback
            )
        
        request_task = asyncio.create_task(send_req())
        
        # Give it time to send
        await asyncio.sleep(0.1)
        
        # Verify request was sent
        assert len(sent_messages) == 1
        assert 'request_id' in sent_messages[0]['content']
        request_id = sent_messages[0]['content']['request_id']
        
        # Simulate response
        response_content = {"status": "success", "request_id": request_id}
        await handler.send_response(request_id, response_content)
        
        # Request should complete
        result = await request_task
        assert result["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_send_request_timeout(self):
        """Test request timeout."""
        handler = MessageHandler()
        
        async def send_callback(content, msg_type, recipient):
            pass  # Don't respond
        
        with pytest.raises(TimeoutError, match="timed out"):
            await handler.send_request(
                content={"operation": "test"},
                recipient="TARGET",
                sender_id="SENDER",
                timeout=0.1,
                send_callback=send_callback
            )
    
    @pytest.mark.asyncio
    async def test_process_message_handles_response(self):
        """Test that process_message handles responses automatically."""
        handler = MessageHandler()
        
        # Set up pending request
        request_id = "test_req_123"
        future = asyncio.Future()
        handler._pending_requests[request_id] = future
        
        # Create response message
        response = Message(
            type=MessageType.CUSTOM,
            content={"status": "ok", "request_id": request_id}
        )
        
        # Process should complete the future
        await handler.process_message(response, None)
        
        assert future.done()
        assert future.result()["status"] == "ok"


@pytest.mark.asyncio
async def test_integration_request_response():
    """Integration test for request-response between client and server."""
    # Start server
    server = ChatterServer(host="localhost", port=9999)
    await server.start()
    
    try:
        # Connect client
        client = ChatterClient("ws://localhost:9999")
        await client.connect()
        
        # Register a handler on client to respond to requests
        responses_received = []
        
        async def handle_custom(message, context):
            if isinstance(message.content, dict) and 'request_id' in message.content:
                # This is a request - send response
                responses_received.append(message.content)
                response = {
                    "status": "success",
                    "request_id": message.content["request_id"],
                    "result": "done"
                }
                await client.send_message(response, MessageType.CUSTOM)
        
        client.message_handler.register_handler(MessageType.CUSTOM, handle_custom)
        
        # Give time for setup
        await asyncio.sleep(0.2)
        
        # Note: This test demonstrates the pattern but can't fully test
        # without multiple clients. The client can't send_request to itself.
        # In production, this would work between different clients.
        
    finally:
        await client.disconnect()
        await server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
