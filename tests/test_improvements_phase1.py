"""
Tests for ChatterCore Phase 1 Improvements

This module tests:
1. Message Content Type Contract - ensuring CUSTOM messages are always dicts
2. Request-Response Pattern - built-in request-response support
"""

import pytest
import asyncio
import json
from pydantic import ValidationError
from chattercore.message_handler import Message, MessageType, MessageHandler
from chattercore.exceptions import ValidationException, TimeoutException
from chattercore.client import ChatterClient
from chattercore.server import ChatterServer


class TestMessageContentTypeContract:
    """Test the message content type contract improvements."""
    
    def test_custom_message_dict_content(self):
        """CUSTOM messages with dict content should remain as dicts."""
        message = Message(
            type=MessageType.CUSTOM,
            content={"operation": "test", "data": {"key": "value"}}
        )
        
        assert isinstance(message.content, dict)
        assert message.content["operation"] == "test"
        assert message.content["data"]["key"] == "value"
    
    def test_custom_message_json_string_auto_parse(self):
        """CUSTOM messages with JSON strings should auto-parse to dicts."""
        message = Message(
            type=MessageType.CUSTOM,
            content='{"operation": "test", "data": {"key": "value"}}'
        )
        
        # Should be automatically parsed to dict
        assert isinstance(message.content, dict)
        assert message.content["operation"] == "test"
        assert message.content["data"]["key"] == "value"
    
    def test_custom_message_invalid_json_raises_error(self):
        """CUSTOM messages with invalid JSON should raise ValidationException."""
        with pytest.raises(ValueError, match="CUSTOM message content must be valid JSON"):
            Message(
                type=MessageType.CUSTOM,
                content='not valid json{'
            )
    
    def test_custom_message_non_dict_raises_error(self):
        """CUSTOM messages with non-dict content should raise ValidationException."""
        with pytest.raises((ValueError, ValidationError)):
            Message(
                type=MessageType.CUSTOM,
                content="plain string"
            )
        
        with pytest.raises((ValueError, ValidationError)):
            Message(
                type=MessageType.CUSTOM,
                content=123
            )
    
    def test_text_message_string_content(self):
        """TEXT messages must have string content."""
        message = Message(
            type=MessageType.TEXT,
            content="Hello, World!"
        )
        
        assert isinstance(message.content, str)
        assert message.content == "Hello, World!"
    
    def test_text_message_non_string_raises_error(self):
        """TEXT messages with non-string content should raise ValidationException."""
        with pytest.raises(ValueError, match="TEXT message content must be string"):
            Message(
                type=MessageType.TEXT,
                content={"not": "a string"}
            )
    
    def test_custom_message_serialization(self):
        """CUSTOM messages should serialize to JSON string for transport."""
        message = Message(
            type=MessageType.CUSTOM,
            content={"operation": "test", "data": "value"}
        )
        
        # to_dict should serialize content for wire transport
        msg_dict = message.to_dict()
        assert isinstance(msg_dict['content'], str)  # Should be JSON string for transport
        assert '"operation"' in msg_dict['content']
        
        # to_json should produce valid JSON
        json_str = message.to_json()
        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed['type'] == 'custom'
    
    def test_custom_message_deserialization(self):
        """CUSTOM messages should deserialize JSON strings back to dicts."""
        # Simulate receiving a message from the wire
        json_data = {
            'type': 'custom',
            'content': '{"operation": "test", "data": "value"}'  # JSON string as received
        }
        
        message = Message.from_dict(json_data)
        
        # Should be auto-parsed to dict
        assert isinstance(message.content, dict)
        assert message.content["operation"] == "test"
        assert message.content["data"] == "value"
    
    def test_custom_message_roundtrip(self):
        """CUSTOM messages should maintain dict type through serialization roundtrip."""
        original = Message(
            type=MessageType.CUSTOM,
            content={"operation": "test", "nested": {"key": "value"}}
        )
        
        # Serialize to JSON (as sent over wire)
        json_str = original.to_json()
        
        # Deserialize from JSON (as received)
        reconstructed = Message.from_json(json_str)
        
        # Content should be dict on both ends
        assert isinstance(reconstructed.content, dict)
        assert reconstructed.content == original.content
    
    def test_system_messages_allow_string_content(self):
        """System messages should allow string content."""
        message = Message(
            type=MessageType.SYSTEM,
            content="System notification"
        )
        
        assert isinstance(message.content, str)
        assert message.content == "System notification"
    
    def test_error_messages_allow_string_content(self):
        """Error messages should allow string content."""
        message = Message(
            type=MessageType.ERROR,
            content="Error occurred"
        )
        
        assert isinstance(message.content, str)
        assert message.content == "Error occurred"


class TestRequestResponsePattern:
    """Test the built-in request-response pattern."""
    
    @pytest.mark.asyncio
    async def test_send_request_auto_id_generation(self):
        """Test automatic request ID generation."""
        handler = MessageHandler()
        
        # Mock send callback
        sent_messages = []
        async def mock_send(content, msg_type, recipient):
            sent_messages.append((content, msg_type, recipient))
        
        # Start request but don't wait for it
        request_task = asyncio.create_task(
            handler.send_request(
                content={"operation": "test"},
                recipient="AGENT_B",
                sender_id="AGENT_A",
                timeout=0.5,
                send_callback=mock_send
            )
        )
        
        # Give it time to send
        await asyncio.sleep(0.1)
        
        # Check that message was sent with auto-generated request_id
        assert len(sent_messages) == 1
        content, msg_type, recipient = sent_messages[0]
        assert "request_id" in content
        assert content["request_id"].startswith("AGENT_A_")
        assert msg_type == MessageType.CUSTOM
        assert recipient == "AGENT_B"
        
        # Cancel the request task
        request_task.cancel()
        try:
            await request_task
        except asyncio.CancelledError:
            pass
    
    @pytest.mark.asyncio
    async def test_send_response_routing(self):
        """Test response routing back to requester."""
        handler = MessageHandler()
        
        # Create a pending request
        request_id = "test_request_123"
        handler._pending_requests[request_id] = asyncio.Future()
        handler._request_senders[request_id] = "AGENT_A"
        
        # Send response
        response_content = {"status": "success", "data": "result"}
        await handler.send_response(request_id, response_content)
        
        # Future should be completed
        assert request_id not in handler._pending_requests
        assert request_id not in handler._request_senders
    
    @pytest.mark.asyncio
    async def test_pending_request_cleanup(self):
        """Test cleanup of pending requests on timeout."""
        handler = MessageHandler()
        
        # Mock send callback
        async def mock_send(content, msg_type, recipient):
            pass
        
        # Send request with very short timeout
        with pytest.raises((asyncio.TimeoutError, TimeoutException)):
            await handler.send_request(
                content={"operation": "test"},
                recipient="AGENT_B",
                sender_id="AGENT_A",
                timeout=0.1,
                send_callback=mock_send
            )
        
        # Request should be cleaned up
        assert len(handler._pending_requests) == 0
        assert len(handler._request_senders) == 0
    
    @pytest.mark.asyncio
    async def test_multiple_concurrent_requests(self):
        """Test handling multiple concurrent requests."""
        handler = MessageHandler()
        
        sent_messages = []
        async def mock_send(content, msg_type, recipient):
            sent_messages.append(content["request_id"])
        
        # Start multiple requests concurrently
        tasks = []
        for i in range(5):
            task = asyncio.create_task(
                handler.send_request(
                    content={"operation": f"test_{i}"},
                    recipient=f"AGENT_{i}",
                    sender_id="SENDER",
                    timeout=1.0,
                    send_callback=mock_send
                )
            )
            tasks.append(task)
        
        # Give them time to send
        await asyncio.sleep(0.1)
        
        # All messages should be sent with unique request IDs
        assert len(sent_messages) == 5
        assert len(set(sent_messages)) == 5  # All unique
        
        # All should start with sender ID
        for req_id in sent_messages:
            assert req_id.startswith("SENDER_")
        
        # Cancel all tasks
        for task in tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    
    @pytest.mark.asyncio
    async def test_send_request_basic(self):
        """Test sending a request and receiving a response."""
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
        """Test request timeout handling."""
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


class TestMessageHandlerCallbacks:
    """Test callback-based message handling."""
    
    @pytest.mark.asyncio
    async def test_wait_for_response(self):
        """Test waiting for a response to a specific message."""
        from chattercore.message_handler import CallbackHandler
        
        handler = CallbackHandler()
        message_id = "test-message-123"
        
        # Start waiting for response
        wait_task = asyncio.create_task(
            handler.wait_for_response(message_id, timeout=2.0)
        )
        
        # Simulate response after short delay
        await asyncio.sleep(0.1)
        response = Message(
            type=MessageType.CUSTOM,
            content={"status": "success"},
            reply_to=message_id
        )
        handler.deliver_response(message_id, response)
        
        # Should receive the response
        result = await wait_task
        assert result == response
        assert result.content["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_wait_for_response_timeout(self):
        """Test timeout when no response is received."""
        from chattercore.message_handler import CallbackHandler
        
        handler = CallbackHandler()
        message_id = "test-message-timeout"
        
        # Should timeout after 0.5 seconds
        with pytest.raises(asyncio.TimeoutError):
            await handler.wait_for_response(message_id, timeout=0.5)
    
    @pytest.mark.asyncio
    async def test_register_callback(self):
        """Test registering a callback for response."""
        from chattercore.message_handler import CallbackHandler
        
        handler = CallbackHandler()
        message_id = "test-callback-123"
        received_response = None
        
        # Define callback
        def callback(response):
            nonlocal received_response
            received_response = response
        
        # Register callback
        handler.register_callback(message_id, callback, timeout=2.0)
        
        # Deliver response
        response = Message(
            type=MessageType.CUSTOM,
            content={"status": "ok"},
            reply_to=message_id
        )
        handler.deliver_response(message_id, response)
        
        # Give callback time to execute
        await asyncio.sleep(0.1)
        
        # Should have received response via callback
        assert received_response is not None
        assert received_response.content["status"] == "ok"
    
    @pytest.mark.asyncio
    async def test_callback_cleanup_on_timeout(self):
        """Test that callbacks are cleaned up after timeout."""
        from chattercore.message_handler import CallbackHandler
        
        handler = CallbackHandler()
        message_id = "test-cleanup"
        
        callback_called = False
        
        def callback(response):
            nonlocal callback_called
            callback_called = True
        
        # Register callback with short timeout
        handler.register_callback(message_id, callback, timeout=0.5)
        
        # Wait for timeout
        await asyncio.sleep(0.6)
        
        # Callback should be cleaned up, delivering response should not call it
        response = Message(type=MessageType.CUSTOM, content={"test": "data"})
        handler.deliver_response(message_id, response)
        
        await asyncio.sleep(0.1)
        assert not callback_called
    
    @pytest.mark.asyncio
    async def test_duplicate_wait_raises_error(self):
        """Test that waiting for the same message twice raises an error."""
        from chattercore.message_handler import CallbackHandler
        from chattercore.exceptions import MessageException
        
        handler = CallbackHandler()
        message_id = "test-duplicate"
        
        # Start first wait
        wait_task = asyncio.create_task(
            handler.wait_for_response(message_id, timeout=2.0)
        )
        
        await asyncio.sleep(0.1)
        
        # Second wait should raise error
        with pytest.raises(MessageException, match="Already waiting for response"):
            await handler.wait_for_response(message_id, timeout=2.0)
        
        # Cleanup
        wait_task.cancel()
        try:
            await wait_task
        except asyncio.CancelledError:
            pass


class TestContentTypeEdgeCases:
    """Test edge cases in content type handling."""
    
    def test_empty_custom_message_content(self):
        """Test CUSTOM message with empty dict."""
        message = Message(
            type=MessageType.CUSTOM,
            content={}
        )
        
        assert isinstance(message.content, dict)
        assert len(message.content) == 0
    
    def test_nested_custom_message_content(self):
        """Test CUSTOM message with deeply nested content."""
        message = Message(
            type=MessageType.CUSTOM,
            content={
                "level1": {
                    "level2": {
                        "level3": {
                            "data": "deep value"
                        }
                    }
                }
            }
        )
        
        assert message.content["level1"]["level2"]["level3"]["data"] == "deep value"
    
    def test_custom_message_with_arrays(self):
        """Test CUSTOM message with array content."""
        message = Message(
            type=MessageType.CUSTOM,
            content={
                "items": [1, 2, 3],
                "names": ["alice", "bob"],
                "mixed": [{"id": 1}, {"id": 2}]
            }
        )
        
        assert len(message.content["items"]) == 3
        assert message.content["names"][0] == "alice"
        assert message.content["mixed"][0]["id"] == 1
    
    def test_custom_message_with_special_characters(self):
        """Test CUSTOM message with special characters."""
        message = Message(
            type=MessageType.CUSTOM,
            content={
                "emoji": "🚀",
                "unicode": "café",
                "newlines": "line1\nline2",
                "quotes": 'He said "hello"'
            }
        )
        
        assert message.content["emoji"] == "🚀"
        assert message.content["unicode"] == "café"
        assert "\n" in message.content["newlines"]
