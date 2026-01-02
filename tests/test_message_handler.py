"""
Tests for ChatterCore message handling
"""

import pytest
from datetime import datetime
from chattercore.message_handler import Message, MessageType, MessageHandler
from chattercore.exceptions import ValidationException


class TestMessage:
    """Test the Message class."""
    
    def test_create_message(self):
        """Test creating a basic message."""
        message = Message(
            type=MessageType.TEXT,
            content="Hello, World!",
            sender_id="user123"
        )
        
        assert message.type == MessageType.TEXT
        assert message.content == "Hello, World!"
        assert message.sender_id == "user123"
        assert message.id is not None
        assert isinstance(message.timestamp, datetime)
    
    def test_message_to_dict(self):
        """Test converting message to dictionary."""
        message = Message(
            type=MessageType.TEXT,
            content="Test message",
            sender_id="user123"
        )
        
        msg_dict = message.to_dict()
        assert isinstance(msg_dict, dict)
        assert msg_dict['type'] == MessageType.TEXT
        assert msg_dict['content'] == "Test message"
        assert msg_dict['sender_id'] == "user123"
    
    def test_message_to_json(self):
        """Test converting message to JSON."""
        message = Message(
            type=MessageType.TEXT,
            content="Test message"
        )
        
        json_str = message.to_json()
        assert isinstance(json_str, str)
        assert "Test message" in json_str
    
    def test_message_from_dict(self):
        """Test creating message from dictionary."""
        data = {
            'type': MessageType.TEXT,
            'content': 'Test message',
            'sender_id': 'user123'
        }
        
        message = Message.from_dict(data)
        assert message.type == MessageType.TEXT
        assert message.content == 'Test message'
        assert message.sender_id == 'user123'
    
    def test_message_from_json(self):
        """Test creating message from JSON."""
        json_data = '{"type": "text", "content": "Test message", "sender_id": "user123"}'
        
        message = Message.from_json(json_data)
        assert message.type == MessageType.TEXT
        assert message.content == "Test message"
        assert message.sender_id == "user123"
    
    def test_invalid_json(self):
        """Test handling invalid JSON."""
        with pytest.raises(ValidationException):
            Message.from_json("invalid json")
    
    def test_message_expiry(self):
        """Test message expiry functionality."""
        from datetime import datetime, timezone
        
        # Message without expiry should not be expired
        message = Message(content="Test")
        assert not message.is_expired()
        
        # Message with past expiry should be expired
        past_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
        message_expired = Message(content="Test", expires_at=past_time)
        assert message_expired.is_expired()


class TestMessageHandler:
    """Test the MessageHandler class."""
    
    def test_create_handler(self):
        """Test creating a message handler."""
        handler = MessageHandler()
        assert handler is not None
    
    def test_register_handler(self):
        """Test registering a message handler."""
        handler = MessageHandler()
        
        def test_handler(message, context=None):
            pass
        
        handler.register_handler(MessageType.TEXT, test_handler)
        
        # Check handler was registered
        assert MessageType.TEXT in handler._handlers
        assert test_handler in handler._handlers[MessageType.TEXT]
    
    def test_unregister_handler(self):
        """Test unregistering a message handler."""
        handler = MessageHandler()
        
        def test_handler(message, context=None):
            pass
        
        handler.register_handler(MessageType.TEXT, test_handler)
        handler.unregister_handler(MessageType.TEXT, test_handler)
        
        # Check handler was removed
        assert test_handler not in handler._handlers.get(MessageType.TEXT, [])
    
    @pytest.mark.asyncio
    async def test_process_message(self):
        """Test processing a message."""
        handler = MessageHandler()
        processed = []
        
        def test_handler(message, context=None):
            processed.append(message)
        
        handler.register_handler(MessageType.TEXT, test_handler)
        
        message = Message(type=MessageType.TEXT, content="Test")
        result = await handler.process_message(message)
        
        assert result is True
        assert len(processed) == 1
        assert processed[0] == message
    
    @pytest.mark.asyncio
    async def test_middleware(self):
        """Test message middleware."""
        handler = MessageHandler()
        middleware_calls = []
        
        def test_middleware(message, context=None):
            middleware_calls.append(message)
            return message
        
        handler.add_middleware(test_middleware)
        
        message = Message(type=MessageType.TEXT, content="Test")
        await handler.process_message(message)
        
        assert len(middleware_calls) == 1
        assert middleware_calls[0] == message
    
    def test_create_message(self):
        """Test creating messages via handler."""
        handler = MessageHandler()
        
        message = handler.create_message(
            "Test content",
            MessageType.TEXT,
            sender_id="user123"
        )
        
        assert message.content == "Test content"
        assert message.type == MessageType.TEXT
        assert message.sender_id == "user123"
    
    def test_create_system_message(self):
        """Test creating system messages."""
        handler = MessageHandler()
        
        message = handler.create_system_message("System alert")
        
        assert message.content == "System alert"
        assert message.type == MessageType.SYSTEM
    
    def test_create_error_message(self):
        """Test creating error messages."""
        handler = MessageHandler()
        
        message = handler.create_error_message("Something went wrong")
        
        assert message.content == "Something went wrong"
        assert message.type == MessageType.ERROR
