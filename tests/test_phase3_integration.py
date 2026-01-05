"""
Focused Integration Tests for Schema Validation and Tracing

These tests verify the Phase 3 improvements work correctly.
"""

import pytest
import asyncio
import logging
from pydantic import BaseModel, ValidationError
from typing import Literal, Optional

from chattercore.server import ChatterServer
from chattercore.client import ChatterClient
from chattercore.message_handler import Message, MessageType
from chattercore.exceptions import MessageException


class MemoryRequest(BaseModel):
    """Example schema for memory requests."""
    operation: Literal["recall", "retain", "forget"]
    request_id: str
    agent_id: str
    query: Optional[str] = None


@pytest.mark.asyncio
class TestSchemaValidationIntegration:
    """Test schema validation in live server-client scenario."""
    
    async def test_register_and_validate_schema(self):
        """Test registering a schema and validating messages against it."""
        server = ChatterServer(host="localhost", port=8766)
        
        # Register schema
        server.register_schema("operation:*", MemoryRequest)
        
        # Create a valid message
        valid_content = {
            "operation": "recall",
            "request_id": "req-123",
            "agent_id": "AGENT_A",
            "query": "test query"
        }
        
        message = Message(
            type=MessageType.CUSTOM,
            content=valid_content
        )
        
        # Should validate successfully
        assert server._validate_message(message)
    
    async def test_invalid_schema_lenient_mode(self):
        """Test that invalid messages fail validation in lenient mode."""
        server = ChatterServer(host="localhost", port=8767)
        
        # Register schema
        server.register_schema("operation:*", MemoryRequest)
        
        # Enable lenient mode (default)
        server.enable_strict_validation(False)
        
        # Create an invalid message (missing required fields)
        invalid_content = {
            "operation": "invalid_op",  # Invalid literal
            "request_id": "req-123"
            # missing agent_id
        }
        
        message = Message(
            type=MessageType.CUSTOM,
            content=invalid_content
        )
        
        # Should return False but not raise
        assert not server._validate_message(message)
    
    async def test_invalid_schema_strict_mode(self):
        """Test that invalid messages raise exception in strict mode."""
        server = ChatterServer(host="localhost", port=8768)
        
        # Register schema
        server.register_schema("operation:*", MemoryRequest)
        
        # Enable strict mode
        server.enable_strict_validation(True)
        
        # Create an invalid message
        invalid_content = {
            "operation": "invalid_op",
            "request_id": "req-123"
        }
        
        message = Message(
            type=MessageType.CUSTOM,
            content=invalid_content
        )
        
        # Should raise MessageException
        with pytest.raises(MessageException, match="Schema validation failed"):
            server._validate_message(message)
    
    async def test_pattern_matching(self):
        """Test schema pattern matching."""
        server = ChatterServer(host="localhost", port=8769)
        
        # Test key:value pattern
        content1 = {"operation": "recall", "data": "test"}
        assert server._matches_pattern(content1, "operation:*")
        assert server._matches_pattern(content1, "operation:recall")
        assert not server._matches_pattern(content1, "operation:invalid")
        
        # Test key existence
        content2 = {"test_key": "value"}
        assert server._matches_pattern(content2, "test_key")
        assert not server._matches_pattern(content2, "nonexistent")


@pytest.mark.asyncio
class TestTracingIntegration:
    """Test message tracing functionality."""
    
    async def test_enable_tracing(self):
        """Test enabling and disabling tracing."""
        server = ChatterServer(host="localhost", port=8770)
        
        # Initially disabled
        assert not server._tracing_enabled
        
        # Enable tracing
        server.enable_tracing(True)
        assert server._tracing_enabled
        
        # Disable tracing
        server.enable_tracing(False)
        assert not server._tracing_enabled
    
    async def test_trace_message(self, caplog):
        """Test message tracing logs."""
        server = ChatterServer(host="localhost", port=8771)
        server.enable_tracing(True)
        
        message = Message(
            type=MessageType.CUSTOM,
            content={"test": "data"}
        )
        
        # Capture logs
        with caplog.at_level(logging.DEBUG):
            server._trace_message("send", message, "conn-123")
            
            # Check that trace was logged
            assert any("TRACE" in record.message for record in caplog.records)
            assert any("conn-123" in record.message for record in caplog.records)


@pytest.mark.asyncio
class TestAllPhasesIntegration:
    """Test all improvement phases working together."""
    
    async def test_custom_message_with_validation_and_tracing(self):
        """Test CUSTOM message with schema validation and tracing."""
        server = ChatterServer(host="localhost", port=8772)
        
        # Phase 3: Enable tracing
        server.enable_tracing(True)
        
        # Phase 3: Register schema
        server.register_schema("operation:*", MemoryRequest)
        
        # Phase 1: Create CUSTOM message with dict content
        content = {
            "operation": "recall",
            "request_id": "req-456",
            "agent_id": "AGENT_B",
            "query": "test"
        }
        
        message = Message(
            type=MessageType.CUSTOM,
            content=content
        )
        
        # Should validate successfully
        assert server._validate_message(message)
        
        # Content should be dict (Phase 1)
        assert isinstance(message.content, dict)
        assert message.content["operation"] == "recall"
    
    async def test_request_response_with_custom_type(self):
        """Test request-response pattern with CUSTOM message type."""
        # Phase 1: CUSTOM messages are dicts
        # Phase 1: Request-response pattern
        
        content = {
            "operation": "test",
            "data": {"key": "value"}
        }
        
        message = Message(
            type=MessageType.CUSTOM,
            content=content
        )
        
        # Serialize for transport
        json_str = message.to_json()
        
        # Deserialize
        received = Message.from_json(json_str)
        
        # Content should remain as dict
        assert isinstance(received.content, dict)
        assert received.content == content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
