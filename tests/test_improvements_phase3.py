"""
Tests for ChatterCore Phase 3 Improvements

This module tests:
1. Message Validation & Schema - Pydantic schema validation
2. Logging & Observability - message tracing and debugging
"""

import pytest
import asyncio
from pydantic import BaseModel, ValidationError
from typing import Literal, Optional
from chattercore.message_handler import Message, MessageType
from chattercore.exceptions import ValidationException


class TestSchemaValidation:
    """Test message schema validation."""
    
    def test_register_schema(self):
        """Test registering a Pydantic schema for validation."""
        from chattercore.server import ChatterServer
        
        server = ChatterServer(host="localhost", port=9000)
        
        class TestSchema(BaseModel):
            name: str
            value: int
        
        # Register schema
        server.register_schema("test:*", TestSchema)
        
        # Should be in schemas dict
        assert "test:*" in server._schemas
        assert server._schemas["test:*"] == TestSchema
    
    def test_validate_message_against_schema(self):
        """Test validating a message against a registered schema."""
        from chattercore.server import ChatterServer
        
        server = ChatterServer(host="localhost", port=9001)
        
        class TestSchema(BaseModel):
            name: str
            value: int
        
        server.register_schema("test:*", TestSchema)
        
        # Valid message
        valid_msg = Message(
            type=MessageType.CUSTOM,
            content={"test": "valid", "name": "test", "value": 42}
        )
        
        assert server._validate_message(valid_msg)
    
    def test_schema_validation_failure(self):
        """Test that invalid messages fail schema validation."""
        from chattercore.server import ChatterServer
        
        server = ChatterServer(host="localhost", port=9002)
        
        class TestSchema(BaseModel):
            name: str
            value: int
        
        server.register_schema("test:*", TestSchema)
        server.enable_strict_validation(False)  # Lenient mode
        
        # Invalid message (wrong type for value)
        invalid_msg = Message(
            type=MessageType.CUSTOM,
            content={"test": "invalid", "name": "test", "value": "not_an_int"}
        )
        
        # Should return False in lenient mode
        assert not server._validate_message(invalid_msg)
    
    def test_schema_pattern_matching(self):
        """Test pattern matching for schema selection."""
        from chattercore.server import ChatterServer
        
        server = ChatterServer(host="localhost", port=9003)
        
        # Test various patterns
        content1 = {"operation": "test"}
        assert server._matches_pattern(content1, "operation:*")
        assert server._matches_pattern(content1, "operation:test")
        assert not server._matches_pattern(content1, "operation:other")
        
        content2 = {"type": "message", "subtype": "data"}
        assert server._matches_pattern(content2, "type:*")
        assert server._matches_pattern(content2, "type")
    
    def test_strict_validation_mode(self):
        """Test strict validation mode that raises on errors."""
        from chattercore.server import ChatterServer
        from chattercore.exceptions import MessageException
        
        server = ChatterServer(host="localhost", port=9004)
        
        class TestSchema(BaseModel):
            name: str
        
        server.register_schema("test:*", TestSchema)
        server.enable_strict_validation(True)
        
        # Invalid message
        invalid_msg = Message(
            type=MessageType.CUSTOM,
            content={"test": "invalid"}  # Missing 'name'
        )
        
        # Should raise in strict mode
        with pytest.raises(MessageException):
            server._validate_message(invalid_msg)
    
    def test_lenient_validation_mode(self):
        """Test lenient validation mode that logs but doesn't raise."""
        from chattercore.server import ChatterServer
        
        server = ChatterServer(host="localhost", port=9005)
        
        class TestSchema(BaseModel):
            name: str
        
        server.register_schema("test:*", TestSchema)
        server.enable_strict_validation(False)
        
        # Invalid message
        invalid_msg = Message(
            type=MessageType.CUSTOM,
            content={"test": "invalid"}  # Missing 'name'
        )
        
        # Should return False but not raise
        result = server._validate_message(invalid_msg)
        assert result is False
    
    def test_optional_validation(self):
        """Test that validation can be disabled."""
        from chattercore.server import ChatterServer
        
        server = ChatterServer(host="localhost", port=9006)
        
        # No schemas registered - validation should pass
        invalid_msg = Message(
            type=MessageType.CUSTOM,
            content={"anything": "goes"}
        )
        
        # Should pass when no schemas registered
        assert server._validate_message(invalid_msg)


class TestPydanticSchemaIntegration:
    """Test integration with Pydantic schemas."""
    
    def test_memory_request_schema(self):
        """Test schema for memory requests."""
        
        class MemoryRequest(BaseModel):
            operation: Literal["recall", "retain", "forget"]
            request_id: str
            agent_id: str
            query: Optional[str] = None
        
        # Valid memory request
        valid_data = {
            "operation": "recall",
            "request_id": "req-123",
            "agent_id": "AGENT_A",
            "query": "test query"
        }
        
        request = MemoryRequest(**valid_data)
        assert request.operation == "recall"
        assert request.request_id == "req-123"
    
    def test_invalid_memory_request_schema(self):
        """Test that invalid memory requests fail validation."""
        
        class MemoryRequest(BaseModel):
            operation: Literal["recall", "retain", "forget"]
            request_id: str
            agent_id: str
            query: Optional[str] = None
        
        # Invalid operation
        invalid_data = {
            "operation": "invalid_op",
            "request_id": "req-123",
            "agent_id": "AGENT_A"
        }
        
        with pytest.raises(ValidationError):
            MemoryRequest(**invalid_data)
    
    def test_nested_schema_validation(self):
        """Test validation of nested schemas."""
        
        class Address(BaseModel):
            street: str
            city: str
            zipcode: str
        
        class Person(BaseModel):
            name: str
            age: int
            address: Address
        
        valid_data = {
            "name": "Alice",
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "Springfield",
                "zipcode": "12345"
            }
        }
        
        person = Person(**valid_data)
        assert person.name == "Alice"
        assert person.address.city == "Springfield"


class TestLoggingAndTracing:
    """Test logging and message tracing functionality."""
    
    def test_enable_tracing(self):
        """Test enabling and disabling tracing."""
        from chattercore.server import ChatterServer
        
        server = ChatterServer(host="localhost", port=9100)
        
        # Initially disabled
        assert not server._tracing_enabled
        
        # Enable
        server.enable_tracing(True)
        assert server._tracing_enabled
        
        # Disable
        server.enable_tracing(False)
        assert not server._tracing_enabled
    
    def test_trace_id_generation(self):
        """Test trace ID generation for messages."""
        from chattercore.server import ChatterServer
        
        server = ChatterServer(host="localhost", port=9101)
        server.enable_tracing(True)
        
        message = Message(
            type=MessageType.CUSTOM,
            content={"test": "data"}
        )
        
    def test_message_metrics(self):
        """Test collecting message metrics."""
        from chattercore.server import ChatterServer
        
        server = ChatterServer(host="localhost", port=9010)
        
        # Enable metrics
        server.enable_metrics(True)
        
        # Simulate some messages
        msg1 = Message(type=MessageType.TEXT, content="test1")
        msg2 = Message(type=MessageType.CUSTOM, content={"test": "data"})
        msg3 = Message(type=MessageType.TEXT, content="test2")
        
        server._track_message_received(msg1)
        server._track_message_received(msg2)
        server._track_message_sent(msg3)
        
        # Get metrics
        metrics = server.get_message_metrics()
        
        assert metrics['total_received'] == 2
        assert metrics['total_sent'] == 1
        assert 'MessageType.TEXT' in metrics['by_type']
        assert metrics['by_type']['MessageType.TEXT']['received'] == 1
        assert metrics['by_type']['MessageType.TEXT']['sent'] == 1
        assert 'MessageType.CUSTOM' in metrics['by_type']
        assert metrics['by_type']['MessageType.CUSTOM']['received'] == 1
    
    def test_performance_monitoring(self):
        """Test performance monitoring."""
        from chattercore.server import ChatterServer
        
        server = ChatterServer(host="localhost", port=9011)
        
        # Enable performance monitoring
        server.enable_performance_monitoring(True)
        
        # Track some processing times
        server._track_processing_time(0.001)
        server._track_processing_time(0.002)
        server._track_processing_time(0.003)
        
        # Get performance metrics
        perf_metrics = server.get_performance_metrics()
        
        assert perf_metrics['total_messages'] == 3
        assert perf_metrics['min_processing_time'] == 0.001
        assert perf_metrics['max_processing_time'] == 0.003
        assert 0.001 < perf_metrics['avg_processing_time'] < 0.003
    
    def test_error_tracking(self):
        """Test error tracking and reporting."""
        from chattercore.server import ChatterServer
        
        server = ChatterServer(host="localhost", port=9012)
        
        # Enable error tracking
        server.enable_error_tracking(True)
        
        # Track some errors
        error1 = ValueError("Test error 1")
        error2 = RuntimeError("Test error 2")
        
        server._track_error(error1, "context1")
        server._track_error(error2, "context2")
        
        # Get error log
        error_log = server.get_error_log()
        
        assert len(error_log) == 2
        assert error_log[0]['error_type'] == 'ValueError'
        assert error_log[0]['error_message'] == 'Test error 1'
        assert error_log[0]['context'] == 'context1'
        assert error_log[1]['error_type'] == 'RuntimeError'
        assert 'timestamp' in error_log[0]


class TestSchemaEdgeCases:
    """Test edge cases in schema validation."""
    
    def test_schema_with_optional_fields(self):
        """Test schema with optional fields."""
        
        class OptionalFieldSchema(BaseModel):
            required_field: str
            optional_field: Optional[str] = None
            optional_int: Optional[int] = None
        
        # Without optional fields
        data1 = {"required_field": "test"}
        schema1 = OptionalFieldSchema(**data1)
        assert schema1.optional_field is None
        
        # With optional fields
        data2 = {"required_field": "test", "optional_field": "value"}
        schema2 = OptionalFieldSchema(**data2)
        assert schema2.optional_field == "value"
    
    def test_schema_with_defaults(self):
        """Test schema with default values."""
        
        class DefaultValueSchema(BaseModel):
            name: str
            count: int = 0
            enabled: bool = True
        
        data = {"name": "test"}
        schema = DefaultValueSchema(**data)
        assert schema.count == 0
        assert schema.enabled is True
    
    def test_schema_with_custom_validators(self):
        """Test schema with custom validators."""
        from pydantic import field_validator
        
        class CustomValidatorSchema(BaseModel):
            email: str
            
            @field_validator('email')
            @classmethod
            def validate_email(cls, v):
                if '@' not in v:
                    raise ValueError('Invalid email format')
                return v
        
        # Valid email
        schema = CustomValidatorSchema(email="test@example.com")
        assert schema.email == "test@example.com"
        
        # Invalid email
        with pytest.raises(ValidationError):
            CustomValidatorSchema(email="invalid-email")
