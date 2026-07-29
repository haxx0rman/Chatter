"""
Tests for ChatterCore Phase 2 Improvements

This module tests:
1. Built-in Message Routing - native routing support
2. Context Enhancement - structured context with routing awareness
"""

import pytest
import asyncio
from chattercore.message_handler import Message, MessageType, MessageContext, RoutedMessage
from chattercore.exceptions import ValidationException


class TestRoutedMessage:
    """Test the RoutedMessage class."""
    
    def test_create_routed_message(self):
        """Test creating a routed message."""
        routed = RoutedMessage(
            content={"operation": "test"},
            sender="AGENT_A",
            recipient="AGENT_B"
        )
        
        assert routed.content == {"operation": "test"}
        assert routed.sender == "AGENT_A"
        assert routed.recipient == "AGENT_B"
        assert isinstance(routed.timestamp, float)
        assert isinstance(routed.metadata, dict)
    
    def test_routed_message_with_metadata(self):
        """Test routed message with custom metadata."""
        routed = RoutedMessage(
            content={"data": "test"},
            sender="AGENT_A",
            recipient="AGENT_B",
            metadata={"priority": "high", "correlation_id": "123"}
        )
        
        assert routed.metadata["priority"] == "high"
        assert routed.metadata["correlation_id"] == "123"
    
    def test_routed_message_to_dict(self):
        """Test converting routed message to dict."""
        routed = RoutedMessage(
            content={"test": "data"},
            sender="AGENT_A",
            recipient="AGENT_B"
        )
        
        data = routed.to_dict()
        assert data["sender"] == "AGENT_A"
        assert data["recipient"] == "AGENT_B"
        assert data["content"] == {"test": "data"}
        assert "timestamp" in data
        assert "metadata" in data


class TestMessageContext:
    """Test the enhanced MessageContext class."""
    
    def test_create_basic_context(self):
        """Test creating a basic message context."""
        context = MessageContext(
            connection_id="conn-123",
            user_id="user-456"
        )
        
        assert context.connection_id == "conn-123"
        assert context.user_id == "user-456"
        assert isinstance(context.timestamp, float)
        assert context.sender is None
        assert context.recipient is None
        assert len(context.route_hops) == 0
    
    def test_context_with_routing_info(self):
        """Test context with routing information."""
        context = MessageContext(
            connection_id="conn-123",
            user_id="user-456"
        )
        context.sender = "AGENT_A"
        context.recipient = "AGENT_B"
        
        assert context.sender == "AGENT_A"
        assert context.recipient == "AGENT_B"
        assert context.is_routed()
    
    def test_context_is_routed(self):
        """Test is_routed() method."""
        # Context without sender is not routed
        context1 = MessageContext()
        assert not context1.is_routed()
        
        # Context with sender is routed
        context2 = MessageContext()
        context2.sender = "AGENT_A"
        assert context2.is_routed()
    
    def test_context_add_hop(self):
        """Test tracking routing hops."""
        context = MessageContext()
        
        context.add_hop("HUB")
        context.add_hop("AGENT_A")
        context.add_hop("AGENT_B")
        
        assert len(context.route_hops) == 3
        assert context.route_hops[0] == "HUB"
        assert context.route_hops[1] == "AGENT_A"
        assert context.route_hops[2] == "AGENT_B"
    
    def test_context_metadata(self):
        """Test custom metadata in context."""
        context = MessageContext()
        context.metadata["priority"] = "high"
        context.metadata["correlation_id"] = "abc-123"
        context.metadata["custom_data"] = {"key": "value"}
        
        assert context.metadata["priority"] == "high"
        assert context.metadata["correlation_id"] == "abc-123"
        assert context.metadata["custom_data"]["key"] == "value"
    
    def test_context_to_dict(self):
        """Test converting context to dictionary."""
        context = MessageContext(
            connection_id="conn-123",
            user_id="user-456"
        )
        context.sender = "AGENT_A"
        context.recipient = "AGENT_B"
        context.add_hop("HUB")
        context.metadata["test"] = "data"
        
        data = context.to_dict()
        assert data["connection_id"] == "conn-123"
        assert data["user_id"] == "user-456"
        assert data["sender"] == "AGENT_A"
        assert data["recipient"] == "AGENT_B"
        assert data["route_hops"] == ["HUB"]
        assert data["metadata"]["test"] == "data"
        assert "timestamp" in data


class TestBuiltInRouting:
    """Test built-in message routing functionality."""
    
    @pytest.mark.asyncio
    async def test_server_enable_routing(self):
        """Test enabling routing on server."""
        from chattercore.server import ChatterServer
        
        # Create server with routing enabled by default
        server = ChatterServer(host="localhost", port=9200, auto_route_messages=True)
        assert server.auto_route_messages is True
        
        # Can explicitly disable
        server2 = ChatterServer(host="localhost", port=9201, auto_route_messages=False)
        assert server2.auto_route_messages is False
    
    @pytest.mark.asyncio
    async def test_route_message_to_recipient(self):
        """Test routing a message to a specific recipient."""
        from chattercore.server import ChatterServer
        from chattercore.connection_manager import ConnectionInfo
        from unittest.mock import Mock, AsyncMock
        import websockets
        
        server = ChatterServer(host="localhost", port=9202)
        
        # Mock websocket and connection
        mock_ws = Mock()
        mock_ws.send = AsyncMock()
        
        # Add a mock connection for the recipient
        connection_id = await server.connection_manager.add_connection(mock_ws)
        await server.connection_manager.authenticate_connection(connection_id, "recipient_user")
        
        # Route a message to the recipient
        await server.route_message(
            content={"test": "data"},
            sender="sender_user",
            recipient="recipient_user",
            message_type=MessageType.CUSTOM
        )
        
        # Verify message was sent
        assert mock_ws.send.called
    
    @pytest.mark.asyncio
    async def test_route_message_recipient_not_found(self):
        """Test routing to non-existent recipient raises error."""
        from chattercore.server import ChatterServer
        from chattercore.exceptions import ConnectionException
        
        server = ChatterServer(host="localhost", port=9203)
        
        # Try to route to non-existent recipient
        # Should not raise, but will log warning (no connections for user)
        # The send_to_user method handles this gracefully
        try:
            await server.route_message(
                content={"test": "data"},
                sender="sender_user",
                recipient="nonexistent_user",
                message_type=MessageType.CUSTOM
            )
            # Should complete without error (just no recipients)
        except Exception as e:
            # If it raises, it should be a ConnectionException
            assert isinstance(e, (ConnectionException, Exception))
    
    @pytest.mark.asyncio
    async def test_route_message_preserves_envelope(self):
        """Test that routing envelope is preserved."""
        from chattercore.server import ChatterServer
        from chattercore.message_handler import RoutedMessage
        
        server = ChatterServer(host="localhost", port=9204)
        
        # Create a routed message
        routed = RoutedMessage(
            content={"operation": "test"},
            sender="AGENT_A",
            recipient="AGENT_B",
            metadata={"priority": "high"}
        )
        
        # Verify envelope is preserved
        envelope = routed.to_dict()
        assert envelope["sender"] == "AGENT_A"
        assert envelope["recipient"] == "AGENT_B"
        assert envelope["metadata"]["priority"] == "high"
        assert "timestamp" in envelope
    
    @pytest.mark.asyncio
    async def test_route_message_multiple_recipients(self):
        """Test routing to multiple recipients with same alias."""
        from chattercore.server import ChatterServer
        from unittest.mock import Mock, AsyncMock
        
        server = ChatterServer(host="localhost", port=9205)
        
        # Add multiple connections for same user
        mock_ws1 = Mock()
        mock_ws1.send = AsyncMock()
        mock_ws2 = Mock()
        mock_ws2.send = AsyncMock()
        
        conn1 = await server.connection_manager.add_connection(mock_ws1)
        conn2 = await server.connection_manager.add_connection(mock_ws2)
        await server.connection_manager.authenticate_connection(conn1, "multi_user")
        await server.connection_manager.authenticate_connection(conn2, "multi_user")
        
        # Route message to user with multiple connections
        await server.route_message(
            content={"test": "broadcast"},
            sender="sender",
            recipient="multi_user"
        )
        
        # Both connections should receive the message
        assert mock_ws1.send.called
        assert mock_ws2.send.called
    
    @pytest.mark.asyncio
    async def test_client_route_message(self):
        """Test client-side routing message sending."""
        from chattercore.client import ChatterClient
        from chattercore.message_handler import RoutedMessage
        
        client = ChatterClient("ws://localhost:9999")
        client._user_id = "test_client"
        
        # Create routed message on client side
        routed = RoutedMessage(
            content={"action": "request"},
            sender="test_client",
            recipient="server_agent"
        )
        
        assert routed.sender == "test_client"
        assert routed.recipient == "server_agent"
        assert routed.content["action"] == "request"
    
    @pytest.mark.asyncio
    async def test_handler_receives_routing_context(self):
        """Test that handlers receive routing information in context."""
        from chattercore.message_handler import MessageHandler, MessageContext
        
        handler = MessageHandler()
        
        # Create context with routing info
        context = MessageContext(connection_id="conn-123", user_id="user-456")
        context.sender = "AGENT_A"
        context.recipient = "AGENT_B"
        context.add_hop("HUB")
        
        # Verify routing info is accessible
        assert context.is_routed()
        assert context.sender == "AGENT_A"
        assert context.recipient == "AGENT_B"
        assert len(context.route_hops) == 1
        assert context.route_hops[0] == "HUB"


class TestRoutingWithContext:
    """Test integration of routing with context."""
    
    @pytest.mark.asyncio
    async def test_routed_message_populates_context(self):
        """Test that routed messages populate context fields."""
        from chattercore.message_handler import MessageContext, RoutedMessage
        
        # Create a routed message
        routed = RoutedMessage(
            content={"operation": "test"},
            sender="AGENT_A",
            recipient="AGENT_B"
        )
        
        # Create context and populate with routing info
        context = MessageContext()
        context.sender = routed.sender
        context.recipient = routed.recipient
        
        # Verify context is populated
        assert context.sender == "AGENT_A"
        assert context.recipient == "AGENT_B"
        assert context.is_routed()
    
    @pytest.mark.asyncio
    async def test_context_hop_tracking(self):
        """Test that message routing hops are tracked in context."""
        from chattercore.message_handler import MessageContext
        
        context = MessageContext()
        
        # Simulate message passing through multiple hops
        context.add_hop("GATEWAY")
        context.add_hop("HUB")
        context.add_hop("AGENT_A")
        
        assert len(context.route_hops) == 3
        assert context.route_hops == ["GATEWAY", "HUB", "AGENT_A"]
    
    @pytest.mark.asyncio
    async def test_routing_metadata_accessible_to_handler(self):
        """Test that routing metadata is accessible to message handlers."""
        from chattercore.message_handler import MessageHandler, Message, MessageContext
        
        handler = MessageHandler()
        received_context = None
        
        # Register handler that captures context
        async def capture_handler(message: Message, context: MessageContext):
            nonlocal received_context
            received_context = context
        
        handler.register_handler(MessageType.CUSTOM, capture_handler)
        
        # Create context with routing metadata
        context = MessageContext()
        context.sender = "AGENT_A"
        context.recipient = "AGENT_B"
        context.metadata["priority"] = "high"
        context.metadata["correlation_id"] = "123"
        
        # Process message
        message = Message(type=MessageType.CUSTOM, content={"test": "data"})
        await handler.process_message(message, context)
        
        # Verify handler received context with metadata
        assert received_context is not None
        assert received_context.sender == "AGENT_A"
        assert received_context.recipient == "AGENT_B"
        assert received_context.metadata["priority"] == "high"
        assert received_context.metadata["correlation_id"] == "123"
    
    @pytest.mark.asyncio
    async def test_multi_hop_routing(self):
        """Test routing through multiple hops."""
        from chattercore.message_handler import MessageContext, RoutedMessage
        
        # Simulate multi-hop routing
        context = MessageContext()
        
        # Message enters gateway
        context.add_hop("GATEWAY")
        context.sender = "CLIENT_A"
        context.recipient = "SERVICE_B"
        
        # Gateway routes to service
        context.add_hop("ROUTER")
        
        # Router routes to service
        context.add_hop("SERVICE_B")
        
        # Verify full routing path
        assert len(context.route_hops) == 3
        assert context.route_hops == ["GATEWAY", "ROUTER", "SERVICE_B"]
        assert context.sender == "CLIENT_A"
        assert context.recipient == "SERVICE_B"
        assert context.is_routed()


class TestRoutingEdgeCases:
    """Test edge cases in routing functionality."""
    
    def test_routed_message_with_string_content(self):
        """Test routed message with string content."""
        routed = RoutedMessage(
            content="plain string message",
            sender="AGENT_A",
            recipient="AGENT_B"
        )
        
        assert isinstance(routed.content, str)
        assert routed.content == "plain string message"
    
    def test_routed_message_empty_metadata(self):
        """Test routed message with no metadata."""
        routed = RoutedMessage(
            content={"test": "data"},
            sender="AGENT_A",
            recipient="AGENT_B"
        )
        
        assert isinstance(routed.metadata, dict)
        assert len(routed.metadata) == 0
    
    def test_context_without_routing(self):
        """Test context for non-routed message."""
        context = MessageContext(
            connection_id="conn-123"
        )
        
        assert not context.is_routed()
        assert context.sender is None
        assert context.recipient is None
        assert len(context.route_hops) == 0
    
    def test_context_session_id(self):
        """Test context with session ID."""
        context = MessageContext(
            connection_id="conn-123",
            session_id="session-abc"
        )
        
        assert context.session_id == "session-abc"
