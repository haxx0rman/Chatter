"""
Integration tests for ChatterCore server and client
"""

import pytest
import asyncio
from chattercore import ChatterServer, ChatterClient, EventType, MessageType


class TestIntegration:
    """Integration tests for server-client communication."""
    
    @pytest.mark.asyncio
    async def test_server_client_connection(self):
        """Test basic server-client connection."""
        # Create server
        server = ChatterServer(host="localhost", port=8766)
        
        # Start server
        await server.start()
        
        try:
            # Create client
            client = ChatterClient("ws://localhost:8766")
            
            # Connect client
            connected = await client.connect()
            assert connected is True
            assert client.is_connected is True
            
            # Disconnect client
            await client.disconnect()
            assert client.is_connected is False
            
        finally:
            # Stop server
            await server.stop()
    
    @pytest.mark.asyncio
    async def test_message_sending(self):
        """Test sending messages between client and server."""
        server = ChatterServer(host="localhost", port=8767)
        received_messages = []
        
        # Handler to capture messages
        async def message_handler(message, context):
            received_messages.append(message)
        
        server.register_message_handler(MessageType.TEXT, message_handler)
        
        await server.start()
        
        try:
            client = ChatterClient("ws://localhost:8767")
            await client.connect()
            
            # Send a message
            message = await client.send_message("Hello, Server!")
            assert message is not None
            
            # Give time for message processing
            await asyncio.sleep(0.1)
            
            # Check message was received
            assert len(received_messages) == 1
            assert received_messages[0].content == "Hello, Server!"
            assert received_messages[0].type == MessageType.TEXT
            
            await client.disconnect()
            
        finally:
            await server.stop()
    
    @pytest.mark.asyncio
    async def test_channel_operations(self):
        """Test channel join/leave operations."""
        server = ChatterServer(host="localhost", port=8768)
        await server.start()
        
        try:
            client = ChatterClient("ws://localhost:8768")
            await client.connect()
            
            # Join a channel
            join_result = await client.join_channel("test-channel")
            assert join_result is True
            
            # Leave the channel
            leave_result = await client.leave_channel("test-channel")
            assert leave_result is True
            
            await client.disconnect()
            
        finally:
            await server.stop()
    
    @pytest.mark.asyncio
    async def test_event_system(self):
        """Test event system functionality."""
        server = ChatterServer(host="localhost", port=8769)
        events_received = []
        
        # Event listener
        async def event_listener(event):
            events_received.append(event)
        
        server.subscribe_to_event(EventType.CLIENT_CONNECTED, event_listener)
        server.subscribe_to_event(EventType.CLIENT_DISCONNECTED, event_listener)
        
        await server.start()
        
        try:
            client = ChatterClient("ws://localhost:8769")
            
            # Connect client - should trigger CLIENT_CONNECTED event
            await client.connect()
            
            # Give time for event processing
            await asyncio.sleep(0.1)
            
            # Disconnect client - should trigger CLIENT_DISCONNECTED event
            await client.disconnect()
            
            # Give time for event processing
            await asyncio.sleep(0.1)
            
            # Check events were received
            assert len(events_received) >= 1  # At least connection event
            
            # Find connection event
            connection_events = [e for e in events_received if e.type == EventType.CLIENT_CONNECTED]
            assert len(connection_events) >= 1
            
        finally:
            await server.stop()
    
    @pytest.mark.asyncio
    async def test_multiple_clients(self):
        """Test multiple clients connecting simultaneously."""
        server = ChatterServer(host="localhost", port=8770)
        await server.start()
        
        try:
            # Create multiple clients
            clients = []
            for i in range(3):
                client = ChatterClient("ws://localhost:8770")
                clients.append(client)
            
            # Connect all clients
            for client in clients:
                connected = await client.connect()
                assert connected is True
            
            # Check all clients are connected
            for client in clients:
                assert client.is_connected is True
            
            # Disconnect all clients
            for client in clients:
                await client.disconnect()
            
            # Check all clients are disconnected
            for client in clients:
                assert client.is_connected is False
            
        finally:
            await server.stop()
    
    @pytest.mark.asyncio
    async def test_heartbeat_system(self):
        """Test heartbeat functionality."""
        server = ChatterServer(host="localhost", port=8771)
        await server.start()
        
        try:
            # Client with short heartbeat interval for testing
            client = ChatterClient("ws://localhost:8771", heartbeat_interval=1)
            await client.connect()
            
            # Send a heartbeat manually
            await client.send_message("ping", MessageType.HEARTBEAT)
            
            # Give time for response
            await asyncio.sleep(0.5)
            
            # Client should still be connected
            assert client.is_connected is True
            
            await client.disconnect()
            
        finally:
            await server.stop()
    
    @pytest.mark.asyncio
    async def test_server_stats(self):
        """Test server statistics functionality."""
        server = ChatterServer(host="localhost", port=8772)
        await server.start()
        
        try:
            # Get initial stats
            stats = server.get_stats()
            assert 'server' in stats
            assert 'connections' in stats
            assert 'events' in stats
            assert stats['server']['running'] is True
            
            # Connect a client
            client = ChatterClient("ws://localhost:8772")
            await client.connect()
            
            # Give time for connection processing
            await asyncio.sleep(0.1)
            
            # Get updated stats
            new_stats = server.get_stats()
            assert new_stats['connections']['active_connections'] >= 1
            
            await client.disconnect()
            
        finally:
            await server.stop()
