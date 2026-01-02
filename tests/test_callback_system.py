"""
Test the callback system functionality
"""

import asyncio
import pytest
from chattercore import MessageHandler, Message, MessageType


class TestCallbackSystem:
    """Test the callback system functionality."""
    
    def test_callback_handler_creation(self):
        """Test that callback handler is created."""
        handler = MessageHandler()
        assert handler.callback_handler is not None
        
    @pytest.mark.asyncio
    async def test_wait_for_response_timeout(self):
        """Test waiting for response with timeout."""
        handler = MessageHandler()
        
        # This should timeout since no response will come
        with pytest.raises(asyncio.TimeoutError):
            await handler.wait_for_response("test-message-id", timeout=0.1)
    
    @pytest.mark.asyncio
    async def test_wait_for_response_success(self):
        """Test successful response handling."""
        handler = MessageHandler()
        
        # Start waiting for response in background
        wait_task = asyncio.create_task(
            handler.wait_for_response("test-message-id", timeout=2.0)
        )
        
        # Give it a moment to set up
        await asyncio.sleep(0.01)
        
        # Create response message
        response = Message(
            content="Response content",
            message_type=MessageType.TEXT,
            reply_to="test-message-id"
        )
        
        # Process the response
        await handler.process_message(response)
        
        # Wait for the response
        result = await wait_task
        
        assert result is not None
        assert result.content == "Response content"
        assert result.reply_to == "test-message-id"
    
    @pytest.mark.asyncio
    async def test_callback_registration_and_call(self):
        """Test callback registration and execution."""
        handler = MessageHandler()
        callback_called = False
        received_message = None
        
        async def test_callback(message: Message):
            nonlocal callback_called, received_message
            callback_called = True
            received_message = message
        
        # Register callback
        handler.register_callback("test-message-id", test_callback, timeout=2.0)
        
        # Create response message
        response = Message(
            content="Callback response",
            message_type=MessageType.TEXT,
            reply_to="test-message-id"
        )
        
        # Process the response
        await handler.process_message(response)
        
        # Give callback a chance to execute
        await asyncio.sleep(0.01)
        
        assert callback_called
        assert received_message is not None
        assert received_message.content == "Callback response"
        assert received_message.reply_to == "test-message-id"
    
    @pytest.mark.asyncio
    async def test_callback_and_wait_exclusivity(self):
        """Test that both wait and callback can't be used together through client."""
        from chattercore import ChatterClient
        
        # Create client (not connected, just for method testing)
        client = ChatterClient("ws://localhost:8765", auto_reconnect=False)
        
        async def dummy_callback(message: Message):
            pass
        
        # This should raise an exception when both are provided
        with pytest.raises(Exception):
            await client.send_message(
                "test", 
                wait_for_response=True, 
                callback=dummy_callback
            )
    
    @pytest.mark.asyncio
    async def test_callback_cleanup_on_timeout(self):
        """Test that callbacks are cleaned up on timeout."""
        handler = MessageHandler()
        
        async def test_callback(message: Message):
            pass
        
        # Register callback with short timeout
        handler.register_callback("test-timeout", test_callback, timeout=0.1)
        
        # Check stats before timeout
        stats_before = handler.callback_handler.get_stats()
        assert stats_before['active_callbacks'] == 1
        assert stats_before['timeout_tasks'] == 1
        
        # Wait for timeout
        await asyncio.sleep(0.2)
        
        # Check stats after timeout
        stats_after = handler.callback_handler.get_stats()
        assert stats_after['active_callbacks'] == 0
        assert stats_after['timeout_tasks'] == 0
    
    @pytest.mark.asyncio 
    async def test_multiple_callbacks(self):
        """Test handling multiple callbacks simultaneously."""
        handler = MessageHandler()
        callback_results = []
        
        async def make_callback(callback_id):
            async def callback(message: Message):
                callback_results.append(f"callback-{callback_id}: {message.content}")
            return callback
        
        # Register multiple callbacks
        for i in range(3):
            callback = await make_callback(i)
            handler.register_callback(f"msg-{i}", callback, timeout=2.0)
        
        # Send responses for each
        for i in range(3):
            response = Message(
                content=f"Response {i}",
                message_type=MessageType.TEXT,
                reply_to=f"msg-{i}"
            )
            await handler.process_message(response)
        
        # Give callbacks time to execute
        await asyncio.sleep(0.01)
        
        # Check results
        assert len(callback_results) == 3
        for i in range(3):
            expected = f"callback-{i}: Response {i}"
            assert expected in callback_results
    
    def test_callback_stats(self):
        """Test callback handler statistics."""
        handler = MessageHandler()
        
        # Initial stats should be empty
        stats = handler.callback_handler.get_stats()
        assert stats['pending_responses'] == 0
        assert stats['active_callbacks'] == 0
        assert stats['timeout_tasks'] == 0
    
    @pytest.mark.asyncio
    async def test_cancel_all_callbacks(self):
        """Test canceling all callbacks."""
        handler = MessageHandler()
        
        # Set up some callbacks and pending responses
        async def dummy_callback(msg):
            pass
        
        handler.register_callback("test1", dummy_callback, timeout=10.0)
        handler.register_callback("test2", dummy_callback, timeout=10.0)
        
        wait_task1 = asyncio.create_task(
            handler.wait_for_response("wait1", timeout=10.0)
        )
        wait_task2 = asyncio.create_task(
            handler.wait_for_response("wait2", timeout=10.0)
        )
        
        await asyncio.sleep(0.01)  # Let tasks set up
        
        # Check stats before cancel
        stats_before = handler.callback_handler.get_stats()
        assert stats_before['pending_responses'] == 2
        assert stats_before['active_callbacks'] == 2
        
        # Cancel all
        handler.callback_handler.cancel_all()
        
        # Check stats after cancel
        stats_after = handler.callback_handler.get_stats()
        assert stats_after['pending_responses'] == 0
        assert stats_after['active_callbacks'] == 0
        assert stats_after['timeout_tasks'] == 0
        
        # Wait tasks should be cancelled
        with pytest.raises(asyncio.CancelledError):
            await wait_task1
        
        with pytest.raises(asyncio.CancelledError):
            await wait_task2
