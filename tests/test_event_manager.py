"""
Tests for ChatterCore event management
"""

import pytest
import asyncio
from chattercore.event_manager import EventManager, EventType, Event


class TestEvent:
    """Test the Event class."""
    
    def test_create_event(self):
        """Test creating an event."""
        event = Event(
            type=EventType.CLIENT_CONNECTED,
            data={'connection_id': '123'},
            source='test'
        )
        
        assert event.type == EventType.CLIENT_CONNECTED
        assert event.data == {'connection_id': '123'}
        assert event.source == 'test'
        assert event.event_id is not None
    
    def test_event_auto_id(self):
        """Test automatic event ID generation."""
        event = Event(type=EventType.CLIENT_CONNECTED)
        assert event.event_id is not None
        assert len(event.event_id) > 0


class TestEventManager:
    """Test the EventManager class."""
    
    def test_create_manager(self):
        """Test creating an event manager."""
        manager = EventManager()
        assert manager is not None
    
    def test_subscribe(self):
        """Test subscribing to events."""
        manager = EventManager()
        
        def test_listener(event):
            pass
        
        manager.subscribe(EventType.CLIENT_CONNECTED, test_listener)
        
        # Check listener was registered
        assert EventType.CLIENT_CONNECTED in manager._listeners
        assert test_listener in manager._listeners[EventType.CLIENT_CONNECTED]
    
    def test_unsubscribe(self):
        """Test unsubscribing from events."""
        manager = EventManager()
        
        def test_listener(event):
            pass
        
        manager.subscribe(EventType.CLIENT_CONNECTED, test_listener)
        manager.unsubscribe(EventType.CLIENT_CONNECTED, test_listener)
        
        # Check listener was removed
        assert test_listener not in manager._listeners.get(EventType.CLIENT_CONNECTED, [])
    
    def test_subscribe_once(self):
        """Test one-time event subscription."""
        manager = EventManager()
        
        def test_listener(event):
            pass
        
        manager.subscribe_once(EventType.CLIENT_CONNECTED, test_listener)
        
        # Check listener was registered as one-time
        assert EventType.CLIENT_CONNECTED in manager._once_listeners
        assert test_listener in manager._once_listeners[EventType.CLIENT_CONNECTED]
    
    @pytest.mark.asyncio
    async def test_publish_and_handle(self):
        """Test publishing and handling events."""
        manager = EventManager()
        received_events = []
        
        def test_listener(event):
            received_events.append(event)
        
        manager.subscribe(EventType.CLIENT_CONNECTED, test_listener)
        await manager.start()
        
        # Publish an event
        await manager.publish(
            EventType.CLIENT_CONNECTED,
            data={'connection_id': '123'}
        )
        
        # Give time for event processing
        await asyncio.sleep(0.1)
        
        # Check event was received
        assert len(received_events) == 1
        assert received_events[0].type == EventType.CLIENT_CONNECTED
        assert received_events[0].data == {'connection_id': '123'}
        
        await manager.stop()
    
    @pytest.mark.asyncio
    async def test_once_listener(self):
        """Test one-time listeners are removed after execution."""
        manager = EventManager()
        received_events = []
        
        def test_listener(event):
            received_events.append(event)
        
        manager.subscribe_once(EventType.CLIENT_CONNECTED, test_listener)
        await manager.start()
        
        # Publish event twice
        await manager.publish(EventType.CLIENT_CONNECTED, data={'test': 1})
        await manager.publish(EventType.CLIENT_CONNECTED, data={'test': 2})
        
        # Give time for event processing
        await asyncio.sleep(0.1)
        
        # Should only receive one event
        assert len(received_events) == 1
        
        await manager.stop()
    
    @pytest.mark.asyncio
    async def test_emit(self):
        """Test emitting events with keyword arguments."""
        manager = EventManager()
        received_events = []
        
        def test_listener(event):
            received_events.append(event)
        
        manager.subscribe(EventType.CLIENT_CONNECTED, test_listener)
        await manager.start()
        
        # Emit event with kwargs
        await manager.emit(EventType.CLIENT_CONNECTED, connection_id='123', user_id='456')
        
        # Give time for event processing
        await asyncio.sleep(0.1)
        
        # Check event data
        assert len(received_events) == 1
        assert received_events[0].data == {'connection_id': '123', 'user_id': '456'}
        
        await manager.stop()
    
    @pytest.mark.asyncio
    async def test_middleware(self):
        """Test event middleware."""
        manager = EventManager()
        middleware_calls = []
        received_events = []
        
        def test_middleware(event):
            middleware_calls.append(event)
            return event
        
        def test_listener(event):
            received_events.append(event)
        
        manager.add_middleware(test_middleware)
        manager.subscribe(EventType.CLIENT_CONNECTED, test_listener)
        await manager.start()
        
        await manager.publish(EventType.CLIENT_CONNECTED, data={'test': 'data'})
        
        # Give time for processing
        await asyncio.sleep(0.1)
        
        # Check middleware was called
        assert len(middleware_calls) == 1
        assert len(received_events) == 1
        
        await manager.stop()
    
    def test_get_stats(self):
        """Test getting event manager statistics."""
        manager = EventManager()
        stats = manager.get_stats()
        
        assert isinstance(stats, dict)
        assert 'events_published' in stats
        assert 'events_processed' in stats
        assert 'events_failed' in stats
        assert 'listeners_count' in stats
    
    def test_get_listener_count(self):
        """Test getting listener counts."""
        manager = EventManager()
        
        def test_listener(event):
            pass
        
        # Initially no listeners
        assert manager.get_listener_count() == 0
        assert manager.get_listener_count(EventType.CLIENT_CONNECTED) == 0
        
        # Add a listener
        manager.subscribe(EventType.CLIENT_CONNECTED, test_listener)
        
        # Check counts
        assert manager.get_listener_count() == 1
        assert manager.get_listener_count(EventType.CLIENT_CONNECTED) == 1
    
    def test_clear_listeners(self):
        """Test clearing listeners."""
        manager = EventManager()
        
        def test_listener(event):
            pass
        
        manager.subscribe(EventType.CLIENT_CONNECTED, test_listener)
        manager.subscribe(EventType.CLIENT_DISCONNECTED, test_listener)
        
        # Clear specific event type
        manager.clear_listeners(EventType.CLIENT_CONNECTED)
        assert manager.get_listener_count(EventType.CLIENT_CONNECTED) == 0
        assert manager.get_listener_count(EventType.CLIENT_DISCONNECTED) == 1
        
        # Clear all listeners
        manager.clear_listeners()
        assert manager.get_listener_count() == 0
