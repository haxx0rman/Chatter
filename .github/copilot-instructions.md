<!-- Use this file to provide workspace-specific custom instructions to Copilot. For more details, visit https://code.visualstudio.com/docs/copilot/copilot-customization#_use-a-githubcopilotinstructionsmd-file -->

# ChatterCore Development Instructions

This is a Python-based real-time communication module built with asyncio and WebSockets. When working on this project, please follow these guidelines:

## Architecture
- **ChatterServer**: Main WebSocket server handling connections and routing
- **ChatterClient**: Client library with auto-reconnection and event handling  
- **MessageHandler**: Message parsing, validation, and routing system
- **EventManager**: Event-driven architecture with pub/sub pattern
- **ConnectionManager**: WebSocket connection lifecycle and channel management

## Code Style
- Use type hints throughout the codebase
- Follow PEP 8 naming conventions
- Use Pydantic models for message validation
- Implement proper error handling with custom exceptions
- Use asyncio for all I/O operations

## Key Design Patterns
- **Event-driven**: All significant actions emit events that can be subscribed to
- **Middleware support**: Both message and connection middleware for extensibility  
- **Channel-based**: Support for grouping connections into channels
- **Auto-reconnection**: Robust client with automatic reconnection logic
- **Type safety**: Full type hints and Pydantic validation

## Testing
- Use pytest with pytest-asyncio for async test support
- Test both sync and async code paths
- Mock WebSocket connections for unit testing
- Include integration tests for server-client communication

## Dependencies
- **websockets**: WebSocket server/client implementation
- **pydantic**: Data validation and serialization
- **typing-extensions**: Extended type hints support

## Error Handling
- Use custom exception hierarchy (ChatterCoreException as base)
- Always handle WebSocket connection errors gracefully
- Emit error events for monitoring and debugging
- Log errors appropriately with structured logging

## Performance Considerations
- Use connection pooling and cleanup for stale connections
- Implement message queuing for high-throughput scenarios  
- Consider memory usage with large numbers of connections
- Use asyncio.Queue for internal message passing
