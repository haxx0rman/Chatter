# ChatterCore

A self-contained, general-purpose real-time communication module for Python.

## Overview

ChatterCore is a modern, asyncio-based communication system that provides:

- **Real-time messaging** via WebSockets
- **Event-driven architecture** with custom event handlers
- **Message routing and broadcasting** capabilities
- **Client-server communication** with automatic reconnection
- **Modular design** for easy integration into existing projects
- **Type safety** with Pydantic models

## Features

- 🚀 **High Performance**: Built on asyncio for concurrent connections
- 🔧 **Modular**: Clean separation of concerns with pluggable components
- 🛡️ **Type Safe**: Full type hints and Pydantic validation
- 🔄 **Auto-Reconnect**: Robust client with automatic reconnection logic
- 📡 **Event System**: Flexible event handling and message routing
- 💬 **Callback System**: Two-way communication with sync/async response handling
- 🧪 **Tested**: Comprehensive test suite with pytest

## Quick Start

### Installation

```bash
pip install chattercore
```

### Basic Server

```python
from chattercore import ChatterServer

async def main():
    server = ChatterServer(host="localhost", port=8765)
    await server.start()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

### Basic Client

```python
from chattercore import ChatterClient

async def main():
    client = ChatterClient("ws://localhost:8765")
    await client.connect()
    
    # Send a message and wait for response
    response = await client.send_message(
        "Hello, ChatterCore!", 
        wait_for_response=True
    )
    print(f"Server replied: {response.content}")
    
    # Or use async callback
    async def handle_response(message):
        print(f"Async response: {message.content}")
    
    await client.send_message(
        "Another message", 
        callback=handle_response
    )

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

## Callback System

ChatterCore supports two routes for handling message responses:

1. **Synchronous Waiting**: Wait for response and get it returned
2. **Asynchronous Callbacks**: Register function called when response arrives

```python
# Route 1: Synchronous waiting
response = await client.send_message(
    "Request data",
    wait_for_response=True,
    timeout=10
)

# Route 2: Asynchronous callback
async def response_handler(message):
    print(f"Got response: {message.content}")

await client.send_message(
    "Background request",
    callback=response_handler,
    timeout=10
)
```

See `docs/callback_system.md` for detailed documentation.

## Architecture

ChatterCore is built with several key components:

- **ChatterServer**: WebSocket server for handling connections
- **ChatterClient**: Client library with auto-reconnection
- **MessageHandler**: Routes and processes different message types
- **EventManager**: Handles custom events and callbacks
- **ConnectionManager**: Manages active WebSocket connections

## Development

```bash
# Clone the repository
git clone https://github.com/yourusername/chattercore.git
cd chattercore

# Install in development mode
pip install -e .[dev]

# Run tests
pytest

# Format code
black chattercore/

# Type checking
mypy chattercore/
```

## License

MIT License - see LICENSE file for details.
