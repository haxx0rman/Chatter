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

## Request-Response with Callback IDs

ChatterCore has **native callback ID support** for request-response messaging patterns. Messages automatically include a `reply_to` field that links responses to their original requests.

### Two Routes for Handling Responses:

#### 1. Synchronous Waiting (wait_for_response=True)
Block until response is received - perfect for simple request-response flows:

```python
response = await client.send_message(
    "What is 2 + 2?",
    wait_for_response=True,
    timeout=10
)
print(f"Answer: {response.content}")
# response.reply_to automatically contains the original message ID
```

#### 2. Asynchronous Callbacks (callback=function)
Non-blocking - continue other work while waiting for response:

```python
async def response_handler(message):
    print(f"Got response: {message.content}")
    print(f"Reply to message: {message.reply_to}")

await client.send_message(
    "Process this in background",
    callback=response_handler,
    timeout=10
)
# Continue immediately with other work
```

### Server-Side Response

The server automatically creates responses linked to requests:

```python
async def request_handler(message, context):
    # Create response with reply_to field
    response = server.message_handler.create_message(
        content="Processed!",
        reply_to=message.id  # ← Callback ID linking response to request
    )
    # Send response - client automatically matches by callback ID
    await send_to_client(response)
```

**The callback ID system is built-in** - no external tracking needed! See [docs/REQUEST_RESPONSE_PATTERN.md](docs/REQUEST_RESPONSE_PATTERN.md) for complete documentation.

## Architecture

ChatterCore is built with several key components:

- **ChatterServer**: WebSocket server for handling connections
- **ChatterClient**: Client library with auto-reconnection
- **MessageHandler**: Routes and processes different message types
- **EventManager**: Handles custom events and callbacks
- **ConnectionManager**: Manages active WebSocket connections

## Development

This project uses [uv](https://github.com/astral-sh/uv) for fast, reliable dependency management.

```bash
# Clone the repository
git clone https://github.com/haxx0rman/Chatter.git
cd Chatter

# Install dependencies using uv
uv sync

# Or install in development mode
uv pip install -e ".[dev]"

# Run tests
uv run pytest

# Format code
uv run black chattercore/

# Type checking
uv run mypy chattercore/
```

### Using traditional pip

If you prefer using pip:

```bash
pip install -e .[dev]
pytest
```

## License

MIT License - see LICENSE file for details.
