# ChatterCore Callback System

The ChatterCore callback system provides two different approaches for handling message responses:

1. **Synchronous Waiting** - Wait for a response and get it returned
2. **Asynchronous Callbacks** - Register a function to be called when the response arrives

## Route 1: Synchronous Waiting

Use `wait_for_response=True` when sending messages to wait synchronously for a response:

```python
import asyncio
from chattercore import ChatterClient

async def sync_example():
    client = ChatterClient("ws://localhost:8765")
    await client.connect()
    
    # Send message and wait for response
    response = await client.send_message(
        "Hello server, please respond!",
        wait_for_response=True,
        timeout=10  # Optional timeout in seconds
    )
    
    print(f"Received response: {response.content}")
    await client.disconnect()
```

### Advantages of Synchronous Waiting:
- Simple linear code flow
- Direct access to response data
- Easy error handling with try/catch
- Good for request-response patterns

### Disadvantages:
- Blocks execution until response arrives
- Not suitable for high-throughput scenarios
- Can lead to timeouts if server is slow

## Route 2: Asynchronous Callbacks

Use the `callback` parameter to register a function that gets called when the response arrives:

```python
import asyncio
from chattercore import ChatterClient, Message

async def response_handler(message: Message):
    """Called when response is received"""
    print(f"Async response: {message.content}")
    print(f"Reply to message: {message.reply_to}")
    # Handle the response asynchronously

async def async_example():
    client = ChatterClient("ws://localhost:8765")
    await client.connect()
    
    # Send message with callback
    await client.send_message(
        "Hello server, call my callback!",
        callback=response_handler,
        timeout=10  # Optional timeout for cleanup
    )
    
    # Continue with other work immediately
    print("Message sent, continuing with other work...")
    await asyncio.sleep(2)  # Do other work
    
    await client.disconnect()
```

### Advantages of Asynchronous Callbacks:
- Non-blocking - continue other work immediately
- Better for high-throughput applications
- Suitable for event-driven architectures
- Can handle multiple responses in parallel

### Disadvantages:
- More complex code structure
- Harder to debug
- Need to manage callback lifecycle

## Advanced Usage

### Multiple Messages with Different Routes

You can mix both approaches in the same application:

```python
async def mixed_example():
    client = ChatterClient("ws://localhost:8765")
    await client.connect()
    
    # Some messages need immediate responses
    important_response = await client.send_message(
        "Critical request",
        wait_for_response=True,
        timeout=5
    )
    
    # Others can be handled asynchronously
    async def log_response(msg: Message):
        print(f"Logged: {msg.content}")
    
    await client.send_message(
        "Background task",
        callback=log_response
    )
    
    await client.disconnect()
```

### Direct MessageHandler Usage

You can also use the callback system directly through the MessageHandler:

```python
from chattercore import MessageHandler, Message, MessageType

handler = MessageHandler()

# Wait for response directly
response = await handler.wait_for_response("message-id", timeout=10)

# Register callback directly
async def my_callback(message: Message):
    print(f"Got response: {message.content}")

handler.register_callback("message-id", my_callback, timeout=30)

# Process a response (this would normally come from the network)
response_msg = Message(
    content="This is the response",
    message_type=MessageType.TEXT,
    reply_to="message-id"
)

# This will trigger the callback or resolve the wait
await handler.process_message(response_msg)
```

### Error Handling

Both routes support timeout handling:

```python
# Synchronous with timeout
try:
    response = await client.send_message(
        "Request",
        wait_for_response=True,
        timeout=5
    )
except asyncio.TimeoutError:
    print("No response received within 5 seconds")

# Asynchronous with timeout (callback gets cleaned up automatically)
await client.send_message(
    "Request",
    callback=my_callback,
    timeout=5  # Callback removed after 5 seconds if no response
)
```

### Callback System Statistics

Monitor the callback system performance:

```python
# Get statistics
stats = client.message_handler.callback_handler.get_stats()
print(f"Pending responses: {stats['pending_responses']}")
print(f"Active callbacks: {stats['active_callbacks']}")
print(f"Timeout tasks: {stats['timeout_tasks']}")

# Clean up all callbacks (useful for shutdown)
client.message_handler.callback_handler.cancel_all()
```

## Best Practices

1. **Use synchronous waiting for:**
   - Critical requests that need immediate responses
   - Simple request-response patterns
   - Error-sensitive operations

2. **Use asynchronous callbacks for:**
   - High-throughput messaging
   - Background processing
   - Event-driven architectures
   - When you need to continue other work immediately

3. **Timeout considerations:**
   - Always set reasonable timeouts
   - Consider network latency and server processing time
   - Use shorter timeouts for interactive applications
   - Use longer timeouts for background processing

4. **Error handling:**
   - Wrap synchronous waits in try/catch blocks
   - Design callbacks to handle errors gracefully
   - Monitor callback system statistics

5. **Resource management:**
   - Call `cancel_all()` during shutdown
   - Monitor active callbacks to prevent memory leaks
   - Use appropriate timeouts to prevent resource buildup

## Integration Examples

See the `examples/callback_demo.py` file for a comprehensive demonstration of both callback routes in action.
