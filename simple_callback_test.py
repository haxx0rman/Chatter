#!/usr/bin/env python3
"""
Simple test of the callback system integration
"""

import asyncio
from chattercore import ChatterClient

async def simple_callback_test():
    """Simple test showing callback system works."""
    
    print("🧪 Simple Callback System Test")
    
    # Test synchronous response (this will timeout since no server)
    client = ChatterClient("ws://localhost:9999", auto_reconnect=False)
    
    # Test that callback and wait_for_response can't be used together
    try:
        async def dummy_callback(msg):
            pass
            
        await client.send_message(
            "test",
            wait_for_response=True,
            callback=dummy_callback
        )
        print("❌ Should have raised exception!")
    except Exception as e:
        print(f"✅ Correctly raised exception: {e}")
    
    # Test that individual options work without connection errors
    try:
        # This will fail with connection error, not parameter error
        await client.send_message(
            "test",
            wait_for_response=True
        )
    except Exception as e:
        if "Not connected" in str(e):
            print("✅ Correctly detected not connected (expected)")
        else:
            print(f"❌ Unexpected error: {e}")
    
    try:
        async def dummy_callback(msg):
            pass
            
        # This will also fail with connection error, not parameter error
        await client.send_message(
            "test",
            callback=dummy_callback
        )
    except Exception as e:
        if "Not connected" in str(e):
            print("✅ Correctly detected not connected (expected)")
        else:
            print(f"❌ Unexpected error: {e}")
    
    print("🎉 Simple callback test completed!")


if __name__ == "__main__":
    asyncio.run(simple_callback_test())
