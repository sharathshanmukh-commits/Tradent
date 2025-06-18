#!/usr/bin/env python3
"""
Simple debug script to check Polygon.io raw messages
"""

import asyncio
import json
import websockets
from datetime import datetime

async def debug_raw_polygon():
    print('🔍 RAW POLYGON.IO DEBUG')
    print('=' * 50)
    
    api_key = 'R0jSMnnnhxzvbqDFXMbiaJpCyDUwUHod'
    url = 'wss://delayed.polygon.io/stocks'
    
    message_count = 0
    
    try:
        print(f'🔌 Connecting to {url}...')
        
        async with websockets.connect(url) as websocket:
            print('✅ Connected!')
            
            # Authenticate
            auth_msg = {"action": "auth", "params": api_key}
            await websocket.send(json.dumps(auth_msg))
            print('🔑 Sent authentication...')
            
            # Subscribe to QQQ minute aggregates
            sub_msg = {"action": "subscribe", "params": "AM.QQQ"}
            await websocket.send(json.dumps(sub_msg))
            print('📡 Subscribed to AM.QQQ (1-minute aggregates)...')
            
            print(f'⏰ Current time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S ET")}')
            print('📨 Listening for 45 seconds...')
            print('=' * 50)
            
            # Listen for messages for 45 seconds
            start_time = asyncio.get_event_loop().time()
            while asyncio.get_event_loop().time() - start_time < 45:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    message_count += 1
                    
                    print(f'📨 MESSAGE #{message_count}:')
                    data = json.loads(message)
                    print(f'   Raw: {message}')
                    
                    # Parse messages
                    if isinstance(data, list):
                        for msg in data:
                            msg_type = msg.get('ev', 'unknown')
                            if msg_type == 'status':
                                print(f'   📊 Status: {msg.get("status")} - {msg.get("message", "")}')
                            elif msg_type == 'AM':  # Minute aggregate
                                symbol = msg.get('sym', '')
                                timestamp = msg.get('s', 0)
                                dt = datetime.fromtimestamp(timestamp / 1000)
                                print(f'   🎯 MINUTE BAR: {symbol} at {dt.strftime("%H:%M:%S")}')
                                print(f'      O:{msg.get("o", 0)} H:{msg.get("h", 0)} L:{msg.get("l", 0)} C:{msg.get("c", 0)} V:{msg.get("v", 0)}')
                            else:
                                print(f'   🔍 Other: {msg_type} -> {msg}')
                    else:
                        print(f'   📄 Single message: {data}')
                    
                    print('-' * 30)
                    
                except asyncio.TimeoutError:
                    print('.', end='', flush=True)  # Show we're still waiting
                    
            print(f'\n\n📊 SUMMARY:')
            print(f'   Total messages received: {message_count}')
            
            if message_count == 0:
                print('❌ NO MESSAGES - Possible issues:')
                print('   • Market is closed')
                print('   • API key issues')
                print('   • Symbol not trading')
                print('   • Subscription issues')
            else:
                print('✅ Messages received - check above for data bars')
                
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_raw_polygon()) 