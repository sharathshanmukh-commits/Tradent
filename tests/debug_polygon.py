#!/usr/bin/env python3
"""
Debug script to check Polygon.io data flow
"""

import asyncio
from data_sources import DataSourceFactory
from datetime import datetime

async def debug_polygon():
    print('🔍 DEBUGGING POLYGON.IO DATA FLOW')
    print('=' * 50)
    
    config = {
        'type': 'polygon',
        'polygon': {
            'api_key': 'R0jSMnnnhxzvbqDFXMbiaJpCyDUwUHod',
            'symbols': ['QQQ'],
            'subscription_types': ['A'],
            'timeframe': '1minute',
            'data_type': 'auto'
        }
    }
    
    data_source = DataSourceFactory.create(config)
    
    message_count = 0
    raw_message_count = 0
    
    # Override the _process_message method to see all raw messages
    original_process = data_source._process_message
    
    async def debug_process_message(message):
        nonlocal raw_message_count
        raw_message_count += 1
        print(f'📨 RAW MESSAGE #{raw_message_count}: {message}')
        return await original_process(message)
    
    data_source._process_message = debug_process_message
    
    async def debug_callback(bar):
        nonlocal message_count
        message_count += 1
        print(f'📊 PROCESSED BAR #{message_count}:')
        print(f'   Symbol: {bar["symbol"]}')
        print(f'   DateTime: {bar["datetime"]}')
        print(f'   Close: ${bar["close"]:.2f}')
        print(f'   Volume: {bar["volume"]:,}')
        print('-' * 30)
    
    try:
        print('🔌 Connecting...')
        await data_source.connect()
        
        print('📡 Subscribing to QQQ...')
        await data_source.subscribe(['QQQ'])
        
        print(f'⏰ Current time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S ET")}')
        print('📡 Listening for 30 seconds for ANY messages...')
        print('=' * 50)
        
        # Start streaming
        streaming_task = asyncio.create_task(
            data_source.start_streaming(debug_callback)
        )
        
        # Wait 30 seconds
        await asyncio.sleep(30)
        
        print(f'\n📊 RESULTS after 30 seconds:')
        print(f'   Raw messages received: {raw_message_count}')
        print(f'   Processed bars: {message_count}')
        
        if raw_message_count == 0:
            print('❌ NO MESSAGES AT ALL - Connection issue or market closed')
        elif message_count == 0:
            print('⚠️  Messages received but no bars processed - Check message format')
        else:
            print('✅ Data flowing correctly!')
        
        await data_source.stop_streaming()
        await data_source.disconnect()
        
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_polygon()) 