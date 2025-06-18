#!/usr/bin/env python3
"""
Test script to show live Polygon.io data
"""

import asyncio
import json
from data_sources import DataSourceFactory
from datetime import datetime

async def show_live_data():
    print('🔍 CHECKING YOUR LIVE POLYGON DATA')
    print('=' * 50)
    
    config = {
        'type': 'polygon',
        'polygon': {
            'api_key': 'R0jSMnnnhxzvbqDFXMbiaJpCyDUwUHod',
            'symbols': ['QQQ'],
            'subscription_types': ['A'],
            'timeframe': '5minute',
            'data_type': 'auto'
        }
    }
    
    data_source = DataSourceFactory.create(config)
    print('📡 Connecting to Polygon.io...')
    await data_source.connect()
    print('✅ Connected!')
    
    print('🕐 Current time:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print('⏰ Your subscription gives you data delayed by ~15 minutes')
    print('📊 Waiting for live 5-minute bars from Polygon.io...')
    print('=' * 50)
    
    bar_count = 0
    
    async def data_callback(bar):
        nonlocal bar_count
        bar_count += 1
        print(f'📈 BAR #{bar_count} | {bar["symbol"]} | {bar["datetime"]}')
        print(f'   💰 O:{bar["open"]:.2f} H:{bar["high"]:.2f} L:{bar["low"]:.2f} C:{bar["close"]:.2f}')
        print(f'   📊 Volume: {bar["volume"]:,}')
        print(f'   🔗 Source: {bar["source"]} (Polygon.io)')
        print(f'   ⏱️  Received at: {bar["timestamp_received"]}')
        print('-' * 50)
        
        if bar_count >= 3:
            print('✅ Successfully receiving live Polygon data!')
            await data_source.stop_streaming()
    
    await data_source.subscribe(['QQQ'])
    
    try:
        await data_source.start_streaming(data_callback)
        # Wait for some data or timeout
        await asyncio.sleep(60)  # Wait up to 1 minute for data
    except KeyboardInterrupt:
        print('\n⏹️ Stopped by user')
    finally:
        await data_source.stop_streaming()
        await data_source.disconnect()
        print('🔌 Disconnected from Polygon.io')

if __name__ == "__main__":
    asyncio.run(show_live_data()) 