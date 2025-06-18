#!/usr/bin/env python3
"""
Test live data compatibility with Patient Labels strategy
"""

import asyncio
import pandas as pd
from data_sources import DataSourceFactory
from streaming_buffer import StreamingBuffer
from patient_labels_strategy_copy import PatientLabelsStrategy

async def test_compatibility():
    print('🔍 TESTING LIVE DATA COMPATIBILITY')
    print('=' * 50)
    
    # Create components
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
    buffer = StreamingBuffer()
    strategy = PatientLabelsStrategy(
        risk_reward_ratio=1.0,
        cycles_required=3,
        display_duration=5,
        use_hod_lod_breaks=True,
        use_confirmed_swings=True,
        preserve_dominant_trend=True,
        session_start_time='09:30',
        session_end_time='16:00',
        session_timezone='America/New_York',
        strategy_buffer=0.10
    )
    
    bars_received = 0
    
    async def test_callback(bar):
        nonlocal bars_received
        bars_received += 1
        
        print(f'\n📊 BAR #{bars_received}:')
        print(f'   Structure: {list(bar.keys())}')
        print(f'   Sample values:')
        for key, value in bar.items():
            print(f'     {key}: {value} ({type(value).__name__})')
        
        # Add to buffer
        success = buffer.append_bar(bar)
        print(f'   Buffer add: {"✅ Success" if success else "❌ Failed"}')
        
        # Test strategy when we have enough data
        if len(buffer.df) >= 5:
            print(f'\n🧠 Testing strategy with {len(buffer.df)} bars...')
            try:
                df = buffer.get_dataframe()
                print(f'   DataFrame columns: {list(df.columns)}')
                print(f'   DataFrame dtypes:')
                for col, dtype in df.dtypes.items():
                    print(f'     {col}: {dtype}')
                
                # Test strategy processing
                result_df = strategy.process_data(df)
                print(f'   ✅ Strategy processing successful!')
                print(f'   Output columns: {list(result_df.columns)}')
                
                # Check for signals
                signals = strategy.get_signals()
                print(f'   🚨 Signals generated: {len(signals)}')
                
                if len(signals) > 0:
                    latest_signal = signals[-1]
                    print(f'   Latest signal: {latest_signal["type"]} at ${latest_signal["entry_price"]:.2f}')
                
                # Stop after successful test
                await data_source.stop_streaming()
                
            except Exception as e:
                print(f'   ❌ Strategy error: {e}')
                import traceback
                traceback.print_exc()
                await data_source.stop_streaming()
    
    try:
        print('🔌 Connecting to Polygon.io...')
        await data_source.connect()
        await data_source.subscribe(['QQQ'])
        
        print('📡 Waiting for live bars (up to 5 minutes)...')
        await data_source.start_streaming(test_callback)
        
        # Wait up to 5 minutes for enough data
        await asyncio.sleep(300)
        
    except Exception as e:
        print(f'❌ Test error: {e}')
        import traceback
        traceback.print_exc()
    finally:
        await data_source.disconnect()
        
        print('\n📊 TEST SUMMARY:')
        print(f'   Bars received: {bars_received}')
        print(f'   Buffer size: {len(buffer.df)}')
        print(f'   Strategy tested: {"✅ Yes" if len(buffer.df) >= 5 else "❌ No (insufficient data)"}')

if __name__ == "__main__":
    asyncio.run(test_compatibility()) 