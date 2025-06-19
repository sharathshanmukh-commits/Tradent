#!/usr/bin/env python3
"""
Debug the full pipeline step by step
"""

import asyncio
import json
from data_sources import DataSourceFactory
from streaming_buffer import StreamingBuffer
from patient_labels_strategy_copy import PatientLabelsStrategy
from datetime import datetime

async def debug_pipeline():
    print('🔍 DEBUGGING FULL PIPELINE')
    print('=' * 60)
    
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
    
    print('🔧 Creating components...')
    data_source = DataSourceFactory.create(config)
    buffer = StreamingBuffer()
    
    try:
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
        print('✅ Strategy created successfully')
    except Exception as e:
        print(f'❌ Strategy creation failed: {e}')
        return
    
    bars_received = 0
    
    async def debug_callback(bar):
        nonlocal bars_received
        bars_received += 1
        
        print(f'\n📊 STEP 1: BAR #{bars_received} RECEIVED')
        print(f'   Time: {datetime.now().strftime("%H:%M:%S")}')
        print(f'   Bar datetime: {bar.get("datetime", "N/A")}')
        print(f'   Bar data: {bar}')
        
        # STEP 2: Add to buffer
        print(f'\n📊 STEP 2: Adding to buffer...')
        try:
            success = buffer.append_bar(bar)
            print(f'   Buffer add result: {"✅ Success" if success else "❌ Failed"}')
            print(f'   Buffer size: {len(buffer.df)} rows')
            
            if len(buffer.df) > 0:
                print(f'   Buffer columns: {list(buffer.df.columns)}')
                print(f'   Latest row: {buffer.df.iloc[-1].to_dict()}')
            else:
                print('   ❌ Buffer is empty!')
                
        except Exception as e:
            print(f'   ❌ Buffer error: {e}')
            import traceback
            traceback.print_exc()
            return
        
        # STEP 3: Try strategy when we have enough data
        if len(buffer.df) >= 3:
            print(f'\n📊 STEP 3: Running Patient Labels Strategy...')
            try:
                df = buffer.get_dataframe()
                print(f'   Input DataFrame shape: {df.shape}')
                print(f'   Input columns: {list(df.columns)}')
                print(f'   First few datetime values: {df["datetime"].head().tolist() if "datetime" in df else "No datetime column"}')
                
                # Debug session times
                if "datetime" in df:
                    sample_dt = df.iloc[-1]["datetime"]
                    print(f'   Latest bar time: {sample_dt}')
                    print(f'   Latest bar time (local): {sample_dt.tz_convert("America/New_York") if sample_dt.tzinfo else "No timezone"}')
                
                # Process with strategy
                print('   🧠 Calling strategy.process_data()...')
                result_df = strategy.process_data(df)
                
                print(f'   ✅ Strategy completed!')
                print(f'   Output shape: {result_df.shape}')
                print(f'   Output columns: {list(result_df.columns)}')
                
                # Check for signals
                signals = strategy.get_signals()
                print(f'   🚨 Total signals: {len(signals)}')
                
                if len(signals) > 0:
                    latest_signal = signals[-1]
                    print(f'   Latest signal: {latest_signal}')
                
                # Check signal columns in DataFrame
                if 'signal' in result_df.columns:
                    signal_count = (result_df['signal'] != 0).sum()
                    print(f'   Signals in DataFrame: {signal_count}')
                    if signal_count > 0:
                        signal_rows = result_df[result_df['signal'] != 0]
                        print(f'   Signal details: {signal_rows[["datetime", "signal", "entry_price", "stop_loss"]].to_dict("records")}')
                    else:
                        print('   No signals generated (all signal values are 0)')
                else:
                    print('   No signal column in output DataFrame')
                
                # Stop after first successful run
                await data_source.stop_streaming()
                
            except Exception as e:
                print(f'   ❌ Strategy error: {e}')
                import traceback
                traceback.print_exc()
                await data_source.stop_streaming()
        else:
            print(f'\n📊 STEP 3: Waiting for more data ({len(buffer.df)}/3 bars)')
    
    try:
        print('\n🔌 Connecting to Polygon.io...')
        await data_source.connect()
        await data_source.subscribe(['QQQ'])
        
        print('📡 Starting stream (will stop after strategy runs or 2 minutes)...')
        print('=' * 60)
        
        # Start streaming
        streaming_task = asyncio.create_task(
            data_source.start_streaming(debug_callback)
        )
        
        # Wait up to 2 minutes
        await asyncio.sleep(120)
        
    except Exception as e:
        print(f'❌ Pipeline error: {e}')
        import traceback
        traceback.print_exc()
    finally:
        await data_source.disconnect()
        
        print('\n' + '=' * 60)
        print('📊 FINAL RESULTS:')
        print(f'   Bars received: {bars_received}')
        print(f'   Buffer size: {len(buffer.df)}')
        
        if len(buffer.df) > 0:
            print(f'   Buffer columns: {list(buffer.df.columns)}')
            print(f'   Sample data: {buffer.df.head().to_dict("records")}')
        
        try:
            signals = strategy.get_signals()
            print(f'   Total signals generated: {len(signals)}')
        except:
            print(f'   Could not get signals from strategy')

if __name__ == "__main__":
    asyncio.run(debug_pipeline()) 