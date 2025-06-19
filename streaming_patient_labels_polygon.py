#!/usr/bin/env python3
"""
Streaming Patient Labels with Polygon.io Data Source
Real-time strategy execution using the data source abstraction layer
"""

import asyncio
import json
import argparse
import logging
import sys
import signal
from typing import Optional, Dict, Any
from datetime import datetime
import threading
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from data_sources import DataSourceFactory
from streaming_buffer import StreamingBuffer
from patient_labels_strategy_copy import PatientLabelsStrategy
from event_system import EventSystem, ConsoleLogger, EventType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PolygonStreamingRunner:
    """Main runner for streaming patient labels with Polygon.io data"""
    
    def __init__(self, config_path: str = "config_data_sources.json"):
        """Initialize runner with configuration"""
        self.config = self._load_config(config_path)
        self.data_source = None
        self.buffer = None
        self.strategy = None
        self.event_system = None
        self.is_running = False
        self.signal_count = 0
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found, using defaults")
            return self._get_default_config()
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration for Polygon data source"""
        return {
            "data_source": {
                "type": "polygon",
                "polygon": {
                    "api_key": "R0jSMnnnhxzvbqDFXMbiaJpCyDUwUHod",
                    "symbols": ["QQQ"],
                    "subscription_types": ["A"],
                    "timeframe": "1minute",
                    "data_type": "auto",
                    "reconnect_attempts": 3,
                    "reconnect_delay_ms": 5000
                }
            }
        }
    
    def setup_components(self):
        """Setup all system components"""
        # Create event system
        self.event_system = EventSystem()
        
        # Create console handler for events
        console_handler = ConsoleLogger(verbose=True)
        self.event_system.register_global_handler(console_handler)
        
        # Create data source
        self.data_source = DataSourceFactory.create(self.config['data_source'])
        
        # Create buffer
        self.buffer = StreamingBuffer(max_rows=None)
        
        # Create strategy with event system
        strategy_config = {
            'risk_reward_ratio': 1.0,
            'cycles_required': 3,
            'display_duration': 5,
            'use_hod_lod_breaks': True,
            'use_confirmed_swings': True,
            'preserve_dominant_trend': True,
            'session_start_time': '09:30',
            'session_end_time': '16:00',
            'session_timezone': 'America/New_York',
            'strategy_buffer': 0.10
        }
        
        self.strategy = PatientLabelsStrategy(
            risk_reward_ratio=strategy_config['risk_reward_ratio'],
            cycles_required=strategy_config['cycles_required'], 
            display_duration=strategy_config['display_duration'],
            use_hod_lod_breaks=strategy_config['use_hod_lod_breaks'],
            use_confirmed_swings=strategy_config['use_confirmed_swings'],
            preserve_dominant_trend=strategy_config['preserve_dominant_trend'],
            session_start_time=strategy_config['session_start_time'],
            session_end_time=strategy_config['session_end_time'],
            session_timezone=strategy_config['session_timezone'],
            strategy_buffer=strategy_config['strategy_buffer']
        )
        
        logger.info("✅ All components initialized")
    
    async def _handle_signal(self, event_data):
        """Handle new trading signals"""
        self.signal_count += 1
        signal_data = event_data['signal']
        
        print(f"\n🚨 [{datetime.now().strftime('%H:%M:%S')}] NEW SIGNAL: {signal_data['type']}")
        print(f"   Entry: ${signal_data['entry_price']:.2f}")
        print(f"   Stop Loss: ${signal_data['stop_loss']:.2f}")
        print(f"   Take Profit: ${signal_data.get('take_profit', 0.00):.2f}")
        print(f"   Risk/Reward: {signal_data.get('risk_reward_ratio', 0.0):.2f}")
    
    async def data_callback(self, bar):
        """Handle incoming data bars from Polygon.io"""
        # Add to buffer
        self.buffer.append_bar(bar)
        
        # Process with strategy every few bars (or when we have enough data)
        if len(self.buffer.df) >= 2:  # Minimum 2 bars for live data analysis
            # Get current DataFrame
            current_df = self.buffer.get_dataframe()
            
            # Process with strategy (this adds signal columns)
            try:
                result_df = self.strategy.process_data(current_df)
                
                # Check for new signals in the latest bar
                if len(result_df) > 0:
                    latest_bar = result_df.iloc[-1]
                    if latest_bar.get('signal', 0) != 0:  # Non-zero signal
                        self.signal_count += 1
                        signal_type = "BUY" if latest_bar['signal'] > 0 else "SELL"
                        print(f"\n🚨 [{datetime.now().strftime('%H:%M:%S')}] NEW SIGNAL: {signal_type}")
                        print(f"   Entry: ${latest_bar.get('entry_price', 0):.2f}")
                        print(f"   Stop Loss: ${latest_bar.get('stop_loss', 0):.2f}")
                        print(f"   Target: ${latest_bar.get('target_price', 0):.2f}")
                        print(f"   🎯 Strategy processed {len(current_df)} bars successfully!")
                    else:
                        print(f"   📊 Strategy processed {len(current_df)} bars (no signals)")
            except Exception as e:
                print(f"   ❌ Strategy error: {e}")
        
        # Display bar info
        current_time = datetime.now().strftime('%H:%M:%S')
        print(f"[{current_time}] 📊 {bar['symbol']} | {bar['datetime']} | "
              f"Close: ${bar['close']:.2f} | Volume: {bar['volume']:,} | "
              f"Buffer: {len(self.buffer.df)} bars")
    
    async def run_stream(self, duration_minutes: Optional[int] = None):
        """Run the live data stream"""
        try:
            # Setup components
            self.setup_components()
            
            # Connect to data source
            logger.info("🔌 Connecting to Polygon.io...")
            await self.data_source.connect()
            logger.info("✅ Connected to Polygon.io!")
            
            # Subscribe to symbols
            symbols = self.config['data_source']['polygon']['symbols']
            logger.info(f"📡 Subscribing to symbols: {symbols}")
            await self.data_source.subscribe(symbols)
            
            # Display dashboard
            self.display_dashboard()
            
            # Start streaming
            self.is_running = True
            print(f"🚀 Starting live stream from Polygon.io...")
            print(f"📊 Subscribed to: {', '.join(symbols)}")
            print(f"⏰ Data delay: ~15 minutes (your subscription level)")
            print("=" * 60)
            
            # Start streaming with callback
            streaming_task = asyncio.create_task(
                self.data_source.start_streaming(self.data_callback)
            )
            
            # Run for specified duration or until interrupted
            if duration_minutes:
                await asyncio.sleep(duration_minutes * 60)
                logger.info(f"⏰ Duration limit reached ({duration_minutes} minutes)")
            else:
                # Run indefinitely until interrupted
                await streaming_task
            
        except KeyboardInterrupt:
            logger.info("\n⏹️ Stream stopped by user")
        except Exception as e:
            logger.error(f"❌ Error during streaming: {e}")
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.data_source:
            try:
                await self.data_source.stop_streaming()
                await self.data_source.disconnect()
                logger.info("🔌 Disconnected from Polygon.io")
            except Exception as e:
                logger.warning(f"Warning during cleanup: {e}")
        
        # Save buffer data
        if self.buffer and len(self.buffer.df) > 0:
            output_dir = Path("output")
            output_dir.mkdir(exist_ok=True)
            
            buffer_file = output_dir / "polygon_data.csv"
            self.buffer.to_csv(str(buffer_file))
            logger.info(f"💾 Data saved to {buffer_file}")
        
        self.display_summary()
    
    def display_dashboard(self):
        """Display live dashboard"""
        print("\n" + "="*60)
        print("🎯 POLYGON.IO LIVE STREAMING - PATIENT LABELS")
        print("="*60)
        print("📊 Data Source: Polygon.io WebSocket (15-min delayed)")
        print("⚡ Strategy: Patient Labels Signal Detection")
        print("🎯 Symbols: QQQ (1-minute bars)")
        print("\nCommands:")
        print("  [Ctrl+C] - Stop streaming")
        print("="*60 + "\n")
    
    def display_summary(self):
        """Display session summary"""
        bars_processed = len(self.buffer.df) if self.buffer else 0
        
        print("\n" + "="*60)
        print("📊 POLYGON.IO STREAMING SESSION SUMMARY")
        print("="*60)
        print(f"📈 Bars Processed: {bars_processed}")
        print(f"🚨 Signals Generated: {self.signal_count}")
        print(f"📊 Data Source: Polygon.io (live)")
        print(f"💾 Output Directory: output/")
        print("="*60)


def setup_signal_handlers():
    """Setup graceful shutdown handlers"""
    def signal_handler(sig, frame):
        logger.info("\n🛑 Shutting down gracefully...")
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Streaming Patient Labels with Polygon.io - Real-time strategy execution",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run live stream (default)
  python streaming_patient_labels_polygon.py
  
  # Run for 30 minutes then stop
  python streaming_patient_labels_polygon.py --duration 30
  
  # Use custom config file
  python streaming_patient_labels_polygon.py --config my_polygon_config.json
  
  # Test connection only
  python streaming_patient_labels_polygon.py --test-connection
        """
    )
    
    parser.add_argument('--config', default='config_data_sources.json',
                       help='Path to data source configuration file')
    parser.add_argument('--duration', type=int,
                       help='Run for specified minutes then stop')
    parser.add_argument('--test-connection', action='store_true',
                       help='Test Polygon.io connection and exit')
    
    args = parser.parse_args()
    
    # Setup signal handlers
    setup_signal_handlers()
    
    # Create runner
    runner = PolygonStreamingRunner(args.config)
    
    if args.test_connection:
        # Test connection only
        print("🔍 Testing Polygon.io connection...")
        try:
            runner.setup_components()
            await runner.data_source.connect()
            print("✅ Connection successful!")
            await runner.data_source.disconnect()
        except Exception as e:
            print(f"❌ Connection failed: {e}")
        return
    
    # Run the stream
    try:
        await runner.run_stream(duration_minutes=args.duration)
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 