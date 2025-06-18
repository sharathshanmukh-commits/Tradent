#!/usr/bin/env python3
"""
Patient Labels Trading System
Switch between CSV and Polygon by changing config.json

Usage:
    python main.py                    # Run with default config
    python main.py --config my.json  # Run with custom config
    python main.py --duration 10     # Run for 10 minutes
"""

import asyncio
import json
import argparse
import logging
import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from data_sources import DataSourceFactory
from strategy.streaming_buffer import StreamingBuffer
from strategy.patient_labels_strategy import PatientLabelsStrategy

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PatientLabelsSystem:
    """Main Patient Labels Trading System"""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize with configuration file"""
        self.config = self._load_config(config_path)
        self.data_source = None
        self.buffer = None
        self.strategy = None
        self.signal_count = 0
        self.bars_processed = 0
        self.signals_data = []  # Store signals for saving
        self.strategy_results = None  # Store full strategy results
        self.event_log = []  # Store events for logging
        self.session_start_time = datetime.now()
        self.output_dir = self._create_output_directory()
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"✅ Loaded config from {config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"❌ Config file not found: {config_path}")
            logger.info("💡 Creating default config.json...")
            self._create_default_config(config_path)
            return self._load_config(config_path)
        except Exception as e:
            logger.error(f"❌ Error loading config: {e}")
            sys.exit(1)
    
    def _create_default_config(self, config_path: str):
        """Create default configuration file"""
        default_config = {
            "_comment": "Patient Labels Trading System Configuration",
            "_switch_data_source": "Change 'data_source' to 'csv' or 'polygon'",
            
            "data_source": "csv",
            
            "csv": {
                "file_path": "data/QQQ_5min_2025-05-01_to_2025-05-01.csv",
                "speed": 10.0,
                "symbol": "QQQ"
            },
            
            "polygon": {
                "api_key": "R0jSMnnnhxzvbqDFXMbiaJpCyDUwUHod",
                "symbols": ["QQQ"],
                "timeframe": "1minute",
                "data_type": "auto"
            },
            
            "strategy": {
                "risk_reward_ratio": 1.0,
                "cycles_required": 3,
                "display_duration": 5,
                "use_hod_lod_breaks": True,
                "use_confirmed_swings": True,
                "preserve_dominant_trend": True,
                "session_start_time": "09:30",
                "session_end_time": "16:00",
                "session_timezone": "America/New_York",
                "strategy_buffer": 0.10
            },
            
            "output": {
                "directory": "output",
                "save_data": True,
                "save_signals": True
            }
        }
        
        with open(config_path, 'w') as f:
            json.dump(default_config, f, indent=2)
        logger.info(f"✅ Created default config: {config_path}")
    
    def _create_output_directory(self) -> Path:
        """Create timestamped output directory"""
        base_dir = Path(self.config['output']['directory'])
        
        # Get data source info
        source_type = self.config['data_source']
        timestamp = self.session_start_time.strftime('%Y-%m-%d_%H-%M-%S')
        
        # Create descriptive directory name
        if source_type == 'csv':
            symbol = self.config['csv'].get('symbol', 'UNKNOWN')
            speed = self.config['csv'].get('speed', 1.0)
            session_dir = base_dir / f"{source_type}_{symbol}_{speed}x_{timestamp}"
        else:  # polygon
            symbols = '_'.join(self.config['polygon'].get('symbols', ['UNKNOWN']))
            timeframe = self.config['polygon'].get('timeframe', '1min')
            session_dir = base_dir / f"{source_type}_{symbols}_{timeframe}_{timestamp}"
        
        # Create directory
        session_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Created session directory: {session_dir}")
        
        return session_dir
    
    def _log_event(self, event_type: str, data: dict):
        """Log events for later saving to events.log"""
        event = {
            'timestamp': datetime.now().isoformat(),
            'type': event_type,
            'data': data
        }
        self.event_log.append(event)
    
    def setup_components(self):
        """Setup all system components"""
        logger.info("🔧 Setting up components...")
        
        # Create data source based on config
        data_source_type = self.config['data_source']
        logger.info(f"📊 Data source: {data_source_type}")
        
        if data_source_type == 'csv':
            source_config = {
                'type': 'csv',
                'csv': self.config['csv']
            }
        elif data_source_type == 'polygon':
            source_config = {
                'type': 'polygon', 
                'polygon': self.config['polygon']
            }
        else:
            raise ValueError(f"Unknown data source: {data_source_type}")
        
        self.data_source = DataSourceFactory.create(source_config)
        
        # Create buffer
        self.buffer = StreamingBuffer()
        
        # Create strategy
        strategy_config = self.config['strategy']
        self.strategy = PatientLabelsStrategy(**strategy_config)
        
        logger.info("✅ All components ready")
    
    async def process_bar(self, bar):
        """Process incoming data bar"""
        self.bars_processed += 1
        
        # Log new bar event
        self._log_event("NEW_BAR", {
            'bar_count': self.bars_processed,
            'datetime': bar.get('datetime', ''),
            'symbol': bar.get('symbol', ''),
            'close': bar.get('close', 0),
            'volume': bar.get('volume', 0)
        })
        
        # Add to buffer
        self.buffer.append_bar(bar)
        
        # Process with strategy when we have enough data
        min_bars = 3 if self.config['data_source'] == 'polygon' else 5
        
        if len(self.buffer.df) >= min_bars:
            try:
                # Process with strategy
                current_df = self.buffer.get_dataframe()
                result_df = self.strategy.process_data(current_df)
                
                # Store full strategy results for saving
                self.strategy_results = result_df
                
                # Check for new signals
                if len(result_df) > 0:
                    latest_bar = result_df.iloc[-1]
                    if latest_bar.get('signal', 0) != 0:
                        self.signal_count += 1
                        signal_type = "BUY" if latest_bar['signal'] > 0 else "SELL"
                        
                        # Store signal data for saving (matching reference format)
                        signal_data = {
                            'timestamp': datetime.now().isoformat(),
                            'datetime': str(latest_bar.get('datetime', '')),
                            'signal': latest_bar['signal'],
                            'entry_price': latest_bar.get('entry_price', 0),
                            'stop_loss': latest_bar.get('stop_loss', 0),
                            'take_profit': latest_bar.get('target_price', 0),
                            'risk_reward': self.config['strategy']['risk_reward_ratio']
                        }
                        self.signals_data.append(signal_data)
                        
                        # Log signal event
                        self._log_event("NEW_SIGNAL", {
                            'signal_count': self.signal_count,
                            'type': signal_type,
                            'entry_price': signal_data['entry_price'],
                            'stop_loss': signal_data['stop_loss'],
                            'take_profit': signal_data['take_profit']
                        })
                        
                        print(f"\n🚨 [{datetime.now().strftime('%H:%M:%S')}] NEW SIGNAL #{self.signal_count}: {signal_type}")
                        print(f"   💰 Entry: ${latest_bar.get('entry_price', 0):.2f}")
                        print(f"   🛡️  Stop: ${latest_bar.get('stop_loss', 0):.2f}")
                        print(f"   🎯 Target: ${latest_bar.get('target_price', 0):.2f}")
                        print(f"   📊 Processed {len(current_df)} bars")
                
            except Exception as e:
                logger.error(f"❌ Strategy error: {e}")
                # Log error event
                self._log_event("ERROR", {'error': str(e)})
        
        # Display progress
        current_time = datetime.now().strftime('%H:%M:%S')
        source_type = self.config['data_source'].upper()
        print(f"[{current_time}] 📊 {source_type} | {bar['symbol']} | {bar['datetime']} | "
              f"${bar['close']:.2f} | Vol: {bar['volume']:,} | Buffer: {len(self.buffer.df)}")
    
    async def run(self, duration_minutes: Optional[int] = None):
        """Run the trading system"""
        try:
            # Setup components
            self.setup_components()
            
            # Connect to data source
            data_source_type = self.config['data_source'].upper()
            logger.info(f"🔌 Connecting to {data_source_type} data source...")
            await self.data_source.connect()
            logger.info(f"✅ Connected to {data_source_type}!")
            
            # Subscribe to symbols
            if self.config['data_source'] == 'csv':
                symbols = [self.config['csv'].get('symbol', 'QQQ')]
            else:
                symbols = self.config['polygon']['symbols']
            
            await self.data_source.subscribe(symbols)
            logger.info(f"📡 Subscribed to: {', '.join(symbols)}")
            
            # Display dashboard
            self._display_dashboard()
            
            # Start streaming
            logger.info("🚀 Starting data stream...")
            await self.data_source.start_streaming(self.process_bar)
            
            # Wait for specified duration or until interrupted
            if duration_minutes:
                await asyncio.sleep(duration_minutes * 60)
                logger.info(f"⏰ Duration limit reached ({duration_minutes} minutes)")
            else:
                # Keep running indefinitely until interrupted
                try:
                    while self.data_source.is_streaming:
                        await asyncio.sleep(1)
                except KeyboardInterrupt:
                    pass
                
        except KeyboardInterrupt:
            logger.info("\n⏹️ Stopped by user")
        except Exception as e:
            logger.error(f"❌ System error: {e}")
        finally:
            await self._cleanup()
    
    def _display_dashboard(self):
        """Display system dashboard"""
        source_type = self.config['data_source'].upper()
        symbols = self.config['polygon']['symbols'] if self.config['data_source'] == 'polygon' else [self.config['csv'].get('symbol', 'QQQ')]
        
        print("\n" + "="*60)
        print("🎯 PATIENT LABELS TRADING SYSTEM")
        print("="*60)
        print(f"📊 Data Source: {source_type}")
        print(f"⚡ Strategy: Patient Labels Signal Detection")
        print(f"🎯 Symbols: {', '.join(symbols)}")
        if self.config['data_source'] == 'polygon':
            print(f"⏰ Data: Live (15-min delayed)")
        else:
            print(f"⚡ Speed: {self.config['csv'].get('speed', 1.0)}x")
        print("\nCommands:")
        print("  [Ctrl+C] - Stop system")
        print("="*60 + "\n")
    
    async def _cleanup(self):
        """Cleanup and save results to timestamped directory"""
        if self.data_source:
            try:
                await self.data_source.stop_streaming()
                await self.data_source.disconnect()
                logger.info("🔌 Disconnected from data source")
            except Exception as e:
                logger.warning(f"Warning during cleanup: {e}")
        
        # Use timestamped directory instead of config directory
        output_dir = self.output_dir
        
        # FILE 1: processed_data.csv (raw buffer data)
        if self.config['output']['save_data'] and self.buffer and len(self.buffer.df) > 0:
            data_file = output_dir / "processed_data.csv"
            self.buffer.to_csv(str(data_file))
            logger.info(f"💾 Processed data saved to {data_file}")
        
        # FILE 2: strategy_results.csv (NEW - full strategy output with indicators)
        if self.strategy_results is not None and len(self.strategy_results) > 0:
            results_file = output_dir / "strategy_results.csv"
            self.strategy_results.to_csv(str(results_file), index=False)
            logger.info(f"🧠 Strategy results saved to {results_file}")
        
        # FILE 3: signals.csv (trading signals only)
        if self.config['output']['save_signals'] and self.signals_data:
            signals_df = pd.DataFrame(self.signals_data)
            signals_file = output_dir / "signals.csv"
            signals_df.to_csv(str(signals_file), index=False)
            logger.info(f"🚨 Signals saved to {signals_file}")
        
        # FILE 4: events.log (NEW - event log)
        if self.event_log:
            events_file = output_dir / "events.log"
            with open(events_file, 'w') as f:
                for event in self.event_log:
                    f.write(f"{event['timestamp']} - {event['type']}: {event['data']}\n")
            logger.info(f"📝 Events log saved to {events_file}")
        
        # FILE 5: session_info.json (BONUS - session metadata)
        self._save_session_info(output_dir)
        
        self._display_summary()
    
    def _save_session_info(self, output_dir: Path):
        """Save session metadata for easy reference"""
        import json
        
        session_info = {
            'session_start': self.session_start_time.isoformat(),
            'session_end': datetime.now().isoformat(),
            'duration_minutes': (datetime.now() - self.session_start_time).total_seconds() / 60,
            'data_source': self.config['data_source'],
            'configuration': self.config,
            'bars_processed': self.bars_processed,
            'signals_generated': self.signal_count,
            'output_directory': str(output_dir)
        }
        
        session_file = output_dir / "session_info.json"
        with open(session_file, 'w') as f:
            json.dump(session_info, f, indent=2)
        logger.info(f"📋 Session info saved to {session_file}")
    
    def _display_summary(self):
        """Display session summary with timestamped directory info"""
        source_type = self.config['data_source'].upper()
        
        print("\n" + "="*70)
        print("📊 SESSION SUMMARY")
        print("="*70)
        print(f"📈 Bars Processed: {self.bars_processed}")
        print(f"🚨 Signals Generated: {self.signal_count}")
        print(f"📊 Data Source: {source_type}")
        print(f"⏱️  Session Duration: {(datetime.now() - self.session_start_time).total_seconds() / 60:.1f} minutes")
        print(f"📁 Output Directory: {self.output_dir}/")
        
        # Show all 5 files
        files_saved = []
        for file_name in ["processed_data.csv", "strategy_results.csv", "signals.csv", "events.log", "session_info.json"]:
            if (self.output_dir / file_name).exists():
                files_saved.append(file_name)
                
        print(f"📄 Files Saved: {', '.join(files_saved)}")
        print("="*70)


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Patient Labels Trading System - Switch data sources via config.json",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default config
  python main.py
  
  # Run for 30 minutes
  python main.py --duration 30
  
  # Use custom config
  python main.py --config my_config.json

Data Source Switching:
  Edit config.json and change "data_source" to:
  - "csv" for historical CSV data
  - "polygon" for live Polygon.io data
        """
    )
    
    parser.add_argument('--config', default='config.json',
                       help='Configuration file path (default: config.json)')
    parser.add_argument('--duration', type=int,
                       help='Run duration in minutes (default: unlimited)')
    
    args = parser.parse_args()
    
    # Create and run system
    system = PatientLabelsSystem(args.config)
    await system.run(duration_minutes=args.duration)


if __name__ == "__main__":
    asyncio.run(main()) 