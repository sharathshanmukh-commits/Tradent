#!/usr/bin/env python3
"""
Patient Labels Trading System with Database Integration
Real-time persistence and signal notifications
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
from database import DatabaseManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PatientLabelsSystemWithDatabase:
    """Patient Labels Trading System with Real-time Database Persistence"""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize with configuration file and database"""
        self.config = self._load_config(config_path)
        self.data_source = None
        self.buffer = None
        self.strategy = None
        self.db = None
        
        # Stats
        self.signal_count = 0
        self.bars_processed = 0
        self.session_start_time = datetime.now()
        self.session_id = self.session_start_time.strftime('%Y%m%d_%H%M%S')
        self.database_was_used = False
        
        # For backup file saving (optional)
        self.output_dir = self._create_output_directory()
        
        logger.info(f"🔧 System initialized with session ID: {self.session_id}")
        
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
            "_comment": "Patient Labels Trading System Configuration with Database",
            "_switch_data_source": "Change 'data_source' to 'csv' or 'polygon'",
            
            "data_source": "csv",
            
            "csv": {
                "file_path": "data/QQQ_5min_2025-05-01_to_2025-05-01.csv",
                "speed": 10.0,
                "symbol": "QQQ"
            },
            
            "polygon": {
                "api_key": "YOUR_POLYGON_API_KEY",
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
            
            "database": {
                "enabled": True,
                "save_backups": True
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
        """Create timestamped output directory for backup files"""
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
    
    async def setup_components(self):
        """Setup all system components including database"""
        logger.info("🔧 Setting up components...")
        
        # 1. Setup database connection
        try:
            self.db = DatabaseManager()
            await self.db.connect()
            
            # Create session in database
            await self.db.create_session(self.session_id, self.config)
            self.database_was_used = True
            logger.info("✅ Database connected and session created")
            
        except Exception as e:
            logger.error(f"❌ Database setup failed: {e}")
            if self.config.get('database', {}).get('enabled', True):
                raise
            else:
                logger.warning("⚠️ Continuing without database")
        
        # 2. Create data source
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
        
        # 3. Create buffer
        self.buffer = StreamingBuffer()
        
        # 4. Create strategy
        strategy_config = self.config['strategy']
        self.strategy = PatientLabelsStrategy(**strategy_config)
        
        logger.info("✅ All components ready")
    
    async def process_bar(self, bar):
        """Process incoming data bar with real-time database persistence"""
        self.bars_processed += 1
        
        # 1. IMMEDIATE: Save market data to database
        if self.db and self.db.is_connected():
            await self.db.save_market_data(bar, self.session_id)
            
            # Save event to database
            await self.db.save_event("NEW_BAR", {
                'bar_count': self.bars_processed,
                'datetime': bar.get('datetime', ''),
                'symbol': bar.get('symbol', ''),
                'close': bar.get('close', 0),
                'volume': bar.get('volume', 0)
            }, self.session_id)
        
        # 2. Add to buffer for strategy processing
        self.buffer.append_bar(bar)
        
        # 3. Process with strategy when we have enough data
        min_bars = 3 if self.config['data_source'] == 'polygon' else 5
        
        if len(self.buffer.df) >= min_bars:
            try:
                # Process with strategy
                current_df = self.buffer.get_dataframe()
                result_df = self.strategy.process_data(current_df)
                
                # Save strategy results to database
                if self.db and self.db.is_connected():
                    latest_result = result_df.iloc[-1]
                    await self.db.save_strategy_result(latest_result, self.session_id)
                
                # 4. CHECK FOR NEW SIGNALS
                if len(result_df) > 0:
                    latest_bar = result_df.iloc[-1]
                    if latest_bar.get('signal', 0) != 0:
                        # 🚨 SIGNAL FOUND - IMMEDIATE DATABASE SAVE + NOTIFICATION
                        await self._handle_new_signal(latest_bar, current_df)
                
            except Exception as e:
                logger.error(f"❌ Strategy error: {e}")
                
                # Log error to database
                if self.db and self.db.is_connected():
                    await self.db.save_event("ERROR", {'error': str(e)}, self.session_id)
        
        # Update session stats every 10 bars
        if self.bars_processed % 10 == 0 and self.db and self.db.is_connected():
            await self.db.update_session_stats(self.session_id, self.bars_processed, self.signal_count)
        
        # Display progress
        self._display_progress(bar)
    
    async def _handle_new_signal(self, signal_bar: pd.Series, current_df: pd.DataFrame):
        """Handle new signal with immediate database save and notifications"""
        self.signal_count += 1
        signal_type = "BUY" if signal_bar['signal'] > 0 else "SELL"
        
        # Create signal data
        signal_datetime = signal_bar.get('datetime', '')
        if hasattr(signal_datetime, 'isoformat'):
            signal_datetime = signal_datetime.isoformat()
        elif isinstance(signal_datetime, str):
            signal_datetime = pd.to_datetime(signal_datetime).isoformat()
        
        signal_data = {
            'bar_index': signal_bar.name,  # DataFrame index
            'datetime': signal_datetime,
            'type': signal_type.lower(),
            'entry_price': signal_bar.get('entry_price', 0),
            'stop_loss': signal_bar.get('stop_loss', 0),
            'target_price': signal_bar.get('target_price', 0),
            'risk_reward_ratio': self.config['strategy']['risk_reward_ratio'],
            'reason': f'Patient Labels {signal_type} signal'
        }
        
        # Create market snapshot (last 20 bars + context)
        # Convert pandas Timestamps to ISO format strings for JSON serialization
        def serialize_timestamps(obj):
            """Convert pandas Timestamps to ISO format strings"""
            if isinstance(obj, pd.Timestamp):
                return obj.isoformat()
            elif isinstance(obj, dict):
                return {k: serialize_timestamps(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [serialize_timestamps(item) for item in obj]
            else:
                return obj
        
        # Get recent bars and convert timestamps
        recent_bars_records = current_df.tail(20).to_dict('records')
        recent_bars_serialized = serialize_timestamps(recent_bars_records)
        
        market_snapshot = {
            'recent_bars': recent_bars_serialized,
            'indicators': {
                'trend_state': int(signal_bar.get('trend_state', 0)),
                'swing_cycle': int(signal_bar.get('up_swing_cycle', 0) or signal_bar.get('down_swing_cycle', 0)),
                'patient_high': float(signal_bar.get('up_patient_high', 0)) if pd.notna(signal_bar.get('up_patient_high')) else None,
                'patient_low': float(signal_bar.get('down_patient_low', 0)) if pd.notna(signal_bar.get('down_patient_low')) else None,
                'is_dual_trend': bool(signal_bar.get('is_dual_trend_active', False))
            },
            'market_context': {
                'current_price': float(signal_bar.get('close', 0)),
                'volume': int(signal_bar.get('volume', 0)),
                'day_high': float(signal_bar.get('day_high', 0)) if pd.notna(signal_bar.get('day_high')) else None,
                'day_low': float(signal_bar.get('day_low', 0)) if pd.notna(signal_bar.get('day_low')) else None
            }
        }
        
        # Save to database immediately
        if self.db and self.db.is_connected():
            try:
                signal_id = await self.db.save_trading_signal(signal_data, self.session_id, market_snapshot)
                
                # Save signal event
                await self.db.save_event("NEW_SIGNAL", {
                    'signal_count': self.signal_count,
                    'signal_id': signal_id,
                    'type': signal_type,
                    'entry_price': signal_data['entry_price'],
                    'stop_loss': signal_data['stop_loss'],
                    'target_price': signal_data['target_price']
                }, self.session_id)
                
                logger.info(f"✅ Signal {signal_id} saved to database")
                
            except Exception as e:
                logger.error(f"❌ Failed to save signal to database: {e}")
        
        # Display signal notification
        print(f"\n🚨 [{datetime.now().strftime('%H:%M:%S')}] NEW SIGNAL #{self.signal_count}: {signal_type}")
        print(f"   💰 Entry: ${signal_data['entry_price']:.2f}")
        print(f"   🛡️  Stop: ${signal_data['stop_loss']:.2f}")
        print(f"   🎯 Target: ${signal_data['target_price']:.2f}")
        print(f"   📊 Processed {len(current_df)} bars")
        
        # TODO: Add webhook notification here if needed
        # await self._send_webhook_notification(signal_data, market_snapshot)
    
    def _display_progress(self, bar):
        """Display progress information"""
        current_time = datetime.now().strftime('%H:%M:%S')
        source_type = self.config['data_source'].upper()
        db_status = "🟢 DB" if (self.db and self.db.is_connected()) else "🔴 DB"
        
        print(f"[{current_time}] 📊 {source_type} {db_status} | {bar['symbol']} | {bar['datetime']} | "
              f"${bar['close']:.2f} | Vol: {bar['volume']:,} | Buffer: {len(self.buffer.df)} | Signals: {self.signal_count}")
    
    async def run(self, duration_minutes: Optional[int] = None):
        """Run the trading system with database integration"""
        try:
            # Setup components (including database)
            await self.setup_components()
            
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
        db_status = "✅ CONNECTED" if (self.db and self.db.is_connected()) else "❌ DISCONNECTED"
        
        print("\n" + "="*70)
        print("🎯 PATIENT LABELS TRADING SYSTEM WITH DATABASE")
        print("="*70)
        print(f"📊 Data Source: {source_type}")
        print(f"🗄️  Database: {db_status}")
        print(f"🆔 Session ID: {self.session_id}")
        print(f"⚡ Strategy: Patient Labels Signal Detection")
        print(f"🎯 Symbols: {', '.join(symbols)}")
        if self.config['data_source'] == 'polygon':
            print(f"⏰ Data: Live (15-min delayed)")
        else:
            print(f"⚡ Speed: {self.config['csv'].get('speed', 1.0)}x")
        print("\nCommands:")
        print("  [Ctrl+C] - Stop system")
        print("="*70 + "\n")
    
    async def _cleanup(self):
        """Cleanup and disconnect from all services"""
        # Stop data source
        if self.data_source:
            try:
                await self.data_source.stop_streaming()
                await self.data_source.disconnect()
                logger.info("🔌 Disconnected from data source")
            except Exception as e:
                logger.warning(f"Warning during data source cleanup: {e}")
        
        # End database session
        if self.db and self.db.is_connected():
            try:
                await self.db.end_session(self.session_id, str(self.output_dir))
                await self.db.disconnect()
                logger.info("🗄️ Database session ended and disconnected")
            except Exception as e:
                logger.warning(f"Warning during database cleanup: {e}")
        
        # Optional: Save backup files if configured
        if self.config.get('database', {}).get('save_backups', True):
            await self._save_backup_files()
        
        self._display_summary()
    
    async def _save_backup_files(self):
        """Save backup CSV files (optional)"""
        try:
            if self.buffer and len(self.buffer.df) > 0:
                # Save processed data backup
                data_file = self.output_dir / "processed_data_backup.csv"
                self.buffer.to_csv(str(data_file))
                logger.info(f"💾 Backup data saved to {data_file}")
                
        except Exception as e:
            logger.warning(f"Failed to save backup files: {e}")
    
    def _display_summary(self):
        """Display session summary"""
        source_type = self.config['data_source'].upper()
        
        print("\n" + "="*70)
        print("📊 SESSION SUMMARY")
        print("="*70)
        print(f"🆔 Session ID: {self.session_id}")
        print(f"📈 Bars Processed: {self.bars_processed}")
        print(f"🚨 Signals Generated: {self.signal_count}")
        print(f"📊 Data Source: {source_type}")
        print(f"🗄️  Database: {'✅ Used' if self.database_was_used else '❌ Not used'}")
        print(f"⏱️  Session Duration: {(datetime.now() - self.session_start_time).total_seconds() / 60:.1f} minutes")
        print(f"📁 Backup Directory: {self.output_dir}/")
        print("="*70)


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Patient Labels Trading System with Database Integration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with database integration
  python main_with_database.py
  
  # Run for 30 minutes
  python main_with_database.py --duration 30
  
  # Use custom config
  python main_with_database.py --config my_config.json

Features:
  - Real-time database persistence
  - Immediate signal notifications
  - Live monitoring and queries
  - Automatic backup files
        """
    )
    
    parser.add_argument('--config', default='config.json',
                       help='Configuration file path (default: config.json)')
    parser.add_argument('--duration', type=int,
                       help='Run duration in minutes (default: unlimited)')
    
    args = parser.parse_args()
    
    # Create and run system
    system = PatientLabelsSystemWithDatabase(args.config)
    await system.run(duration_minutes=args.duration)


if __name__ == "__main__":
    asyncio.run(main())