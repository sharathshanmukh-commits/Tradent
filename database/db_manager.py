import asyncpg
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, date
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

def safe_json_dumps(obj: Any) -> str:
    """JSON encoder that handles date objects and pandas types"""
    def json_serializer(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif hasattr(obj, 'isoformat'):  # pandas Timestamp
            return obj.isoformat()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif hasattr(obj, 'to_pydatetime'):
            return obj.to_pydatetime().isoformat()
        elif hasattr(obj, 'item'):  # numpy types
            return obj.item()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    return json.dumps(obj, default=json_serializer)

class DatabaseManager:
    """Database manager for live trading system with real-time persistence"""
    
    def __init__(self, config_path: str = "database/db_config.json"):
        """Initialize database manager with configuration"""
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Database config file not found: {config_path}")
            
        with open(config_file, 'r') as f:
            self.config = json.load(f)['database']
        
        self.pool = None
        self._connected = False
        
        logger.info("DatabaseManager initialized")
    
    async def connect(self) -> None:
        """Create async connection pool to PostgreSQL"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                min_size=self.config.get('pool_min_size', 5),
                max_size=self.config.get('pool_max_size', 20),
                command_timeout=self.config.get('command_timeout', 60)
            )
            self._connected = True
            logger.info("✅ Database connection pool created successfully")
            
            # Test connection
            async with self.pool.acquire() as conn:
                version = await conn.fetchval('SELECT version()')
                logger.info(f"Connected to: {version}")
                
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            self._connected = False
            logger.info("🔌 Database connection pool closed")
    
    def is_connected(self) -> bool:
        """Check if database is connected"""
        return self._connected and self.pool is not None
    
    async def create_session(self, session_id: str, config: Dict[str, Any]) -> None:
        """Create new trading session record"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO trading_sessions 
                    (session_id, session_start, data_source, configuration, status)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (session_id) DO UPDATE SET
                    session_start = EXCLUDED.session_start,
                    configuration = EXCLUDED.configuration,
                    status = EXCLUDED.status
                """, 
                session_id, 
                datetime.now(), 
                config.get('data_source', 'unknown'), 
                safe_json_dumps(config),
                'RUNNING')
                
            logger.info(f"📊 Session {session_id} created in database")
            
        except Exception as e:
            logger.error(f"❌ Failed to create session: {e}")
            raise
    
    async def update_session_stats(self, session_id: str, bars_processed: int, signals_generated: int) -> None:
        """Update session statistics"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE trading_sessions 
                    SET bars_processed = $2, signals_generated = $3, updated_at = NOW()
                    WHERE session_id = $1
                """, session_id, bars_processed, signals_generated)
                
        except Exception as e:
            logger.error(f"❌ Failed to update session stats: {e}")
    
    async def end_session(self, session_id: str, output_directory: str) -> None:
        """Mark session as completed"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE trading_sessions 
                    SET session_end = NOW(), 
                        duration_minutes = EXTRACT(EPOCH FROM (NOW() - session_start)) / 60,
                        output_directory = $2,
                        status = 'COMPLETED',
                        updated_at = NOW()
                    WHERE session_id = $1
                """, session_id, output_directory)
                
            logger.info(f"✅ Session {session_id} marked as completed")
            
        except Exception as e:
            logger.error(f"❌ Failed to end session: {e}")
    
    async def save_market_data(self, bar: Dict[str, Any], session_id: str) -> None:
        """Save market data bar to database"""
        try:
            # Parse datetime if it's a string
            timestamp = bar['datetime']
            if isinstance(timestamp, str):
                timestamp = pd.to_datetime(timestamp)
            
            # Parse timestamp_received if it's a string
            timestamp_received = bar['timestamp_received']
            if isinstance(timestamp_received, str):
                timestamp_received = pd.to_datetime(timestamp_received)
            
            # Ensure timezone awareness
            if timestamp_received.tz is None:
                timestamp_received = timestamp_received.tz_localize('UTC')
            
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO market_data 
                    (timestamp, symbol, open, high, low, close, volume, source, timestamp_received, session_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """, 
                timestamp,
                bar['symbol'],
                float(bar['open']),
                float(bar['high']),
                float(bar['low']),
                float(bar['close']),
                int(bar['volume']),
                bar['source'],
                timestamp_received,
                session_id)
                
        except Exception as e:
            logger.error(f"❌ Failed to save market data: {e}")
            # Don't raise - we don't want to stop trading for DB issues
    
    async def save_strategy_result(self, result_row: pd.Series, session_id: str) -> None:
        """Save strategy results row to database"""
        try:
            # Handle NaN values
            def safe_float(value):
                return float(value) if pd.notna(value) else None
            
            def safe_int(value):
                return int(value) if pd.notna(value) else None
            
            def safe_bool(value):
                return bool(value) if pd.notna(value) else False
            
            # Parse datetime - handle various datetime types and column naming from merge
            timestamp = None
            
            # Try different datetime column names (due to DataFrame merges)
            for dt_col in ['datetime', 'datetime_x', 'datetime_y']:
                if dt_col in result_row:
                    timestamp = result_row[dt_col]
                    break
            
            if timestamp is None:
                # If no datetime column, try to get from index if it's a DatetimeIndex
                if hasattr(result_row, 'name') and result_row.name is not None:
                    timestamp = result_row.name
                else:
                    raise ValueError("No datetime found in result_row")
            
            if isinstance(timestamp, str):
                timestamp = pd.to_datetime(timestamp)
            elif hasattr(timestamp, 'to_pydatetime'):
                # Handle pandas Timestamp
                timestamp = timestamp.to_pydatetime()
            elif isinstance(timestamp, pd.Timestamp):
                timestamp = timestamp.to_pydatetime()
            
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO strategy_results 
                    (timestamp, symbol, open, high, low, close, volume,
                     trend_state, up_patient_high, down_patient_low, day_high, day_low,
                     is_dual_trend_active, in_uptrend, up_swing_cycle, in_downtrend, 
                     down_swing_cycle, dominant_trend, signal, entry_price, stop_loss, 
                     target_price, session_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 
                            $15, $16, $17, $18, $19, $20, $21, $22, $23)
                """, 
                timestamp,
                result_row.get('symbol', result_row.get('symbol_x', result_row.get('symbol_y', 'QQQ'))),
                safe_float(result_row.get('open', result_row.get('open_x', result_row.get('open_y')))),
                safe_float(result_row.get('high', result_row.get('high_x', result_row.get('high_y')))),
                safe_float(result_row.get('low', result_row.get('low_x', result_row.get('low_y')))),
                safe_float(result_row.get('close', result_row.get('close_x', result_row.get('close_y')))),
                safe_int(result_row.get('volume', result_row.get('volume_x', result_row.get('volume_y')))),
                safe_int(result_row.get('trend_state', 0)),
                safe_float(result_row.get('up_patient_high')),
                safe_float(result_row.get('down_patient_low')),
                safe_float(result_row.get('day_high')),
                safe_float(result_row.get('day_low')),
                safe_bool(result_row.get('is_dual_trend_active', False)),
                safe_bool(result_row.get('in_uptrend', False)),
                safe_int(result_row.get('up_swing_cycle', 0)),
                safe_bool(result_row.get('in_downtrend', False)),
                safe_int(result_row.get('down_swing_cycle', 0)),
                safe_int(result_row.get('dominant_trend', 0)),
                safe_int(result_row.get('signal', 0)),
                safe_float(result_row.get('entry_price')),
                safe_float(result_row.get('stop_loss')),
                safe_float(result_row.get('target_price')),
                session_id)
                
        except Exception as e:
            logger.error(f"❌ Failed to save strategy result: {e}")
            # Don't raise - we don't want to stop trading for DB issues
    
    async def save_trading_signal(self, signal: Dict[str, Any], session_id: str, market_snapshot: Dict[str, Any] = None) -> str:
        """Save trading signal and return signal_id"""
        try:
            # Generate unique signal ID
            signal_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{signal.get('bar_index', 0)}"
            
            # Parse datetime - handle NaT and other edge cases
            signal_datetime = signal['datetime']
            
            # More comprehensive NaT detection
            try:
                # Check for various NaT representations
                if (pd.isna(signal_datetime) or 
                    str(signal_datetime) == 'NaT' or 
                    (hasattr(signal_datetime, '_value') and pd.isna(signal_datetime))):
                    logger.warning(f"Signal datetime is NaT ({type(signal_datetime)}, value: {signal_datetime}), using current time instead")
                    signal_datetime = datetime.now()
                elif isinstance(signal_datetime, str):
                    signal_datetime = pd.to_datetime(signal_datetime)
                elif hasattr(signal_datetime, 'to_pydatetime'):
                    # Handle pandas Timestamp - check if it's NaT first
                    if pd.isna(signal_datetime):
                        logger.warning(f"Signal datetime is NaT, using current time instead")
                        signal_datetime = datetime.now()
                    else:
                        signal_datetime = signal_datetime.to_pydatetime()
                elif isinstance(signal_datetime, pd.Timestamp):
                    if pd.isna(signal_datetime):
                        logger.warning(f"Signal datetime is NaT, using current time instead")
                        signal_datetime = datetime.now()
                    else:
                        signal_datetime = signal_datetime.to_pydatetime()
                elif hasattr(signal_datetime, 'date'):
                    # Handle date objects by converting to datetime
                    signal_datetime = datetime.combine(signal_datetime, datetime.min.time())
            except Exception as dt_error:
                logger.warning(f"Error parsing signal datetime {signal_datetime} ({type(signal_datetime)}): {dt_error}. Using current time.")
                signal_datetime = datetime.now()
            
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO trading_signals 
                    (signal_id, timestamp, datetime_signal, bar_index, signal_type,
                     entry_price, stop_loss, target_price, risk, risk_reward_ratio, 
                     reason, market_snapshot, session_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                """,
                signal_id,
                datetime.now(),
                signal_datetime,
                signal.get('bar_index', 0),
                signal['type'],
                float(signal['entry_price']),
                float(signal['stop_loss']),
                float(signal['target_price']),
                float(signal.get('risk', 0)) if signal.get('risk') else None,
                float(signal.get('risk_reward_ratio', 1.0)),
                signal.get('reason', ''),
                safe_json_dumps(market_snapshot) if market_snapshot else None,
                session_id)
            
            logger.info(f"🚨 Signal {signal_id} saved to database: {signal['type']} at ${signal['entry_price']:.2f}")
            return signal_id
            
        except Exception as e:
            logger.error(f"❌ Failed to save trading signal: {e}")
            raise
    
    async def save_event(self, event_type: str, event_data: Dict[str, Any], session_id: str) -> None:
        """Save system event to database"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO system_events (timestamp, event_type, event_data, session_id)
                    VALUES ($1, $2, $3, $4)
                """, 
                datetime.now(), 
                event_type, 
                safe_json_dumps(event_data), 
                session_id)
                
        except Exception as e:
            logger.error(f"❌ Failed to save event: {e}")
            # Don't raise - events are not critical
    
    async def get_recent_signals(self, session_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent signals for a session"""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT signal_id, timestamp, signal_type, entry_price, stop_loss, target_price, reason
                    FROM trading_signals 
                    WHERE session_id = $1 
                    ORDER BY timestamp DESC 
                    LIMIT $2
                """, session_id, limit)
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"❌ Failed to get recent signals: {e}")
            return []
    
    async def get_session_stats(self, session_id: str) -> Dict[str, Any]:
        """Get current session statistics"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT bars_processed, signals_generated, status, session_start
                    FROM trading_sessions 
                    WHERE session_id = $1
                """, session_id)
                
                if row:
                    return dict(row)
                else:
                    return {}
                    
        except Exception as e:
            logger.error(f"❌ Failed to get session stats: {e}")
            return {}