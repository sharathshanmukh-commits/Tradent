"""
Event System for Streaming Patient Labels
Handles callbacks and notifications for trading events
"""

import logging
from typing import Dict, Any, List, Callable, Optional
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Types of events in the system"""
    NEW_BAR = "new_bar"
    NEW_SIGNAL = "new_signal"
    TREND_CHANGE = "trend_change"
    SWING_UPDATE = "swing_update"
    CONNECTION_STATUS = "connection_status"
    STREAM_STATUS = "stream_status"
    ERROR = "error"


class EventHandler:
    """Base class for event handlers"""
    
    def handle(self, event_data: Dict[str, Any]):
        """Handle an event - to be implemented by subclasses"""
        raise NotImplementedError


class ConsoleLogger(EventHandler):
    """Logs events to console with formatting"""
    
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        
    def handle(self, event_data: Dict[str, Any]):
        """Log event to console"""
        event_type = event_data.get('type')
        timestamp = event_data.get('timestamp', datetime.now())
        
        if event_type == EventType.NEW_BAR.value:
            if self.verbose:
                bar = event_data.get('bar', {})
                print(f"[{timestamp.strftime('%H:%M:%S')}] New Bar: "
                      f"O:{bar.get('open', 0):.2f} H:{bar.get('high', 0):.2f} "
                      f"L:{bar.get('low', 0):.2f} C:{bar.get('close', 0):.2f} "
                      f"V:{bar.get('volume', 0):,}")
                      
        elif event_type == EventType.NEW_SIGNAL.value:
            signal = event_data.get('signal_data', {})
            signal_type = "BUY" if signal.get('signal', 0) > 0 else "SELL"
            print(f"\n🚨 [{timestamp.strftime('%H:%M:%S')}] NEW SIGNAL: {signal_type}")
            print(f"   Entry: ${signal.get('entry_price', 0):.2f}")
            print(f"   Stop Loss: ${signal.get('stop_loss', 0):.2f}")
            print(f"   Take Profit: ${signal.get('take_profit', 0):.2f}")
            print(f"   Risk/Reward: {abs((signal.get('take_profit', 0) - signal.get('entry_price', 0)) / (signal.get('entry_price', 0) - signal.get('stop_loss', 0))):.2f}")
            
        elif event_type == EventType.TREND_CHANGE.value:
            details = event_data.get('trend_data', {})
            print(f"\n📊 [{timestamp.strftime('%H:%M:%S')}] TREND CHANGE: "
                  f"{details.get('previous', 'None')} → {details.get('current', 'None')}")
                  
        elif event_type == EventType.SWING_UPDATE.value:
            update = event_data.get('swing_data', {})
            print(f"[{timestamp.strftime('%H:%M:%S')}] Swing Update: "
                  f"{update.get('type', 'unknown')} at ${update.get('value', 0):.2f}")
                  
        elif event_type == EventType.CONNECTION_STATUS.value:
            status = event_data.get('status', 'unknown')
            print(f"[{timestamp.strftime('%H:%M:%S')}] Connection: {status}")
            
        elif event_type == EventType.STREAM_STATUS.value:
            status = event_data.get('status', {})
            if status.get('completed'):
                print(f"\n✅ [{timestamp.strftime('%H:%M:%S')}] Stream Completed - "
                      f"Processed {status.get('total_rows', 0)} bars")
            else:
                print(f"[{timestamp.strftime('%H:%M:%S')}] Stream Status: {status}")
                
        elif event_type == EventType.ERROR.value:
            error = event_data.get('error', 'Unknown error')
            print(f"\n❌ [{timestamp.strftime('%H:%M:%S')}] ERROR: {error}")


class FileLogger(EventHandler):
    """Logs events to file"""
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        
    def handle(self, event_data: Dict[str, Any]):
        """Log event to file"""
        try:
            with open(self.filepath, 'a') as f:
                timestamp = event_data.get('timestamp', datetime.now())
                f.write(f"{timestamp.isoformat()} - {event_data}\n")
        except Exception as e:
            logger.error(f"Failed to write to log file: {e}")


class SignalCSVLogger(EventHandler):
    """Logs trading signals to CSV"""
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self._write_header()
        
    def _write_header(self):
        """Write CSV header if file doesn't exist"""
        try:
            import os
            if not os.path.exists(self.filepath):
                with open(self.filepath, 'w') as f:
                    f.write("timestamp,datetime,signal,entry_price,stop_loss,take_profit,risk_reward\n")
        except Exception as e:
            logger.error(f"Failed to write CSV header: {e}")
            
    def handle(self, event_data: Dict[str, Any]):
        """Log signal to CSV"""
        if event_data.get('type') != EventType.NEW_SIGNAL.value:
            return
            
        try:
            signal = event_data.get('signal_data', {})
            timestamp = event_data.get('timestamp', datetime.now())
            
            # Calculate risk/reward
            entry = signal.get('entry_price', 0)
            stop = signal.get('stop_loss', 0)
            target = signal.get('take_profit', 0)
            
            if entry and stop and target and stop != entry:
                risk_reward = abs((target - entry) / (entry - stop))
            else:
                risk_reward = 0
                
            with open(self.filepath, 'a') as f:
                f.write(f"{timestamp.isoformat()},"
                       f"{signal.get('datetime', '')},"
                       f"{signal.get('signal', 0)},"
                       f"{entry:.2f},"
                       f"{stop:.2f},"
                       f"{target:.2f},"
                       f"{risk_reward:.2f}\n")
                       
        except Exception as e:
            logger.error(f"Failed to write signal to CSV: {e}")


class EventSystem:
    """Central event system for managing callbacks"""
    
    def __init__(self):
        self.handlers: Dict[EventType, List[EventHandler]] = {
            event_type: [] for event_type in EventType
        }
        self.global_handlers: List[EventHandler] = []
        
    def register_handler(self, event_type: EventType, handler: EventHandler):
        """Register handler for specific event type"""
        self.handlers[event_type].append(handler)
        logger.info(f"Registered handler for {event_type.value}")
        
    def register_global_handler(self, handler: EventHandler):
        """Register handler for all events"""
        self.global_handlers.append(handler)
        logger.info("Registered global handler")
        
    def emit(self, event_type: EventType, data: Dict[str, Any]):
        """Emit an event to all registered handlers"""
        event_data = {
            'type': event_type.value,
            'timestamp': datetime.now(),
            **data
        }
        
        # Call specific handlers
        for handler in self.handlers[event_type]:
            try:
                handler.handle(event_data)
            except Exception as e:
                logger.error(f"Error in handler for {event_type.value}: {e}")
                
        # Call global handlers
        for handler in self.global_handlers:
            try:
                handler.handle(event_data)
            except Exception as e:
                logger.error(f"Error in global handler: {e}")
                
    def create_callback(self, event_type: EventType) -> Callable:
        """Create a callback function for specific event type"""
        def callback(data: Any):
            # Map data based on event type
            if event_type == EventType.NEW_BAR:
                self.emit(event_type, {'bar': data.get('bar', {})})
            elif event_type == EventType.NEW_SIGNAL:
                self.emit(event_type, {'signal_data': data})
            elif event_type == EventType.TREND_CHANGE:
                self.emit(event_type, {'trend_data': data})
            elif event_type == EventType.SWING_UPDATE:
                self.emit(event_type, {'swing_data': data})
            else:
                self.emit(event_type, {'data': data})
                
        return callback