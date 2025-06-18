"""
Polygon.io Data Source implementation for real-time market data streaming.

This module provides real-time market data streaming using Polygon.io WebSocket API.
Requires a paid Polygon.io subscription for real-time data access.
"""

import asyncio
import json
import logging
import websockets
from typing import Dict, Any, List, Callable, Optional
from datetime import datetime, timezone
import aiohttp
from urllib.parse import urlencode
import pytz

from .base import BaseDataSource

logger = logging.getLogger(__name__)


class PolygonDataSource(BaseDataSource):
    """
    Polygon.io WebSocket data source for real-time market data.
    
    Supports real-time streaming of 5-minute aggregates and other timeframes.
    Requires a paid Polygon.io subscription ($29+ plan) for real-time access.
    """
    
    # Polygon.io WebSocket endpoints
    WEBSOCKET_URL_REALTIME = "wss://socket.polygon.io/stocks"
    WEBSOCKET_URL_DELAYED = "wss://delayed.polygon.io/stocks"  # 15-minute delayed
    REST_BASE_URL = "https://api.polygon.io"
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Polygon data source.
        
        Expected config structure:
        {
            "api_key": "your_polygon_api_key",
            "symbols": ["QQQ", "SPY"],
            "subscription_types": ["A"],  # A=aggregates, T=trades, Q=quotes
            "reconnect_attempts": 3,
            "reconnect_delay_ms": 5000,
            "timeframe": "5minute",  # 1minute, 5minute, 15minute, etc.
            "data_type": "auto"  # "realtime", "delayed", or "auto" (detect automatically)
        }
        """
        super().__init__(config)
        
        # Extract configuration
        self.api_key = config.get('api_key', '')
        self.symbols = config.get('symbols', [])
        self.subscription_types = config.get('subscription_types', ['A'])
        self.reconnect_attempts = config.get('reconnect_attempts', 3)
        self.reconnect_delay_ms = config.get('reconnect_delay_ms', 5000)
        self.timeframe = config.get('timeframe', '5minute')
        self.data_type = config.get('data_type', 'auto')  # auto-detect by default
        
        # WebSocket connection
        self.websocket = None
        self.connected_to_ws = False
        self.streaming_task = None
        self.reconnect_count = 0
        self.websocket_url = None  # Will be determined based on subscription
        self.is_realtime = False  # Track if we're using real-time or delayed data
        
        # Validation
        self._validate_config()
        
        self.logger.info(f"Polygon Data Source initialized for streaming")
        self.logger.info(f"Symbols: {self.symbols}, Timeframe: {self.timeframe}, Data Type: {self.data_type}")
        
    def _validate_config(self):
        """Validate the configuration parameters."""
        if not self.api_key:
            raise ValueError("Polygon API key is required")
        
        if not self.symbols:
            raise ValueError("At least one symbol must be specified")
        
        valid_subscription_types = ['A', 'T', 'Q']
        for sub_type in self.subscription_types:
            if sub_type not in valid_subscription_types:
                raise ValueError(f"Invalid subscription type: {sub_type}. Valid types: {valid_subscription_types}")
        
        valid_data_types = ['realtime', 'delayed', 'auto']
        if self.data_type not in valid_data_types:
            raise ValueError(f"Invalid data_type: {self.data_type}. Valid types: {valid_data_types}")
    
    async def connect(self) -> None:
        """Establish connection to Polygon.io WebSocket."""
        try:
            self.logger.info("Connecting to Polygon.io WebSocket...")
            
            # Test API key and determine subscription level
            subscription_info = await self._test_api_key_and_detect_subscription()
            
            # Determine WebSocket URL based on subscription or user preference
            self.websocket_url = self._determine_websocket_url(subscription_info)
            
            self.logger.info(f"Using {'real-time' if self.is_realtime else 'delayed (15-min)'} data feed")
            self.logger.info(f"WebSocket URL: {self.websocket_url}")
            
            # Connect to WebSocket
            self.websocket = await websockets.connect(
                self.websocket_url,
                ping_interval=30,
                ping_timeout=10
            )
            
            self.connected_to_ws = True
            self.logger.info("Connected to Polygon.io WebSocket")
            
            # Authenticate
            await self._authenticate()
            
            # Subscribe to data
            await self._subscribe_to_symbols()
            
            self.is_connected = True
            self.logger.info("Polygon.io connection established successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Polygon.io: {e}")
            await self._cleanup_connection()
            raise ConnectionError(f"Polygon.io connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Disconnect from Polygon.io and cleanup resources."""
        self.logger.info("Disconnecting from Polygon.io...")
        
        await self.stop_streaming()
        await self._cleanup_connection()
        
        self.is_connected = False
        self.logger.info("Disconnected from Polygon.io")
    
    async def subscribe(self, symbols: List[str]) -> None:
        """Subscribe to data for specified symbols."""
        if not symbols:
            raise ValueError("Symbols list cannot be empty")
        
        self._validate_connection()
        
        # Update symbols list
        self.symbols = symbols
        self._subscribed_symbols = symbols.copy()
        
        # If already connected, subscribe to new symbols
        if self.connected_to_ws:
            await self._subscribe_to_symbols()
        
        self.logger.info(f"Subscribed to symbols: {symbols}")
    
    async def start_streaming(self, callback: Callable) -> None:
        """Start streaming real-time data."""
        self._validate_connection()
        self._validate_subscriptions()
        
        if self.is_streaming:
            await self.stop_streaming()
        
        self.data_callback = callback
        self.is_streaming = True
        
        # Start streaming task
        self.streaming_task = asyncio.create_task(self._streaming_loop())
        
        self.logger.info("Started Polygon.io real-time streaming")
    
    async def stop_streaming(self) -> None:
        """Stop streaming data."""
        if not self.is_streaming:
            return
        
        self.is_streaming = False
        
        if self.streaming_task and not self.streaming_task.done():
            self.streaming_task.cancel()
            try:
                await self.streaming_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Stopped Polygon.io streaming")
    
    async def _test_api_key_and_detect_subscription(self) -> Dict[str, Any]:
        """Test API key and detect subscription level."""
        # Test with a simple REST call to check API key validity and subscription level
        test_url = f"{self.REST_BASE_URL}/v2/aggs/ticker/QQQ/range/1/day/2023-01-01/2023-01-02"
        params = {"apikey": self.api_key}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{test_url}?{urlencode(params)}") as response:
                if response.status == 403:
                    raise ValueError("Invalid Polygon.io API key")
                elif response.status == 429:
                    raise ValueError("API rate limit exceeded")
                elif response.status != 200:
                    raise ValueError(f"API test failed with status {response.status}")
                
                # Try to determine subscription level by checking response headers or making additional calls
                subscription_info = {
                    'api_key_valid': True,
                    'has_realtime': False  # Default to assuming delayed data
                }
                
                # Check if we can access real-time data by trying a recent date
                # Real-time subscribers can access very recent data, delayed users cannot
                try:
                    recent_test_url = f"{self.REST_BASE_URL}/v1/last/stocks/QQQ"
                    async with session.get(f"{recent_test_url}?{urlencode(params)}") as recent_response:
                        if recent_response.status == 200:
                            subscription_info['has_realtime'] = True
                            self.logger.info("✓ Detected real-time subscription")
                        else:
                            self.logger.info("✓ Detected delayed data subscription (15-min delay)")
                except:
                    # If the test fails, assume delayed data
                    self.logger.info("✓ Assuming delayed data subscription")
                
                return subscription_info
        
        self.logger.info("✓ Polygon.io API key validated")
    
    def _determine_websocket_url(self, subscription_info: Dict[str, Any]) -> str:
        """Determine which WebSocket URL to use based on subscription and preferences."""
        if self.data_type == 'realtime':
            # User explicitly wants real-time
            if not subscription_info.get('has_realtime', False):
                self.logger.warning("Real-time data requested but subscription may not support it. Attempting anyway...")
            self.is_realtime = True
            return self.WEBSOCKET_URL_REALTIME
        elif self.data_type == 'delayed':
            # User explicitly wants delayed
            self.is_realtime = False
            return self.WEBSOCKET_URL_DELAYED
        else:
            # Auto-detect based on subscription
            if subscription_info.get('has_realtime', False):
                self.logger.info("Auto-detected: Using real-time data feed")
                self.is_realtime = True
                return self.WEBSOCKET_URL_REALTIME
            else:
                self.logger.info("Auto-detected: Using delayed data feed (15-minute delay)")
                self.is_realtime = False
                return self.WEBSOCKET_URL_DELAYED
    
    async def _authenticate(self) -> None:
        """Authenticate with Polygon.io WebSocket."""
        auth_message = {
            "action": "auth",
            "params": self.api_key
        }
        
        await self.websocket.send(json.dumps(auth_message))
        
        # Wait for auth response
        response = await asyncio.wait_for(self.websocket.recv(), timeout=10)
        auth_response = json.loads(response)
        
        # Check for successful authentication
        # Polygon.io sends a status message first, then auth response
        for msg in auth_response:
            if msg.get("ev") == "status":
                if msg.get("status") == "connected":
                    self.logger.info(f"✓ Connected: {msg.get('message', '')}")
                elif msg.get("status") == "auth_success":
                    self.logger.info("✓ Authenticated with Polygon.io")
                    return
                elif msg.get("status") == "auth_failed":
                    raise ConnectionError(f"Authentication failed: {msg.get('message', 'Invalid API key')}")
        
        # If we didn't get auth_success, wait for another message
        try:
            response = await asyncio.wait_for(self.websocket.recv(), timeout=5)
            auth_response = json.loads(response)
            
            for msg in auth_response:
                if msg.get("status") == "auth_success":
                    self.logger.info("✓ Authenticated with Polygon.io")
                    return
                elif msg.get("status") == "auth_failed":
                    raise ConnectionError(f"Authentication failed: {msg.get('message', 'Invalid API key')}")
        except asyncio.TimeoutError:
            # Sometimes auth is implicit after connection
            self.logger.info("✓ Authentication appears successful (no explicit auth_success message)")
            return
        
        # If we get here, assume success (some Polygon plans don't send explicit auth messages)
        self.logger.info("✓ Assuming authentication successful")
    
    async def _subscribe_to_symbols(self) -> None:
        """Subscribe to data streams for configured symbols."""
        subscription_strings = []
        
        for symbol in self.symbols:
            for sub_type in self.subscription_types:
                if sub_type == 'A':  # Aggregates (5-minute bars)
                    subscription_strings.append(f"AM.{symbol}")  # Minute aggregates
                elif sub_type == 'T':  # Trades
                    subscription_strings.append(f"T.{symbol}")
                elif sub_type == 'Q':  # Quotes
                    subscription_strings.append(f"Q.{symbol}")
        
        subscribe_message = {
            "action": "subscribe",
            "params": ",".join(subscription_strings)
        }
        
        await self.websocket.send(json.dumps(subscribe_message))
        self.logger.info(f"Subscribed to: {subscription_strings}")
    
    async def _streaming_loop(self) -> None:
        """Main streaming loop to receive and process data."""
        try:
            self.logger.info("Starting Polygon.io streaming loop...")
            
            # Buffer for minute aggregates to build 5-minute bars
            minute_buffer = {}
            
            while self.is_streaming and self.connected_to_ws:
                try:
                    # Receive message with timeout
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=30)
                    data = json.loads(message)
                    
                    # Process each message in the array
                    for msg in data:
                        if msg.get("ev") == "AM":  # Minute aggregate
                            await self._process_minute_aggregate(msg, minute_buffer)
                        elif msg.get("ev") == "status":
                            self.logger.info(f"Status: {msg}")
                        
                except asyncio.TimeoutError:
                    # Send ping to keep connection alive
                    if self.connected_to_ws:
                        await self.websocket.ping()
                        
                except websockets.exceptions.ConnectionClosed:
                    self.logger.warning("WebSocket connection closed")
                    if self.is_streaming:
                        await self._attempt_reconnect()
                    break
                    
        except asyncio.CancelledError:
            self.logger.info("Streaming loop cancelled")
        except Exception as e:
            self.logger.error(f"Streaming loop error: {e}")
            if self.is_streaming:
                await self._attempt_reconnect()
    
    async def _process_minute_aggregate(self, msg: Dict[str, Any], minute_buffer: Dict[str, List]) -> None:
        """Process minute aggregate and build timeframe-appropriate bars."""
        try:
            symbol = msg.get("sym", "")
            timestamp = msg.get("s", 0)  # Start timestamp in milliseconds
            
            # Convert UTC timestamp to ET timezone
            utc_dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            et_dt = utc_dt.astimezone(pytz.timezone('America/New_York'))
            
            # If timeframe is 1minute, send data directly
            if self.timeframe == "1minute":
                # Create 1-minute bar directly
                standard_bar = self._create_standard_bar(
                    symbol=symbol,
                    datetime_str=et_dt.isoformat(),
                    open_price=msg.get("o", 0),
                    high_price=msg.get("h", 0),
                    low_price=msg.get("l", 0),
                    close_price=msg.get("c", 0),
                    volume=msg.get("v", 0),
                    source='polygon'
                )
                
                # Send to callback immediately
                if self.data_callback:
                    await self._safe_callback(standard_bar)
                return
            
            # For other timeframes (5minute, etc.), use buffering logic
            # Calculate timeframe boundary using ET time
            if self.timeframe == "5minute":
                timeframe_boundary = et_dt.replace(minute=(et_dt.minute // 5) * 5, second=0, microsecond=0)
                required_minutes = 5
            else:
                # Default to 5-minute for unknown timeframes
                timeframe_boundary = et_dt.replace(minute=(et_dt.minute // 5) * 5, second=0, microsecond=0)
                required_minutes = 5
            
            # Buffer key
            buffer_key = f"{symbol}_{timeframe_boundary.isoformat()}"
            
            # Initialize buffer for this timeframe period
            if buffer_key not in minute_buffer:
                minute_buffer[buffer_key] = []
            
            # Add minute data to buffer
            minute_data = {
                'timestamp': timestamp,
                'open': msg.get("o", 0),
                'high': msg.get("h", 0), 
                'low': msg.get("l", 0),
                'close': msg.get("c", 0),
                'volume': msg.get("v", 0)
            }
            
            minute_buffer[buffer_key].append(minute_data)
            
            # Check if we have enough minutes of data
            if len(minute_buffer[buffer_key]) >= required_minutes:
                await self._create_timeframe_bar(symbol, timeframe_boundary, minute_buffer[buffer_key])
                # Clear this buffer
                del minute_buffer[buffer_key]
            
        except Exception as e:
            self.logger.error(f"Error processing minute aggregate: {e}")
    
    async def _create_timeframe_bar(self, symbol: str, dt: datetime, minute_data: List[Dict]) -> None:
        """Create timeframe bar from minute data."""
        if not minute_data:
            return
        
        # Calculate OHLCV for timeframe bar
        open_price = minute_data[0]['open']
        close_price = minute_data[-1]['close']
        high_price = max(bar['high'] for bar in minute_data)
        low_price = min(bar['low'] for bar in minute_data)
        total_volume = sum(bar['volume'] for bar in minute_data)
        
        # Create standardized bar
        standard_bar = self._create_standard_bar(
            symbol=symbol,
            datetime_str=dt.isoformat(),
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=total_volume,
            source='polygon'
        )
        
        # Send to callback
        if self.data_callback:
            await self._safe_callback(standard_bar)
    
    async def _safe_callback(self, bar: Dict[str, Any]) -> None:
        """Safely call the data callback."""
        try:
            if asyncio.iscoroutinefunction(self.data_callback):
                await self.data_callback(bar)
            else:
                self.data_callback(bar)
        except Exception as e:
            self.logger.error(f"Error in data callback: {e}")
    
    async def _attempt_reconnect(self) -> None:
        """Attempt to reconnect to Polygon.io."""
        if self.reconnect_count >= self.reconnect_attempts:
            self.logger.error("Max reconnection attempts reached")
            self.is_streaming = False
            return
        
        self.reconnect_count += 1
        delay = self.reconnect_delay_ms / 1000
        
        self.logger.info(f"Attempting reconnection {self.reconnect_count}/{self.reconnect_attempts} in {delay}s...")
        await asyncio.sleep(delay)
        
        try:
            await self._cleanup_connection()
            await self.connect()
            self.reconnect_count = 0  # Reset on successful reconnection
        except Exception as e:
            self.logger.error(f"Reconnection attempt {self.reconnect_count} failed: {e}")
    
    async def _cleanup_connection(self) -> None:
        """Clean up WebSocket connection."""
        self.connected_to_ws = False
        
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception:
                pass
            self.websocket = None
    
    def get_progress(self) -> Dict[str, Any]:
        """Get streaming progress information."""
        return {
            'connected': self.is_connected,
            'streaming': self.is_streaming,
            'symbols': self._subscribed_symbols,
            'timeframe': self.timeframe,
            'reconnect_count': self.reconnect_count
        }
    
    def __str__(self) -> str:
        """String representation."""
        return f"PolygonDataSource(connected={self.is_connected}, streaming={self.is_streaming}, symbols={len(self._subscribed_symbols)})"
    
    def __repr__(self) -> str:
        """Detailed representation."""
        return self.__str__()