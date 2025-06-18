"""
CSV Data Source implementation for the trading system.

This module provides CSV file reading capabilities with configurable streaming speed
and robust data format handling. It converts CSV data to the standardized bar format
used throughout the trading system.
"""

import asyncio
import pandas as pd
import pytz
from datetime import datetime, timezone
from typing import Dict, Any, List, Callable, Optional
import os
from pathlib import Path
import logging
from utils.data_loader import DataLoader
from .base import BaseDataSource

logger = logging.getLogger(__name__)


class CSVDataSource(BaseDataSource):
    """
    CSV file data source implementation.
    
    Reads CSV files containing OHLCV data and streams them at configurable speeds.
    Handles various CSV column name formats and data types automatically.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize CSV data source.
        
        Expected config format:
        {
            "file_path": "data/QQQ_5min_2025-05-01_to_2025-05-01.csv",
            "speed": 10.0,  # Speed multiplier (higher = faster streaming)
            "delay_ms": 100,  # Base delay between bars in milliseconds
            "symbol": "QQQ"  # Symbol override (uses filename if not provided)
        }
        """
        super().__init__(config)
        
        # CSV specific configuration
        self.file_path = config.get('file_path')
        self.speed = config.get('speed', 1.0)
        self.delay_ms = config.get('delay_ms', 1000)  # Default 1 second
        self.symbol_override = config.get('symbol')
        self.timezone = config.get('timezone', 'America/New_York')  # Default to NYSE timezone
        
        # Data storage
        self.csv_data: Optional[pd.DataFrame] = None
        self.current_row_index = 0
        self._streaming_task: Optional[asyncio.Task] = None
        
        # Calculate actual delay based on speed
        self.actual_delay_seconds = (self.delay_ms / 1000.0) / self.speed
        
        self.logger.info(f"CSV Data Source initialized: file={self.file_path}, "
                        f"speed={self.speed}x, delay={self.actual_delay_seconds:.3f}s")
    
    async def connect(self) -> None:
        """
        Load and validate the CSV file.
        
        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV file format is invalid
            ConnectionError: If data loading fails
        """
        try:
            if not self.file_path:
                raise ValueError("file_path is required in CSV data source config")
            
            file_path = Path(self.file_path)
            if not file_path.exists():
                raise FileNotFoundError(f"CSV file not found: {self.file_path}")
            
            self.logger.info(f"Loading CSV file: {self.file_path}")
            
            # Load CSV data
            await self._load_csv_data()
            
            # Validate data
            if self.csv_data is None or len(self.csv_data) == 0:
                raise ValueError(f"CSV file is empty or invalid: {self.file_path}")
            
            self.is_connected = True
            self.logger.info(f"Connected to CSV: {len(self.csv_data)} rows loaded")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to CSV file: {e}")
            raise ConnectionError(f"CSV connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Disconnect from CSV source and cleanup."""
        try:
            # Stop streaming if active
            if self.is_streaming:
                await self.stop_streaming()
            
            # Clear data
            self.csv_data = None
            self.current_row_index = 0
            self.is_connected = False
            
            self.logger.info("Disconnected from CSV data source")
            
        except Exception as e:
            self.logger.error(f"Error during CSV disconnect: {e}")
            raise
    
    async def subscribe(self, symbols: List[str]) -> None:
        """
        Subscribe to symbols for CSV data source.
        
        Note: CSV files typically contain data for a single symbol,
        so this method validates the requested symbols against available data.
        """
        self._validate_connection()
        
        if not symbols:
            raise ValueError("Symbols list cannot be empty")
        
        # For CSV, we typically have one symbol
        # Use symbol override if provided, otherwise try to detect from filename or data
        available_symbol = self._get_symbol_from_data()
        
        # Validate requested symbols
        for symbol in symbols:
            if symbol != available_symbol:
                self.logger.warning(f"Requested symbol '{symbol}' not available in CSV. "
                                  f"Available: '{available_symbol}'")
        
        # Store the requested symbols (even if they don't match perfectly)
        self._subscribed_symbols = symbols.copy()
        
        self.logger.info(f"Subscribed to symbols: {symbols} (CSV contains: {available_symbol})")
    
    async def start_streaming(self, callback: Callable) -> None:
        """
        Start streaming CSV data to the callback function.
        
        Args:
            callback: Function to receive standardized bar data
        """
        self._validate_connection()
        self._validate_subscriptions()
        
        if self.is_streaming:
            self.logger.warning("Streaming already active")
            return
        
        self.set_data_callback(callback)
        self.is_streaming = True
        self.current_row_index = 0
        
        self.logger.info(f"Starting CSV stream: speed={self.speed}x, "
                        f"delay={self.actual_delay_seconds:.3f}s")
        
        # Start the streaming task
        self._streaming_task = asyncio.create_task(self._stream_loop())
        
        try:
            await self._streaming_task
        except asyncio.CancelledError:
            self.logger.info("CSV streaming was cancelled")
        except Exception as e:
            self.logger.error(f"Error in CSV streaming: {e}")
            raise
        finally:
            self.is_streaming = False
    
    async def stop_streaming(self) -> None:
        """Stop the CSV streaming loop."""
        if not self.is_streaming:
            return
        
        self.logger.info("Stopping CSV streaming")
        
        # Cancel the streaming task
        if self._streaming_task and not self._streaming_task.done():
            self._streaming_task.cancel()
            try:
                await self._streaming_task
            except asyncio.CancelledError:
                pass
        
        self.is_streaming = False
        self.logger.info("CSV streaming stopped")
    
    async def _load_csv_data(self) -> None:
        """Load CSV data from file with robust column detection."""
        try:
            # Try reading with different encodings and separators
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                for sep in [',', ';', '\t']:
                    try:
                        df = pd.read_csv(self.file_path, encoding=encoding, sep=sep)
                        if len(df.columns) >= 5:  # Minimum OHLCV columns
                            self.csv_data = df
                            self.logger.info(f"CSV loaded successfully with encoding={encoding}, sep='{sep}'")
                            break
                    except Exception:
                        continue
                if self.csv_data is not None:
                    break
            
            if self.csv_data is None:
                raise ValueError("Could not parse CSV file with any supported format")
            
            # Clean column names (remove extra whitespace)
            self.csv_data.columns = self.csv_data.columns.str.strip()
            
            # Detect and standardize column names
            self._standardize_column_names()
            
            # Validate required columns
            required_cols = ['datetime', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in self.csv_data.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Convert data types
            await self._convert_data_types()
            
            # Sort by datetime
            self.csv_data = self.csv_data.sort_values('datetime').reset_index(drop=True)
            
            self.logger.info(f"CSV data processed: {len(self.csv_data)} rows, "
                           f"columns: {list(self.csv_data.columns)}")
            
        except Exception as e:
            self.logger.error(f"Failed to load CSV data: {e}")
            raise
    
    def _standardize_column_names(self) -> None:
        """Standardize column names to expected format."""
        column_mapping = {}
        
        for col in self.csv_data.columns:
            col_lower = col.lower().strip()
            
            # DateTime column variations
            if col_lower in ['datetime', 'date', 'time', 'timestamp', 'dt', 'datetime_et']:
                column_mapping[col] = 'datetime'
            # OHLC columns
            elif col_lower in ['open', 'o']:
                column_mapping[col] = 'open'
            elif col_lower in ['high', 'h']:
                column_mapping[col] = 'high'
            elif col_lower in ['low', 'l']:
                column_mapping[col] = 'low'
            elif col_lower in ['close', 'c']:
                column_mapping[col] = 'close'
            elif col_lower in ['volume', 'vol', 'v']:
                column_mapping[col] = 'volume'
        
        # Apply the mapping
        self.csv_data = self.csv_data.rename(columns=column_mapping)
        
        self.logger.debug(f"Column mapping applied: {column_mapping}")
    
    async def _convert_data_types(self) -> None:
        """Convert data types to appropriate formats."""
        try:
            # Convert datetime column - strip timezone suffix and treat as ET
            # Handle formats like "2025-05-01 04:00:00 EDT"
            datetime_strings = self.csv_data['datetime'].astype(str)
            # Remove timezone suffix (EDT, EST, etc.)
            datetime_strings = datetime_strings.str.replace(r'\s+(EDT|EST|ET)$', '', regex=True)
            self.csv_data['datetime'] = pd.to_datetime(datetime_strings)
            
            # Convert numeric columns
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if col in self.csv_data.columns:
                    self.csv_data[col] = pd.to_numeric(self.csv_data[col], errors='coerce')
            
            # Remove rows with NaN values in critical columns
            initial_len = len(self.csv_data)
            self.csv_data = self.csv_data.dropna(subset=numeric_cols)
            final_len = len(self.csv_data)
            
            if initial_len != final_len:
                self.logger.warning(f"Removed {initial_len - final_len} rows with invalid data")
            
        except Exception as e:
            self.logger.error(f"Failed to convert data types: {e}")
            raise ValueError(f"Data type conversion failed: {e}")
    
    def _get_symbol_from_data(self) -> str:
        """Extract symbol from config, filename, or data."""
        # Use override if provided
        if self.symbol_override:
            return self.symbol_override
        
        # Try to extract from filename
        filename = Path(self.file_path).stem
        # Look for common patterns like "QQQ_5min" or "QQQ-data"
        for separator in ['_', '-', '.']:
            if separator in filename:
                potential_symbol = filename.split(separator)[0].upper()
                if len(potential_symbol) >= 1 and potential_symbol.isalpha():
                    return potential_symbol
        
        # Check if there's a symbol column in the data
        if 'symbol' in self.csv_data.columns:
            symbols = self.csv_data['symbol'].unique()
            if len(symbols) == 1:
                return symbols[0]
        
        # Default fallback
        return "UNKNOWN"
    
    async def _stream_loop(self) -> None:
        """Main streaming loop that sends data to callback."""
        symbol = self._get_symbol_from_data()
        
        try:
            while self.current_row_index < len(self.csv_data) and self.is_streaming:
                # Get current row
                row = self.csv_data.iloc[self.current_row_index]
                
                # Convert to standard bar format
                standard_bar = self._convert_to_standard_format(row, symbol)
                
                # Send to callback
                if self.data_callback:
                    try:
                        if asyncio.iscoroutinefunction(self.data_callback):
                            await self.data_callback(standard_bar)
                        else:
                            self.data_callback(standard_bar)
                    except Exception as e:
                        self.logger.error(f"Error in data callback: {e}")
                
                # Move to next row
                self.current_row_index += 1
                
                # Wait before next iteration
                if self.actual_delay_seconds > 0:
                    await asyncio.sleep(self.actual_delay_seconds)
            
            # Streaming completed
            if self.current_row_index >= len(self.csv_data):
                self.logger.info("CSV streaming completed - reached end of data")
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.error(f"Error in streaming loop: {e}")
            raise
        finally:
            self.is_streaming = False
    
    def _convert_to_standard_format(self, row: pd.Series, symbol: str) -> Dict[str, Any]:
        """Convert CSV row to standardized bar format."""
        try:
            # Handle timezone-aware datetime
            dt = row['datetime']
            if dt.tzinfo is None:
                # Assume NYSE timezone (ET) if no timezone, not UTC
                # This is correct for most US stock market data
                et_tz = pytz.timezone(self.timezone)
                dt = et_tz.localize(dt)
            
            datetime_iso = dt.isoformat()
            
            # Create standard bar
            return self._create_standard_bar(
                symbol=symbol,
                datetime_str=datetime_iso,
                open_price=float(row['open']),
                high_price=float(row['high']),
                low_price=float(row['low']),
                close_price=float(row['close']),
                volume=int(row['volume']),
                source='csv'
            )
            
        except Exception as e:
            self.logger.error(f"Error converting row to standard format: {e}")
            raise ValueError(f"Data conversion failed: {e}")
    
    def get_progress(self) -> Dict[str, Any]:
        """Get streaming progress information."""
        if not self.csv_data is not None:
            return {'progress': 0, 'total': 0, 'completed': False}
        
        total_rows = len(self.csv_data)
        return {
            'progress': self.current_row_index,
            'total': total_rows,
            'completed': self.current_row_index >= total_rows,
            'percentage': (self.current_row_index / total_rows * 100) if total_rows > 0 else 0
        }