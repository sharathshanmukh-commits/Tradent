"""
Streaming Data Buffer
Maintains a growing DataFrame for real-time data processing
"""

import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime
import logging
from utils.data_loader import DataLoader

logger = logging.getLogger(__name__)


class StreamingBuffer:
    """Buffer that maintains a growing DataFrame from streaming data"""
    
    def __init__(self, max_rows: Optional[int] = None):
        """
        Initialize streaming buffer
        
        Args:
            max_rows: Maximum number of rows to keep (None = unlimited)
        """
        self.max_rows = max_rows
        self.df = pd.DataFrame()
        self.row_count = 0
        self.column_map = {
            't': 'timestamp',
            'time': 'timestamp',
            'Time': 'timestamp',
            'datetime_ET': 'datetime',
            'date': 'datetime',
            'Date': 'datetime',
            'o': 'open',
            'Open': 'open',
            'h': 'high',
            'High': 'high',
            'l': 'low',
            'Low': 'low',
            'c': 'close',
            'Close': 'close',
            'v': 'volume',
            'Volume': 'volume',
            'vw': 'vwap',
            'n': 'trades'
        }
        
    def append_bar(self, bar_data: Dict[str, Any]) -> bool:
        """
        Append a single bar to the buffer
        
        Args:
            bar_data: Dictionary containing bar data
            
        Returns:
            bool: True if successfully appended
        """
        try:
            # Map column names
            mapped_data = {}
            for key, value in bar_data.items():
                if key.startswith('_'):  # Skip metadata fields
                    continue
                mapped_key = self.column_map.get(key, key)
                mapped_data[mapped_key] = value
                
            # Handle datetime conversion
            if 'datetime' in mapped_data and isinstance(mapped_data['datetime'], str):
                # Remove timezone suffix if present
                datetime_str = mapped_data['datetime']
                datetime_str = datetime_str.replace(' EDT', '').replace(' EST', '')
                datetime_str = datetime_str.replace(' PDT', '').replace(' PST', '')
                datetime_str = datetime_str.replace(' CDT', '').replace(' CST', '')
                mapped_data['datetime'] = pd.to_datetime(datetime_str)
                
            # Add date column
            if 'datetime' in mapped_data:
                mapped_data['date'] = mapped_data['datetime'].date()
                
            # Create DataFrame from single row
            new_row = pd.DataFrame([mapped_data])
            
            # Append to buffer
            if self.df.empty:
                self.df = new_row
            else:
                self.df = pd.concat([self.df, new_row], ignore_index=True)
                
            self.row_count += 1
            
            # Trim buffer if max_rows is set
            if self.max_rows and len(self.df) > self.max_rows:
                self.df = self.df.iloc[-self.max_rows:]
                
            logger.debug(f"Appended bar {self.row_count}: {mapped_data.get('datetime', 'N/A')}")
            return True
            
        except Exception as e:
            logger.error(f"Error appending bar: {e}")
            return False
            
    def get_dataframe(self) -> pd.DataFrame:
        """Get the current DataFrame"""
        return self.df.copy()
        
    def validate_buffer(self) -> bool:
        """Validate the buffer using DataLoader validation"""
        if self.df.empty:
            return True  # Empty buffer is valid
        return DataLoader.validate_data(self.df)
        
    def clear(self):
        """Clear the buffer"""
        self.df = pd.DataFrame()
        self.row_count = 0
        logger.info("Buffer cleared")
        
    def get_latest_bar(self) -> Optional[pd.Series]:
        """Get the most recent bar"""
        if not self.df.empty:
            return self.df.iloc[-1]
        return None
        
    def get_stats(self) -> Dict[str, Any]:
        """Get buffer statistics"""
        if self.df.empty:
            return {
                "total_rows": 0,
                "buffer_size": 0,
                "memory_usage": 0
            }
            
        return {
            "total_rows": self.row_count,
            "buffer_size": len(self.df),
            "memory_usage": self.df.memory_usage(deep=True).sum(),
            "oldest_datetime": self.df['datetime'].min() if 'datetime' in self.df else None,
            "newest_datetime": self.df['datetime'].max() if 'datetime' in self.df else None,
            "columns": list(self.df.columns)
        }
        
    def to_csv(self, filepath: str):
        """Save buffer to CSV file"""
        if not self.df.empty:
            self.df.to_csv(filepath, index=False)
            logger.info(f"Buffer saved to {filepath}")
        else:
            logger.warning("Buffer is empty, nothing to save")