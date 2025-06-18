"""
Abstract base class for all data sources in the trading system.

This module defines the common interface that all data sources must implement,
ensuring consistency across CSV, Polygon, and other data providers.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Callable, Optional
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)


class BaseDataSource(ABC):
    """
    Abstract base class for all data sources.
    
    All data sources must implement this interface to ensure consistency
    and interchangeability across different data providers.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the data source with configuration.
        
        Args:
            config: Configuration dictionary containing data source specific settings
        """
        self.config = config
        self.is_connected = False
        self.is_streaming = False
        self.data_callback: Optional[Callable] = None
        self._subscribed_symbols: List[str] = []
        
        # Set up logging
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
    @abstractmethod
    async def connect(self) -> None:
        """
        Establish connection to the data source.
        
        This method should handle:
        - Authentication if required
        - Initial setup and validation
        - Setting is_connected flag
        
        Raises:
            ConnectionError: If connection fails
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """
        Disconnect from the data source and cleanup resources.
        
        This method should handle:
        - Graceful shutdown of streaming
        - Resource cleanup
        - Setting is_connected flag to False
        """
        pass
    
    @abstractmethod
    async def subscribe(self, symbols: List[str]) -> None:
        """
        Subscribe to data for specified symbols.
        
        Args:
            symbols: List of symbols to subscribe to (e.g., ['QQQ', 'SPY'])
            
        Raises:
            ValueError: If symbols list is empty or contains invalid symbols
            ConnectionError: If not connected to data source
        """
        pass
    
    @abstractmethod
    async def start_streaming(self, callback: Callable) -> None:
        """
        Start streaming data to the provided callback function.
        
        Args:
            callback: Function to call with each data bar. Must accept
                     a dictionary in StandardBar format.
                     
        The callback will receive data in this format:
        {
            'symbol': 'QQQ',
            'datetime': '2025-01-15T10:30:00-05:00',  # ISO format
            'open': 150.25,
            'high': 150.75,
            'low': 150.10,
            'close': 150.50,
            'volume': 1000000,
            'source': 'csv|polygon',
            'timestamp_received': '2025-01-15T10:30:01.123456'
        }
        
        Raises:
            ConnectionError: If not connected to data source
            ValueError: If no symbols are subscribed
        """
        pass
    
    @abstractmethod
    async def stop_streaming(self) -> None:
        """
        Stop streaming data.
        
        This method should:
        - Stop the streaming loop gracefully
        - Set is_streaming flag to False
        - NOT disconnect from the data source
        """
        pass
    
    def set_data_callback(self, callback: Callable) -> None:
        """
        Set the callback function for data processing.
        
        Args:
            callback: Function to call with each data bar
        """
        self.data_callback = callback
        self.logger.info(f"Data callback set for {self.__class__.__name__}")
    
    def get_subscribed_symbols(self) -> List[str]:
        """
        Get the list of currently subscribed symbols.
        
        Returns:
            List of subscribed symbols
        """
        return self._subscribed_symbols.copy()
    
    def is_data_source_connected(self) -> bool:
        """
        Check if the data source is connected.
        
        Returns:
            True if connected, False otherwise
        """
        return self.is_connected
    
    def is_data_streaming(self) -> bool:
        """
        Check if data is currently streaming.
        
        Returns:
            True if streaming, False otherwise
        """
        return self.is_streaming
    
    def _create_standard_bar(self, symbol: str, datetime_str: str, open_price: float,
                           high_price: float, low_price: float, close_price: float,
                           volume: int, source: str) -> Dict[str, Any]:
        """
        Create a standardized bar dictionary.
        
        Args:
            symbol: Symbol name
            datetime_str: ISO formatted datetime string
            open_price: Opening price
            high_price: High price
            low_price: Low price
            close_price: Closing price
            volume: Volume
            source: Data source identifier
            
        Returns:
            Standardized bar dictionary
        """
        return {
            'symbol': symbol,
            'datetime': datetime_str,
            'open': float(open_price),
            'high': float(high_price),
            'low': float(low_price),
            'close': float(close_price),
            'volume': int(volume),
            'source': source,
            'timestamp_received': datetime.now().isoformat()
        }
    
    def _validate_connection(self) -> None:
        """
        Validate that the data source is connected.
        
        Raises:
            ConnectionError: If not connected
        """
        if not self.is_connected:
            raise ConnectionError(f"{self.__class__.__name__} is not connected")
    
    def _validate_subscriptions(self) -> None:
        """
        Validate that symbols are subscribed.
        
        Raises:
            ValueError: If no symbols are subscribed
        """
        if not self._subscribed_symbols:
            raise ValueError("No symbols subscribed for streaming")
    
    def __str__(self) -> str:
        """String representation of the data source."""
        return f"{self.__class__.__name__}(connected={self.is_connected}, streaming={self.is_streaming})"
    
    def __repr__(self) -> str:
        """Detailed string representation of the data source."""
        return (f"{self.__class__.__name__}(config={self.config}, "
                f"connected={self.is_connected}, streaming={self.is_streaming}, "
                f"symbols={self._subscribed_symbols})")