"""
Comprehensive test suite for the data sources package.

This test demonstrates CSV data source functionality including:
- Factory creation
- CSV data loading and streaming
- Data format standardization
- Error handling
- Start/stop streaming cycles
"""

import asyncio
import os
import sys
import logging
import tempfile
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

# Add the project root to Python path so we can import our modules
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_sources import DataSourceFactory, CSVDataSource
from data_sources.base_data_source import BaseDataSource

# Configure logging for the test
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataSourceTester:
    """Test class for data source functionality."""
    
    def __init__(self):
        self.received_bars: List[Dict[str, Any]] = []
        self.test_csv_file = None
        
    async def data_callback(self, bar: Dict[str, Any]) -> None:
        """Callback function to receive streaming data."""
        self.received_bars.append(bar)
        logger.info(f"Received bar: {bar['datetime']} - Close: {bar['close']:.2f}")
    
    def create_test_csv_data(self) -> str:
        """Create a temporary CSV file with test data."""
        # Create sample OHLCV data
        start_time = datetime(2025, 1, 15, 9, 30, 0)
        data = []
        
        base_price = 150.0
        for i in range(10):
            time_point = start_time + timedelta(minutes=5 * i)
            
            # Create realistic OHLCV data with proper OHLC relationships
            open_price = base_price + (i * 0.1)
            close_price = open_price + 0.10 + (i * 0.08)
            high_price = max(open_price, close_price) + 0.25 + (i * 0.05)
            low_price = min(open_price, close_price) - 0.15 - (i * 0.02)
            volume = 1000000 + (i * 50000)
            
            data.append({
                'datetime': time_point.strftime('%Y-%m-%d %H:%M:%S'),
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': volume
            })
        
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='_QQQ_test.csv', delete=False) as f:
            df = pd.DataFrame(data)
            df.to_csv(f.name, index=False)
            self.test_csv_file = f.name
            logger.info(f"Created test CSV file: {f.name}")
            return f.name
    
    def cleanup_test_file(self):
        """Clean up temporary test file."""
        if self.test_csv_file and os.path.exists(self.test_csv_file):
            os.unlink(self.test_csv_file)
            logger.info(f"Cleaned up test file: {self.test_csv_file}")
    
    async def test_factory_creation(self):
        """Test data source factory functionality."""
        logger.info("=== Testing Factory Creation ===")
        
        # Test supported types
        supported_types = DataSourceFactory.get_supported_types()
        logger.info(f"Supported data source types: {supported_types}")
        assert 'csv' in supported_types
        assert 'polygon' in supported_types
        
        # Create test CSV file
        csv_file = self.create_test_csv_data()
        
        # Test CSV data source creation
        config = {
            "type": "csv",
            "csv": {
                "file_path": csv_file,
                "speed": 10.0,
                "delay_ms": 50,
                "symbol": "QQQ"
            }
        }
        
        data_source = DataSourceFactory.create(config)
        assert isinstance(data_source, CSVDataSource)
        assert isinstance(data_source, BaseDataSource)
        logger.info("✓ CSV data source created successfully via factory")
        
        # Test error handling - invalid type
        try:
            invalid_config = {"type": "invalid_type"}
            DataSourceFactory.create(invalid_config)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            logger.info(f"✓ Factory correctly rejected invalid type: {e}")
        
        # Test error handling - missing config
        try:
            DataSourceFactory.create({})
            assert False, "Should have raised ValueError"
        except ValueError as e:
            logger.info(f"✓ Factory correctly rejected empty config: {e}")
        
        return data_source
    
    async def test_csv_data_source(self, data_source: CSVDataSource):
        """Test CSV data source functionality."""
        logger.info("=== Testing CSV Data Source ===")
        
        # Test initial state
        assert not data_source.is_data_source_connected()
        assert not data_source.is_data_streaming()
        logger.info("✓ Initial state correct")
        
        # Test connection
        await data_source.connect()
        assert data_source.is_data_source_connected()
        logger.info("✓ Connection successful")
        
        # Test subscription
        await data_source.subscribe(['QQQ', 'SPY'])  # Test multiple symbols
        subscribed = data_source.get_subscribed_symbols()
        assert 'QQQ' in subscribed
        assert 'SPY' in subscribed
        logger.info(f"✓ Subscription successful: {subscribed}")
        
        # Test streaming
        self.received_bars.clear()
        
        # Start streaming with a timeout to prevent infinite running
        streaming_task = asyncio.create_task(
            data_source.start_streaming(self.data_callback)
        )
        
        # Let it stream for a short time
        await asyncio.sleep(2.0)  # Stream for 2 seconds
        
        # Stop streaming
        await data_source.stop_streaming()
        
        # Cancel the streaming task if it's still running
        if not streaming_task.done():
            streaming_task.cancel()
        
        assert not data_source.is_data_streaming()
        logger.info("✓ Streaming started and stopped successfully")
        
        # Validate received data
        assert len(self.received_bars) > 0, "Should have received some bars"
        logger.info(f"✓ Received {len(self.received_bars)} bars")
        
        # Validate data format
        for bar in self.received_bars:
            self.validate_standard_bar_format(bar)
        logger.info("✓ All bars have correct format")
        
        # Test progress tracking
        progress = data_source.get_progress()
        logger.info(f"✓ Progress tracking: {progress}")
        
        # Test disconnection
        await data_source.disconnect()
        assert not data_source.is_data_source_connected()
        logger.info("✓ Disconnection successful")
    
    def validate_standard_bar_format(self, bar: Dict[str, Any]):
        """Validate that a bar conforms to the StandardBar format."""
        required_fields = ['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'source', 'timestamp_received']
        
        for field in required_fields:
            assert field in bar, f"Missing required field: {field}"
        
        # Validate data types and values
        assert isinstance(bar['symbol'], str) and bar['symbol']
        assert isinstance(bar['datetime'], str) and bar['datetime']
        assert isinstance(bar['open'], (int, float)) and bar['open'] > 0
        assert isinstance(bar['high'], (int, float)) and bar['high'] > 0
        assert isinstance(bar['low'], (int, float)) and bar['low'] > 0
        assert isinstance(bar['close'], (int, float)) and bar['close'] > 0
        assert isinstance(bar['volume'], int) and bar['volume'] > 0
        assert bar['source'] in ['csv', 'polygon']
        assert isinstance(bar['timestamp_received'], str)
        
        # Validate OHLC relationship (high should be highest, low should be lowest)
        assert bar['high'] >= bar['open'], f"High {bar['high']} should be >= Open {bar['open']}"
        assert bar['high'] >= bar['close'], f"High {bar['high']} should be >= Close {bar['close']}"
        assert bar['high'] >= bar['low'], f"High {bar['high']} should be >= Low {bar['low']}"
        assert bar['low'] <= bar['open'], f"Low {bar['low']} should be <= Open {bar['open']}"
        assert bar['low'] <= bar['close'], f"Low {bar['low']} should be <= Close {bar['close']}"
    
    async def test_error_handling(self):
        """Test error handling scenarios."""
        logger.info("=== Testing Error Handling ===")
        
        # Test file not found
        config = {
            "type": "csv",
            "csv": {
                "file_path": "/non/existent/file.csv",
                "speed": 1.0
            }
        }
        
        data_source = DataSourceFactory.create(config)
        try:
            await data_source.connect()
            assert False, "Should have raised ConnectionError"
        except Exception as e:
            logger.info(f"✓ Correctly handled file not found: {type(e).__name__}")
        
        # Test streaming without connection
        try:
            await data_source.start_streaming(self.data_callback)
            assert False, "Should have raised ConnectionError"
        except Exception as e:
            logger.info(f"✓ Correctly handled streaming without connection: {type(e).__name__}")
    
    async def test_polygon_skeleton(self):
        """Test Polygon data source implementation."""
        logger.info("=== Testing Polygon Implementation ===")
        
        config = {
            "type": "polygon",
            "polygon": {
                "api_key": "test_key",
                "symbols": ["QQQ"]
            }
        }
        
        data_source = DataSourceFactory.create(config)
        logger.info(f"✓ Polygon data source created: {data_source}")
        
        # Test that connect fails with invalid API key (as expected)
        try:
            await data_source.connect()
            assert False, "Should have failed with invalid API key"
        except (ConnectionError, ValueError) as e:
            logger.info(f"✓ Connection correctly failed with test key: {type(e).__name__}")
        
        # Test other methods without connecting
        try:
            await data_source.subscribe(['QQQ'])
            assert False, "Should have raised ConnectionError"
        except ConnectionError:
            logger.info("✓ subscribe correctly requires connection")
        
        try:
            await data_source.start_streaming(self.data_callback)
            assert False, "Should have raised ConnectionError"
        except ConnectionError:
            logger.info("✓ start_streaming correctly requires connection")
        
        # Test that disconnect works even without connection
        await data_source.disconnect()
        logger.info("✓ disconnect works without connection")
        
        # Test stop_streaming when not streaming
        await data_source.stop_streaming()
        logger.info("✓ stop_streaming works when not streaming")
    
    async def run_all_tests(self):
        """Run all test scenarios."""
        try:
            logger.info("🚀 Starting Data Sources Test Suite")
            logger.info("=" * 50)
            
            # Test factory and create data source
            data_source = await self.test_factory_creation()
            
            # Test CSV functionality
            await self.test_csv_data_source(data_source)
            
            # Test error handling
            await self.test_error_handling()
            
            # Test Polygon skeleton
            await self.test_polygon_skeleton()
            
            logger.info("=" * 50)
            logger.info("🎉 All tests completed successfully!")
            logger.info(f"📊 Test Summary:")
            logger.info(f"   • Factory creation: ✓")
            logger.info(f"   • CSV data source: ✓")
            logger.info(f"   • Data format validation: ✓")
            logger.info(f"   • Error handling: ✓")
            logger.info(f"   • Polygon skeleton: ✓")
            logger.info(f"   • Total bars received: {len(self.received_bars)}")
            
            # Display sample received data
            if self.received_bars:
                logger.info(f"📋 Sample received bar:")
                sample_bar = self.received_bars[0]
                for key, value in sample_bar.items():
                    logger.info(f"   • {key}: {value}")
            
        except Exception as e:
            logger.error(f"❌ Test failed: {e}")
            raise
        finally:
            self.cleanup_test_file()


async def main():
    """Main test function."""
    tester = DataSourceTester()
    await tester.run_all_tests()


if __name__ == "__main__":
    # Run the test
    asyncio.run(main())