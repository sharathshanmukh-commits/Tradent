"""
Data Source Factory for creating data source instances.

This module provides a factory pattern implementation for creating different
types of data sources based on configuration. It handles validation and
instantiation of CSV, Polygon, and other data source types.
"""

from typing import Dict, Any, Type
import logging

from .csv_source import CSVDataSource
from .polygon_source import PolygonDataSource
from .base import BaseDataSource

logger = logging.getLogger(__name__)


class DataSourceFactory:
    """
    Factory class for creating data source instances.
    
    This factory supports creating different types of data sources based on
    configuration and provides validation and error handling.
    """
    
    # Registry of supported data source types
    _DATA_SOURCE_TYPES: Dict[str, Type[BaseDataSource]] = {
        'csv': CSVDataSource,
        'polygon': PolygonDataSource,
    }
    
    @classmethod
    def create(cls, config: Dict[str, Any]) -> BaseDataSource:
        """
        Create a data source instance based on configuration.
        
        Args:
            config: Configuration dictionary containing data source settings.
                   Must include 'type' field specifying the data source type.
                   
        Example config structure:
        {
            "type": "csv",
            "csv": {
                "file_path": "data/QQQ_5min.csv",
                "speed": 10.0,
                "delay_ms": 100,
                "symbol": "QQQ"
            }
        }
        
        Returns:
            BaseDataSource: Configured data source instance
            
        Raises:
            ValueError: If config is invalid or data source type is unsupported
            KeyError: If required configuration keys are missing
            TypeError: If configuration values are of wrong type
        """
        try:
            # Validate configuration structure
            cls._validate_config(config)
            
            # Extract data source type
            data_source_type = config.get('type', '').lower()
            
            # Get data source class
            data_source_class = cls._get_data_source_class(data_source_type)
            
            # Extract type-specific configuration
            type_config = cls._extract_type_config(config, data_source_type)
            
            # Create and return data source instance
            logger.info(f"Creating {data_source_type} data source")
            data_source = data_source_class(type_config)
            
            logger.info(f"Successfully created {data_source_type} data source: {data_source}")
            return data_source
            
        except Exception as e:
            logger.error(f"Failed to create data source: {e}")
            raise
    
    @classmethod
    def get_supported_types(cls) -> list:
        """
        Get list of supported data source types.
        
        Returns:
            List of supported data source type strings
        """
        return list(cls._DATA_SOURCE_TYPES.keys())
    
    @classmethod
    def register_data_source(cls, type_name: str, data_source_class: Type[BaseDataSource]) -> None:
        """
        Register a new data source type.
        
        This allows for extending the factory with custom data source implementations.
        
        Args:
            type_name: String identifier for the data source type
            data_source_class: Class that implements BaseDataSource interface
            
        Raises:
            ValueError: If type_name is invalid or class doesn't inherit from BaseDataSource
        """
        if not type_name or not isinstance(type_name, str):
            raise ValueError("type_name must be a non-empty string")
        
        if not issubclass(data_source_class, BaseDataSource):
            raise ValueError("data_source_class must inherit from BaseDataSource")
        
        cls._DATA_SOURCE_TYPES[type_name.lower()] = data_source_class
        logger.info(f"Registered new data source type: {type_name}")
    
    @classmethod
    def _validate_config(cls, config: Dict[str, Any]) -> None:
        """
        Validate the basic structure of the configuration.
        
        Args:
            config: Configuration dictionary to validate
            
        Raises:
            ValueError: If configuration structure is invalid
            TypeError: If config is not a dictionary
        """
        if not isinstance(config, dict):
            raise TypeError("Configuration must be a dictionary")
        
        if not config:
            raise ValueError("Configuration cannot be empty")
        
        if 'type' not in config:
            raise ValueError("Configuration must include 'type' field")
        
        data_source_type = config.get('type')
        if not isinstance(data_source_type, str) or not data_source_type.strip():
            raise ValueError("'type' field must be a non-empty string")
    
    @classmethod
    def _get_data_source_class(cls, data_source_type: str) -> Type[BaseDataSource]:
        """
        Get the data source class for the specified type.
        
        Args:
            data_source_type: String identifier for data source type
            
        Returns:
            BaseDataSource class for the specified type
            
        Raises:
            ValueError: If data source type is not supported
        """
        if data_source_type not in cls._DATA_SOURCE_TYPES:
            supported_types = ', '.join(cls._DATA_SOURCE_TYPES.keys())
            raise ValueError(
                f"Unsupported data source type: '{data_source_type}'. "
                f"Supported types: {supported_types}"
            )
        
        return cls._DATA_SOURCE_TYPES[data_source_type]
    
    @classmethod
    def _extract_type_config(cls, config: Dict[str, Any], data_source_type: str) -> Dict[str, Any]:
        """
        Extract and validate type-specific configuration.
        
        Args:
            config: Full configuration dictionary
            data_source_type: Data source type identifier
            
        Returns:
            Type-specific configuration dictionary
            
        Raises:
            ValueError: If type-specific configuration is missing or invalid
        """
        # Look for type-specific configuration section
        if data_source_type in config:
            type_config = config[data_source_type]
            if not isinstance(type_config, dict):
                raise ValueError(f"Configuration for '{data_source_type}' must be a dictionary")
        else:
            # Some data sources might not need a separate config section
            type_config = {}
        
        # Validate type-specific requirements
        cls._validate_type_config(data_source_type, type_config)
        
        return type_config
    
    @classmethod
    def _validate_type_config(cls, data_source_type: str, type_config: Dict[str, Any]) -> None:
        """
        Validate type-specific configuration requirements.
        
        Args:
            data_source_type: Data source type identifier
            type_config: Type-specific configuration dictionary
            
        Raises:
            ValueError: If required configuration is missing or invalid
        """
        if data_source_type == 'csv':
            cls._validate_csv_config(type_config)
        elif data_source_type == 'polygon':
            cls._validate_polygon_config(type_config)
    
    @classmethod
    def _validate_csv_config(cls, config: Dict[str, Any]) -> None:
        """
        Validate CSV-specific configuration.
        
        Args:
            config: CSV configuration dictionary
            
        Raises:
            ValueError: If CSV configuration is invalid
        """
        if not config.get('file_path'):
            raise ValueError("CSV configuration must include 'file_path'")
        
        # Validate optional numeric parameters
        speed = config.get('speed')
        if speed is not None and (not isinstance(speed, (int, float)) or speed <= 0):
            raise ValueError("'speed' must be a positive number")
        
        delay_ms = config.get('delay_ms')
        if delay_ms is not None and (not isinstance(delay_ms, (int, float)) or delay_ms < 0):
            raise ValueError("'delay_ms' must be a non-negative number")
    
    @classmethod
    def _validate_polygon_config(cls, config: Dict[str, Any]) -> None:
        """
        Validate Polygon-specific configuration.
        
        Args:
            config: Polygon configuration dictionary
            
        Raises:
            ValueError: If Polygon configuration is invalid
        """
        if not config.get('api_key'):
            raise ValueError("Polygon configuration must include 'api_key'")
        
        # Validate symbols list
        symbols = config.get('symbols')
        if symbols is not None:
            if not isinstance(symbols, list) or not symbols:
                raise ValueError("'symbols' must be a non-empty list")
            if not all(isinstance(symbol, str) and symbol.strip() for symbol in symbols):
                raise ValueError("All symbols must be non-empty strings")
        
        # Validate subscription types
        sub_types = config.get('subscription_types')
        if sub_types is not None:
            if not isinstance(sub_types, list):
                raise ValueError("'subscription_types' must be a list")
            valid_types = ['A', 'T', 'Q']  # Aggregates, Trades, Quotes
            for sub_type in sub_types:
                if sub_type not in valid_types:
                    raise ValueError(f"Invalid subscription type: '{sub_type}'. Valid types: {valid_types}")
        
        # Validate numeric parameters
        reconnect_attempts = config.get('reconnect_attempts')
        if reconnect_attempts is not None and (not isinstance(reconnect_attempts, int) or reconnect_attempts < 0):
            raise ValueError("'reconnect_attempts' must be a non-negative integer")
        
        reconnect_delay_ms = config.get('reconnect_delay_ms')
        if reconnect_delay_ms is not None and (not isinstance(reconnect_delay_ms, (int, float)) or reconnect_delay_ms < 0):
            raise ValueError("'reconnect_delay_ms' must be a non-negative number")


# Convenience function for creating data sources
def create_data_source(config: Dict[str, Any]) -> BaseDataSource:
    """
    Convenience function to create a data source instance.
    
    This is a shortcut for DataSourceFactory.create(config).
    
    Args:
        config: Configuration dictionary
        
    Returns:
        BaseDataSource: Configured data source instance
    """
    return DataSourceFactory.create(config)