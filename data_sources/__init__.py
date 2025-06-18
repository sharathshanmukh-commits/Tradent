"""
Data Sources Package for Trading System

This package provides a standardized abstraction layer for different data sources
including CSV files and live market data feeds like Polygon.io.

All data sources implement a common interface and output standardized bar data format.
"""

from .factory import DataSourceFactory, create_data_source
from .base import BaseDataSource
from .csv_source import CSVDataSource
from .polygon_source import PolygonDataSource

__all__ = [
    'DataSourceFactory',
    'create_data_source',
    'BaseDataSource',
    'CSVDataSource',
    'PolygonDataSource',
]

__version__ = '1.0.0'