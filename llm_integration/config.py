"""
LLM Integration Configuration

This module provides configuration management for the LLM integration system.
"""

import os
from typing import Dict, Any, Optional
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)


class LLMConfig:
    """
    Configuration management for LLM integration.
    """
    
    # Default configuration values
    DEFAULTS = {
        'enabled': True,
        'api_key': None,  # Will use OPENROUTER_API_KEY env var
        'model': 'google/gemini-2.5-flash',
        
        # Memory store settings
        'memory_retention_minutes': 60,
        'max_memory_signals': 1000,
        
        # Worker settings
        'batch_size': 1,
        'poll_interval': 1.0,
        'cleanup_interval_minutes': 30,
        'max_retries': 3,
        
        # Rate limiting
        'max_requests_per_minute': 14,
        'max_tokens_per_minute': 950000,
        'estimated_tokens_per_request': 50000,
        
        # CSV export settings
        'csv_output_directory': 'output/llm_enhanced/',
        'batch_export_interval_seconds': 60,
        
        # Signal formatting
        'lookback_periods': 20,
        'indicators_to_include': [
            'rsi', 'atr', 'sma_20', 'sma_50', 'volume_ratio',
            'bb_upper', 'bb_lower', 'bb_middle', 'stochastic_k', 'stochastic_d'
        ],
        
        # Database settings
        'db_batch_size': 10,
        'db_async_writes': True
    }
    
    def __init__(self, config_dict: Optional[Dict] = None, config_file: Optional[str] = None):
        """
        Initialize LLM configuration.
        
        Args:
            config_dict: Dictionary with configuration values
            config_file: Path to configuration file (JSON)
        """
        # Start with defaults
        self.config = self.DEFAULTS.copy()
        
        # Load from file if provided
        if config_file:
            self._load_from_file(config_file)
        
        # Override with provided dict
        if config_dict:
            self.config.update(config_dict)
        
        # Load API key from environment if not provided
        if not self.config['api_key']:
            self.config['api_key'] = os.getenv('OPENROUTER_API_KEY')
        
        # Validate configuration
        self._validate_config()
        
        logger.info(f"LLM configuration loaded: enabled={self.config['enabled']}, "
                   f"model={self.config['model']}")
    
    def _load_from_file(self, config_file: str) -> None:
        """
        Load configuration from JSON file.
        
        Args:
            config_file: Path to configuration file
        """
        try:
            config_path = Path(config_file)
            if config_path.exists():
                with open(config_path, 'r') as f:
                    file_config = json.load(f)
                    
                # Extract LLM section if it exists
                if 'llm' in file_config:
                    self.config.update(file_config['llm'])
                else:
                    self.config.update(file_config)
                    
                logger.info(f"Loaded LLM configuration from {config_file}")
            else:
                logger.warning(f"Configuration file {config_file} not found")
                
        except Exception as e:
            logger.error(f"Error loading configuration file: {str(e)}")
    
    def _validate_config(self) -> None:
        """
        Validate configuration values.
        """
        # Check API key
        if self.config['enabled'] and not self.config['api_key']:
            logger.warning("LLM enabled but no API key provided. Set OPENROUTER_API_KEY environment variable.")
        
        # Validate numeric values
        numeric_fields = [
            'memory_retention_minutes', 'max_memory_signals', 'batch_size',
            'poll_interval', 'max_retries', 'max_requests_per_minute',
            'max_tokens_per_minute', 'estimated_tokens_per_request',
            'lookback_periods'
        ]
        
        for field in numeric_fields:
            if field in self.config:
                try:
                    self.config[field] = float(self.config[field]) if 'interval' in field else int(self.config[field])
                except ValueError:
                    logger.error(f"Invalid value for {field}: {self.config[field]}")
                    self.config[field] = self.DEFAULTS[field]
        
        # Create output directory if needed
        output_dir = Path(self.config['csv_output_directory'])
        output_dir.mkdir(parents=True, exist_ok=True)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value.
        
        Args:
            key: Configuration key
            value: Configuration value
        """
        self.config[key] = value
        logger.debug(f"Set configuration: {key} = {value}")
    
    def update(self, updates: Dict[str, Any]) -> None:
        """
        Update multiple configuration values.
        
        Args:
            updates: Dictionary of updates
        """
        self.config.update(updates)
        self._validate_config()
    
    def is_enabled(self) -> bool:
        """
        Check if LLM integration is enabled.
        
        Returns:
            True if enabled
        """
        return bool(self.config.get('enabled', False))
    
    def get_worker_config(self) -> Dict[str, Any]:
        """
        Get configuration for LLM worker.
        
        Returns:
            Worker-specific configuration
        """
        return {
            'api_key': self.config['api_key'],
            'model': self.config['model'],
            'batch_size': self.config['batch_size'],
            'poll_interval': self.config['poll_interval'],
            'cleanup_interval_minutes': self.config['cleanup_interval_minutes'],
            'max_retries': self.config['max_retries']
        }
    
    def get_memory_store_config(self) -> Dict[str, Any]:
        """
        Get configuration for memory store.
        
        Returns:
            Memory store configuration
        """
        return {
            'max_signals': self.config['max_memory_signals'],
            'retention_minutes': self.config['memory_retention_minutes']
        }
    
    def get_formatter_config(self) -> Dict[str, Any]:
        """
        Get configuration for signal formatter.
        
        Returns:
            Formatter configuration
        """
        return {
            'lookback_periods': self.config['lookback_periods'],
            'indicators_to_include': self.config['indicators_to_include']
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Get full configuration as dictionary.
        
        Returns:
            Configuration dictionary
        """
        return self.config.copy()
    
    def save_to_file(self, file_path: str) -> None:
        """
        Save configuration to JSON file.
        
        Args:
            file_path: Path to save configuration
        """
        try:
            with open(file_path, 'w') as f:
                json.dump({'llm': self.config}, f, indent=2)
            logger.info(f"Saved LLM configuration to {file_path}")
        except Exception as e:
            logger.error(f"Error saving configuration: {str(e)}")
    
    def __repr__(self) -> str:
        """String representation."""
        return f"LLMConfig(enabled={self.is_enabled()}, model={self.config['model']})"


# Global configuration instance
_global_config: Optional[LLMConfig] = None


def get_config() -> LLMConfig:
    """
    Get global LLM configuration instance.
    
    Returns:
        Global configuration instance
    """
    global _global_config
    if _global_config is None:
        _global_config = LLMConfig()
    return _global_config


def set_config(config: LLMConfig) -> None:
    """
    Set global LLM configuration instance.
    
    Args:
        config: Configuration instance
    """
    global _global_config
    _global_config = config


def load_config(config_file: str = "config.json") -> LLMConfig:
    """
    Load and set global configuration from file.
    
    Args:
        config_file: Path to configuration file
        
    Returns:
        Loaded configuration instance
    """
    config = LLMConfig(config_file=config_file)
    set_config(config)
    return config