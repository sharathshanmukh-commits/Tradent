"""
Base Strategy Class
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Any, Optional


class BaseStrategy(ABC):
    """Abstract base class for trading strategies."""
    
    def __init__(self, **kwargs):
        """Initialize the base strategy with parameters."""
        self.params = kwargs
        self.signals = []
    
    @abstractmethod
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Process market data to generate trading signals.
        
        Args:
            data: DataFrame with OHLCV data
                
        Returns:
            DataFrame with added signal columns
        """
        pass
    
    def get_signals(self) -> List[Dict[str, Any]]:
        """Return list of generated trading signals."""
        return self.signals
    
    def reset(self):
        """Reset strategy state."""
        self.signals = []
    
    def set_parameters(self, **params):
        """Update strategy parameters."""
        self.params.update(params)
        for key, value in params.items():
            setattr(self, key, value) 