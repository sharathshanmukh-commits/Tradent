# core/data_loader.py
import pandas as pd
from typing import Optional, Dict, Any

class DataLoader:
    """Loads and preprocesses market data for strategy testing."""
    
    @staticmethod
    def load_csv(file_path: str, **kwargs) -> pd.DataFrame:
        """
        Load data from CSV file.
        
        Args:
            file_path: Path to CSV file
            **kwargs: Additional parameters for pd.read_csv
        
        Returns:
            DataFrame with OHLCV data
        """
        data = pd.read_csv(file_path, **kwargs)
        
        # Standardize column names
        column_map = {
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
            'Volume': 'volume'
        }
        
        # Only rename columns that exist
        rename_dict = {k: v for k, v in column_map.items() if k in data.columns}
        if rename_dict:
            data = data.rename(columns=rename_dict)
        
        # Ensure datetime column is in datetime format
        if 'datetime' in data.columns:
            # Remove ALL timezone suffixes if present (EST, EDT, PST, PDT, CST, CDT, etc.)
            if data['datetime'].dtype == 'object':
                # More aggressive timezone cleaning
                data['datetime'] = data['datetime'].str.replace(r'\s+[A-Z]{2,4}$', '', regex=True)
                data['datetime'] = data['datetime'].str.replace(r'[+-]\d{2}:\d{2}$', '', regex=True)  # Remove UTC offsets
                data['datetime'] = data['datetime'].str.strip()  # Remove any trailing whitespace
            
            # Convert to datetime with error handling - suppress warnings
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                data['datetime'] = pd.to_datetime(data['datetime'], errors='coerce')
            
            # Add date column for convenience
            data['date'] = data['datetime'].dt.date
        
        return data
    
    @staticmethod
    def validate_data(data: pd.DataFrame) -> bool:
        """
        Validate that data has required columns and is properly formatted.
        
        Args:
            data: DataFrame to validate
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        required_columns = ['open', 'high', 'low', 'close', 'datetime']
        
        # Check if all required columns exist
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            print(f"Missing required columns: {missing_columns}")
            return False
        
        # Check if data is not empty
        if len(data) == 0:
            print("Data is empty")
            return False
        
        # Check for NaN values in critical columns
        critical_columns = ['open', 'high', 'low', 'close']
        for col in critical_columns:
            if data[col].isna().any():
                print(f"NaN values found in column: {col}")
                return False
        
        # Check if high >= low for all bars
        if (data['high'] < data['low']).any():
            print("Invalid data: high < low found")
            return False
        
        return True
    
    @staticmethod
    def process_single_row(row_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single row of data with column mapping
        
        Args:
            row_data: Dictionary containing single row data
            
        Returns:
            Dict with standardized column names
        """
        column_map = {
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
        
        # Map column names
        processed_data = {}
        for key, value in row_data.items():
            if key.startswith('_'):  # Skip metadata fields
                continue
            mapped_key = column_map.get(key, key)
            processed_data[mapped_key] = value
            
        # Handle datetime conversion
        if 'datetime' in processed_data and isinstance(processed_data['datetime'], str):
            # Remove timezone suffix if present
            datetime_str = processed_data['datetime']
            datetime_str = datetime_str.replace(' EDT', '').replace(' EST', '')
            datetime_str = datetime_str.replace(' PDT', '').replace(' PST', '')
            datetime_str = datetime_str.replace(' CDT', '').replace(' CST', '')
            processed_data['datetime'] = pd.to_datetime(datetime_str)
            
        # Add date column
        if 'datetime' in processed_data:
            if isinstance(processed_data['datetime'], pd.Timestamp):
                processed_data['date'] = processed_data['datetime'].date()
            elif isinstance(processed_data['datetime'], str):
                processed_data['date'] = pd.to_datetime(processed_data['datetime']).date()
                
        return processed_data 