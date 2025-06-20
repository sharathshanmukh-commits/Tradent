"""
CSV Exporter for LLM Enhanced Trading Signals

This module handles exporting LLM-enhanced trading signals to CSV files.
It generates two main outputs:
1. llm_signals.csv - Contains only signal bars with LLM enhancements
2. llm_strategy_results.csv - Contains all bars with LLM columns for signals
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import os
import fcntl
import json
import csv
import traceback

logger = logging.getLogger(__name__)


class LLMCSVExporter:
    """
    Exports LLM-enhanced trading data to CSV files with thread-safe operations.
    """
    
    def __init__(self, output_dir: str = "output/llm_enhanced/"):
        """
        Initialize the CSV exporter.
        
        Args:
            output_dir: Directory for CSV output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # File paths
        self.signals_csv_path = self.output_dir / "llm_signals.csv"
        self.strategy_csv_path = self.output_dir / "llm_strategy_results.csv"
        
        # Test run paths (also save to test_run for debugging)
        self.test_dir = Path("test_run")
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.test_signals_csv = self.test_dir / "test_llm_signals.csv"
        self.test_strategy_csv = self.test_dir / "test_llm_strategy_results.csv"
        
        # Column definitions
        self.signal_columns = [
            'timestamp', 'signal_id', 'symbol', 'signal_type', 
            'entry_price', 'stop_loss', 'target_price', 'risk_reward_ratio',
            'llm_rating', 'llm_choppiness', 'llm_analysis', 'llm_recommendations',
            'llm_processing_time_ms', 'llm_model_used', 'processed_at'
        ]
        
        self.strategy_columns = [
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'signal_type', 'entry_price', 'stop_loss', 'target_price',
            'signal_id', 'llm_rating', 'llm_choppiness', 'llm_analysis',
            'llm_processed'
        ]
        
        # Initialize CSV files with headers if they don't exist
        self._initialize_csv_files()
        
        # Continuous CSV settings
        self.continuous_csv_filename = "continuous_llm_signals.csv"
        self.csv_fieldnames = [
            'signal_id', 'timestamp', 'symbol', 'signal_type', 
            'entry_price', 'stop_loss', 'target_price', 'risk_reward_ratio',
            'reason', 'market_close', 'market_volume',
            'llm_rating', 'llm_choppiness', 'llm_analysis', 
            'llm_model', 'processing_time_ms', 'processed_at'
        ]
        
        logger.info(f"LLMCSVExporter initialized with output directory: {self.output_dir}")
    
    def _initialize_csv_files(self) -> None:
        """Initialize CSV files with headers if they don't exist."""
        # Initialize llm_signals.csv
        if not self.signals_csv_path.exists():
            df = pd.DataFrame(columns=self.signal_columns)
            df.to_csv(self.signals_csv_path, index=False)
            logger.info(f"Created {self.signals_csv_path} with headers")
        
        # Initialize llm_strategy_results.csv
        if not self.strategy_csv_path.exists():
            df = pd.DataFrame(columns=self.strategy_columns)
            df.to_csv(self.strategy_csv_path, index=False)
            logger.info(f"Created {self.strategy_csv_path} with headers")
    
    def export_llm_signals_csv(self, completed_signals: List[Dict]) -> str:
        """
        Export completed signals with LLM enhancements to llm_signals.csv.
        
        Args:
            completed_signals: List of signal dictionaries with LLM results
            
        Returns:
            Path to the updated CSV file
        """
        if not completed_signals:
            logger.warning("No completed signals to export")
            return str(self.signals_csv_path)
        
        try:
            # Convert signals to DataFrame format
            rows = []
            for signal in completed_signals:
                row = self._format_signal_row(signal)
                if row:
                    rows.append(row)
            
            if not rows:
                logger.warning("No valid rows to export")
                return str(self.signals_csv_path)
            
            # Create DataFrame
            df_new = pd.DataFrame(rows)
            
            # Ensure all columns are present
            for col in self.signal_columns:
                if col not in df_new.columns:
                    df_new[col] = None
            
            # Reorder columns
            df_new = df_new[self.signal_columns]
            
            # Append to existing CSV (thread-safe)
            self._append_to_csv_safe(df_new, self.signals_csv_path)
            
            logger.info(f"Exported {len(rows)} signals to {self.signals_csv_path}")
            return str(self.signals_csv_path)
            
        except Exception as e:
            logger.error(f"Error exporting signals to CSV: {str(e)}")
            raise
    
    def export_llm_strategy_results_csv(self, all_bars: List[Dict], enhanced_signals: Dict[str, Dict]) -> str:
        """
        Export all bars with LLM enhancement columns for signals.
        
        Args:
            all_bars: List of all bar data dictionaries
            enhanced_signals: Dictionary mapping signal_id to enhanced signal data
            
        Returns:
            Path to the updated CSV file
        """
        try:
            # Convert bars to DataFrame
            df = pd.DataFrame(all_bars)
            
            # Initialize LLM columns
            df['signal_id'] = None
            df['llm_rating'] = None
            df['llm_choppiness'] = None
            df['llm_analysis'] = None
            df['llm_processed'] = False
            
            # Add LLM data for bars that have signals
            for idx, row in df.iterrows():
                # Check if this bar has a signal
                if pd.notna(row.get('signal_type')):
                    # Find matching enhanced signal
                    bar_time = row.get('timestamp')
                    for signal_id, signal_data in enhanced_signals.items():
                        if signal_data.get('timestamp') == bar_time:
                            # Add LLM enhancement data
                            llm_result = signal_data.get('llm_result', {})
                            df.at[idx, 'signal_id'] = signal_id
                            df.at[idx, 'llm_rating'] = llm_result.get('rating')
                            df.at[idx, 'llm_choppiness'] = llm_result.get('choppiness')
                            df.at[idx, 'llm_analysis'] = llm_result.get('analysis')
                            df.at[idx, 'llm_processed'] = True
                            break
            
            # Ensure all columns are present and in correct order
            for col in self.strategy_columns:
                if col not in df.columns:
                    df[col] = None
            
            df = df[self.strategy_columns]
            
            # Save to CSV (overwrite for strategy results)
            df.to_csv(self.strategy_csv_path, index=False)
            
            logger.info(f"Exported {len(df)} bars to {self.strategy_csv_path}")
            return str(self.strategy_csv_path)
            
        except Exception as e:
            logger.error(f"Error exporting strategy results to CSV: {str(e)}")
            raise
    
    def append_to_continuous_csv(self, enhanced_signal: Dict) -> None:
        """
        Append an enhanced signal to the continuous CSV file.
        
        Args:
            enhanced_signal: Enhanced signal data with LLM results
        """
        try:
            # Extract data for CSV row
            signal_data = enhanced_signal.get('signal_data', {})
            market_snapshot = enhanced_signal.get('market_snapshot', {})
            llm_result = enhanced_signal.get('llm_result', {})
            
            row = {
                'signal_id': enhanced_signal.get('signal_id'),
                'timestamp': enhanced_signal.get('timestamp'),
                'symbol': signal_data.get('symbol', 'UNKNOWN'),
                'signal_type': signal_data.get('type'),
                'entry_price': signal_data.get('entry_price'),
                'stop_loss': signal_data.get('stop_loss'),
                'target_price': signal_data.get('target_price'),
                'risk_reward_ratio': signal_data.get('risk_reward_ratio'),
                'reason': signal_data.get('reason'),
                'market_close': market_snapshot.get('close'),
                'market_volume': market_snapshot.get('volume'),
                'llm_rating': llm_result.get('rating'),
                'llm_choppiness': llm_result.get('choppiness'),
                'llm_analysis': llm_result.get('analysis', '').replace('\n', ' ').replace(',', ';'),  # Clean for CSV
                'llm_model': llm_result.get('llm_model_used'),
                'processing_time_ms': llm_result.get('processing_time_ms'),
                'processed_at': enhanced_signal.get('processed_at', datetime.now().isoformat())
            }
            
            # Create continuous CSV filename
            csv_filename = os.path.join(self.output_dir, self.continuous_csv_filename)
            
            # Write header if file doesn't exist
            file_exists = os.path.exists(csv_filename)
            
            with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                    logger.info(f"Created continuous CSV file: {csv_filename}")
                
                writer.writerow(row)
            
            logger.debug(f"Appended signal {enhanced_signal.get('signal_id')} to continuous CSV")
            
        except Exception as e:
            logger.error(f"Failed to append signal to CSV: {str(e)}")
            logger.debug(traceback.format_exc())
    
    def _format_signal_row(self, signal_data: Dict) -> Optional[Dict]:
        """
        Format a signal dictionary into a row for CSV export.
        
        Args:
            signal_data: Signal data with LLM results
            
        Returns:
            Formatted row dictionary or None if invalid
        """
        try:
            # Extract base signal data
            signal_info = signal_data.get('signal_data', {})
            llm_result = signal_data.get('llm_result', {})
            
            # Calculate risk/reward if not present
            entry = signal_info.get('entry_price', 0)
            stop = signal_info.get('stop_loss', 0)
            target = signal_info.get('target_price', 0)
            
            risk = abs(entry - stop) if entry and stop else 0
            reward = abs(target - entry) if target and entry else 0
            risk_reward = reward / risk if risk > 0 else 0
            
            row = {
                'timestamp': signal_data.get('created_at') or signal_info.get('timestamp'),
                'signal_id': signal_data.get('signal_id'),
                'symbol': signal_info.get('symbol', 'UNKNOWN'),
                'signal_type': signal_info.get('signal_type'),
                'entry_price': entry,
                'stop_loss': stop,
                'target_price': target,
                'risk_reward_ratio': round(risk_reward, 2),
                'llm_rating': llm_result.get('rating'),
                'llm_choppiness': llm_result.get('choppiness'),
                'llm_analysis': llm_result.get('analysis', ''),
                'llm_recommendations': llm_result.get('recommendations'),
                'llm_processing_time_ms': signal_data.get('processing_time_seconds', 0) * 1000,
                'llm_model_used': llm_result.get('llm_model_used', 'unknown'),
                'processed_at': signal_data.get('completed_at') or datetime.now()
            }
            
            return row
            
        except Exception as e:
            logger.error(f"Error formatting signal row: {str(e)}")
            return None
    
    def _append_to_csv_safe(self, df: pd.DataFrame, file_path: Path) -> None:
        """
        Thread-safe append to CSV file using file locking.
        
        Args:
            df: DataFrame to append
            file_path: Path to CSV file
        """
        # Convert datetime columns to string format for CSV
        datetime_columns = df.select_dtypes(include=['datetime64']).columns
        for col in datetime_columns:
            df[col] = df[col].astype(str)
        
        # Use file locking for thread safety
        with open(file_path, 'a') as f:
            # Acquire exclusive lock
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                # Check if file is empty (needs header)
                f.seek(0, 2)  # Go to end
                file_empty = f.tell() == 0
                
                # Write data
                df.to_csv(f, header=file_empty, index=False)
            finally:
                # Release lock
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    
    def get_export_stats(self) -> Dict:
        """
        Get statistics about exported CSV files.
        
        Returns:
            Dictionary with file statistics
        """
        stats = {}
        
        # Check signals CSV
        if self.signals_csv_path.exists():
            try:
                df = pd.read_csv(self.signals_csv_path)
                stats['signals_csv'] = {
                    'path': str(self.signals_csv_path),
                    'rows': len(df),
                    'size_bytes': self.signals_csv_path.stat().st_size,
                    'last_modified': datetime.fromtimestamp(
                        self.signals_csv_path.stat().st_mtime
                    ).isoformat()
                }
            except Exception as e:
                stats['signals_csv'] = {'error': str(e)}
        
        # Check strategy CSV
        if self.strategy_csv_path.exists():
            try:
                df = pd.read_csv(self.strategy_csv_path)
                stats['strategy_csv'] = {
                    'path': str(self.strategy_csv_path),
                    'rows': len(df),
                    'size_bytes': self.strategy_csv_path.stat().st_size,
                    'last_modified': datetime.fromtimestamp(
                        self.strategy_csv_path.stat().st_mtime
                    ).isoformat()
                }
            except Exception as e:
                stats['strategy_csv'] = {'error': str(e)}
        
        return stats
    
    def export_summary_report(self, summary_data: Dict) -> str:
        """
        Export a summary report of LLM processing statistics.
        
        Args:
            summary_data: Dictionary with processing statistics
            
        Returns:
            Path to summary report file
        """
        summary_path = self.output_dir / "llm_processing_summary.json"
        
        try:
            # Add timestamp
            summary_data['report_generated_at'] = datetime.now().isoformat()
            
            # Write summary
            with open(summary_path, 'w') as f:
                json.dump(summary_data, f, indent=2, default=str)
            
            logger.info(f"Exported summary report to {summary_path}")
            return str(summary_path)
            
        except Exception as e:
            logger.error(f"Error exporting summary report: {str(e)}")
            raise