"""
LLM Background Worker

This module implements the background worker that continuously processes signals
from memory store through LLM enhancement and exports results.
"""

import asyncio
import logging
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta, time
import traceback
import pandas as pd
import numpy as np

from .memory_store import SignalMemoryStore
from .signal_formatter_patient_labels import format_signal_data
from .llm_processor_simple import SimpleLLMProcessor
from .response_parser import LLMResponseParser
from .csv_exporter import LLMCSVExporter

logger = logging.getLogger(__name__)


class LLMWorker:
    """
    Background worker for processing trading signals through LLM enhancement.
    """
    
    def __init__(self, 
                 memory_store: SignalMemoryStore,
                 db_manager: Optional[Any] = None,
                 csv_exporter: Optional[LLMCSVExporter] = None,
                 config: Optional[Dict] = None):
        """
        Initialize the LLM worker.
        
        Args:
            memory_store: Shared memory store for signals
            db_manager: Database manager for async writes
            csv_exporter: CSV exporter instance
            config: Configuration dictionary
        """
        self.memory_store = memory_store
        self.db_manager = db_manager
        self.csv_exporter = csv_exporter or LLMCSVExporter()
        self.config = config or {}
        
        # Initialize components
        # Patient Labels formatter is a function, not a class
        self.llm_processor = None  # Will be initialized in setup
        self.response_parser = LLMResponseParser()
        
        # Worker state
        self.is_running = False
        self.processed_count = 0
        self.failed_count = 0
        self.last_cleanup_time = datetime.now()
        
        # Configuration
        self.batch_size = self.config.get('batch_size', 1)
        self.poll_interval = self.config.get('poll_interval', 1.0)
        self.cleanup_interval_minutes = self.config.get('cleanup_interval_minutes', 30)
        self.max_retries = self.config.get('max_retries', 3)
        
        logger.info("LLMWorker initialized")
    
    async def setup(self) -> None:
        """
        Async setup for components that require it.
        """
        # Initialize LLM processor with Google ADK pattern (matching reference)
        self.llm_processor = SimpleLLMProcessor(
            api_key=self.config.get('api_key'),
            model=self.config.get('model', 'openrouter/google/gemini-2.5-flash-preview-05-20')
        )
        await self.llm_processor.__aenter__()
        
        logger.info("LLMWorker setup complete")
    
    async def cleanup(self) -> None:
        """
        Cleanup resources.
        """
        if self.llm_processor:
            await self.llm_processor.__aexit__(None, None, None)
    
    async def run_continuously(self) -> None:
        """
        Main processing loop - continuously process signals from memory store.
        """
        logger.info("Starting LLM worker continuous processing")
        self.is_running = True
        
        try:
            await self.setup()
            
            while self.is_running:
                try:
                    # Check for memory cleanup
                    if datetime.now() - self.last_cleanup_time > timedelta(minutes=self.cleanup_interval_minutes):
                        await self._perform_cleanup()
                    
                    # Get next pending signal
                    signal_id = self.memory_store.pop_next_pending()
                    
                    if signal_id:
                        # Mark as processing
                        self.memory_store.mark_processing(signal_id)
                        
                        # Process the signal
                        await self.process_single_signal(signal_id)
                        
                        # Small delay between signals
                        await asyncio.sleep(0.1)
                    else:
                        # No signals, wait before polling again
                        await asyncio.sleep(self.poll_interval)
                    
                except Exception as e:
                    logger.error(f"Error in main processing loop: {str(e)}")
                    logger.debug(traceback.format_exc())
                    await asyncio.sleep(5)  # Wait before retrying
            
        finally:
            await self.cleanup()
            logger.info("LLM worker stopped")
    
    async def process_single_signal(self, signal_id: str) -> bool:
        """
        Process a single signal through LLM enhancement.
        
        Args:
            signal_id: ID of signal to process
            
        Returns:
            True if successful, False otherwise
        """
        start_time = datetime.now()
        
        try:
            # Get signal data from memory
            signal_data = self.memory_store.get_signal(signal_id)
            if not signal_data:
                logger.error(f"Signal {signal_id} not found in memory store")
                return False

            logger.info(f"Processing signal {signal_id}")
            
            # Print signal details to console for user to see
            signal_info = signal_data.get('signal_data', {})
            print(f"\n🔍 PROCESSING SIGNAL: {signal_id}")
            print(f"   Type: {signal_info.get('type', 'UNKNOWN').upper()}")
            print(f"   Entry: ${signal_info.get('entry_price', 0):.2f}")
            print(f"   Stop: ${signal_info.get('stop_loss', 0):.2f}")
            print(f"   Target: ${signal_info.get('target_price', 0):.2f}")
            print(f"   R/R: {signal_info.get('risk_reward_ratio', 0):.2f}")
            print(f"   Reason: {signal_info.get('reason', 'N/A')}")
            
            # Format signal for LLM
            formatted_text = self._format_signal(signal_data)
            
            # Extract signal timestamp from signal data
            signal_timestamp = signal_data.get('timestamp')
            if signal_timestamp and isinstance(signal_timestamp, str):
                signal_timestamp = datetime.fromisoformat(signal_timestamp.replace('Z', '+00:00'))
            elif not signal_timestamp:
                signal_timestamp = datetime.now()
            
            # Process through LLM
            llm_response = await self.llm_processor.process_signal(formatted_text, signal_id, signal_timestamp)
            
            if not llm_response.get('success'):
                raise Exception(f"LLM processing failed: {llm_response.get('error')}")
            
            # Parse LLM response
            parsed_result = self.response_parser.extract_json_data(llm_response.get('llm_response'))
            
            # Add metadata to parsed result
            parsed_result['llm_model_used'] = llm_response.get('llm_model_used')
            parsed_result['processing_time_ms'] = llm_response.get('processing_time_ms')
            parsed_result['session_id'] = llm_response.get('session_id')
            
            # Mark signal as complete in memory
            self.memory_store.complete_signal(signal_id, parsed_result)
            
            # Export to CSV immediately
            enhanced_signal = self.memory_store.get_signal(signal_id)
            self.csv_exporter.append_to_continuous_csv(enhanced_signal)
            
            # Async database update (non-blocking)
            if self.db_manager:
                asyncio.create_task(self._update_database(signal_id, parsed_result))
            
            self.processed_count += 1
            
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Successfully processed signal {signal_id} in {processing_time:.2f}s. "
                       f"Rating: {parsed_result.get('rating')}, Choppiness: {parsed_result.get('choppiness')}")
            
            # Print results to console
            print(f"✅ SIGNAL COMPLETED: {signal_id}")
            print(f"   💯 Rating: {parsed_result.get('rating')}/3")
            print(f"   🌊 Choppiness: {parsed_result.get('choppiness')}")
            print(f"   📝 Analysis: {parsed_result.get('analysis', 'N/A')[:100]}...")
            print(f"   ⏱️  Time: {processing_time:.2f}s")
            print(f"   🤖 Model: {parsed_result.get('llm_model_used', 'unknown')}")
            print()
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to process signal {signal_id}: {str(e)}"
            logger.error(error_msg)
            logger.debug(traceback.format_exc())
            
            # Mark as failed in memory
            self.memory_store.fail_signal(signal_id, error_msg)
            self.failed_count += 1
            
            # Check if we should retry
            signal_data = self.memory_store.get_signal(signal_id)
            retry_count = signal_data.get('retry_count', 0)
            
            if retry_count < self.max_retries:
                # Reset to pending for retry
                logger.info(f"Resetting signal {signal_id} for retry (attempt {retry_count + 1}/{self.max_retries})")
                self.memory_store.reset_failed_signals()
            
            return False
    
    def _format_signal(self, signal_data: Dict) -> str:
        """
        Format signal data for LLM processing using the reference Patient Labels format.
        
        Args:
            signal_data: Signal data from memory store
            
        Returns:
            Formatted text for LLM matching reference code format
        """
        # Extract components
        signal_info = signal_data.get('signal_data', {})
        market_snapshot = signal_data.get('market_snapshot', {})
        
        # Create proper market data with historical context
        market_data = self._create_market_dataframe(market_snapshot)
        
        # Create Patient Labels data with indicator values
        patient_labels_data = self._create_patient_labels_dataframe(market_data, signal_info)
        
        # Ensure signal has valid bar_index pointing to the last bar (signal bar)
        signal_info['bar_index'] = len(market_data) - 1
        
        # Add debug logging to understand the data structure
        logger.debug(f"Signal info keys: {signal_info.keys()}")
        logger.debug(f"Signal info bar_index: {signal_info.get('bar_index', 'NOT_FOUND')}")
        logger.debug(f"Market data shape: {market_data.shape}")
        logger.debug(f"Patient Labels data shape: {patient_labels_data.shape}")
        
        try:
            # Use the reference Patient Labels signal formatter
            # Default behavior: ALL bars from market open (09:30) to signal (like reference)
            formatted_text = format_signal_data(
                market_data,
                patient_labels_data,  # Now use proper Patient Labels data
                signal_info,
                lookback_bars=None  # None = all bars from market open (reference default)
            )
        except Exception as e:
            logger.error(f"Signal formatting error: {str(e)}")
            logger.error(f"Signal info: {signal_info}")
            logger.error(f"Market data columns: {market_data.columns.tolist()}")
            logger.error(f"Market data head: {market_data.head()}")
            logger.error(f"Patient Labels columns: {patient_labels_data.columns.tolist()}")
            
            # Create a fallback formatted text that's more informative
            formatted_text = f"""
=== SIGNAL ANALYSIS REQUEST ===

Signal Type: {signal_info.get('type', 'UNKNOWN').upper()}
Entry Price: ${signal_info.get('entry_price', 0):.2f}
Stop Loss: ${signal_info.get('stop_loss', 0):.2f}
Target Price: ${signal_info.get('target_price', 0):.2f}
Risk/Reward Ratio: {signal_info.get('risk_reward_ratio', 0):.2f}
Signal Time: {signal_info.get('datetime', 'Unknown')}

Current Market Context:
- Current Price: ${market_data.iloc[-1]['close'] if not market_data.empty else 0:.2f}
- Recent High: ${market_data['high'].max() if not market_data.empty else 0:.2f}
- Recent Low: ${market_data['low'].min() if not market_data.empty else 0:.2f}
- Average Volume: {market_data['volume'].mean() if not market_data.empty and 'volume' in market_data.columns else 0:.0f}

Analysis Request: 
Please analyze this trading signal. Consider the signal quality, market conditions, and provide:
1. A rating from 0-3 (0=poor, 3=excellent)
2. Whether market conditions appear choppy (yes/no)
3. Overall analysis of the trade setup

Note: Limited market data available due to formatting constraints.
"""
        
        return formatted_text
    
    def _create_market_dataframe(self, market_snapshot: Dict) -> "pd.DataFrame":
        """
        Create a proper market DataFrame from market open (9:30 AM) to signal time.
        
        Args:
            market_snapshot: Market snapshot dictionary
            
        Returns:
            DataFrame with market data from 9:30 AM to signal time
        """
        import pandas as pd
        from datetime import time
        
        # TODO: In a full implementation, this should pull actual historical market data
        # from your data source to provide proper context for the LLM
        
        current_price = market_snapshot.get('close', 480.0)  # Default to reasonable price
        current_volume = market_snapshot.get('volume', 100000)
        timestamp = market_snapshot.get('timestamp', datetime.now())
        
        # Parse signal timestamp
        signal_time = pd.to_datetime(timestamp)
        signal_date = signal_time.date()
        
        # Market open at 9:30 AM on signal date
        market_open_time = time(9, 30)
        market_open_datetime = pd.Timestamp.combine(signal_date, market_open_time)
        
        # Create 5-minute bars from market open to signal time
        data = []
        current_time = market_open_datetime
        
        # Calculate how many 5-minute bars from 9:30 AM to signal time
        time_diff = signal_time - market_open_datetime
        total_minutes = int(time_diff.total_seconds() / 60)
        num_bars = max(1, total_minutes // 5)  # At least 1 bar
        
        # If signal is before market open, just create a few bars around signal time
        if num_bars <= 0:
            num_bars = 10
            current_time = signal_time - pd.Timedelta(minutes=45)  # Start 45 mins before signal
        
        # Generate bars from market open to signal time (could be 6+ hours = 70+ bars)
        for i in range(num_bars + 1):  # +1 to include signal bar
            bar_time = current_time + pd.Timedelta(minutes=i*5)
            
            # Stop at signal time
            if bar_time > signal_time:
                bar_time = signal_time
            
            # Simulate realistic price movement (replace with real data)
            # Create some trend and noise
            trend_component = (i / num_bars) * 2.0  # Gradual trend
            cycle_component = 1.5 * np.sin(i * 0.3)  # Some cyclical movement
            noise = (hash(str(bar_time)) % 200 - 100) / 100.0  # Random-ish noise
            
            bar_price = current_price + trend_component + cycle_component + noise
            bar_volume = current_volume + (hash(str(bar_time)) % 50000)
            
            data.append({
                'datetime': bar_time,
                'open': round(bar_price - 0.2, 2),
                'high': round(bar_price + 0.5, 2),
                'low': round(bar_price - 0.3, 2),
                'close': round(bar_price, 2),
                'volume': bar_volume
            })
            
            # Break if we've reached signal time
            if bar_time >= signal_time:
                break
        
        df = pd.DataFrame(data)
        
        # Ensure datetime is properly set
        if 'datetime' not in df.columns:
            df['datetime'] = pd.to_datetime(datetime.now())
        
        return df

    def _create_patient_labels_dataframe(self, market_data: "pd.DataFrame", signal_info: Dict) -> "pd.DataFrame":
        """
        Create Patient Labels DataFrame with simulated indicator values.
        
        In production, this should run actual Patient Labels indicator on market data.
        
        Args:
            market_data: Market DataFrame
            signal_info: Signal information
            
        Returns:
            DataFrame with Patient Labels indicator data
        """
        import pandas as pd
        
        # Create Patient Labels data with same structure as market data
        pl_data = market_data.copy()
        
        # Add Patient Labels indicator columns (simulated values)
        # In production, run actual PatientLabelsIndicator here
        
        signal_type = signal_info.get('type', 'buy').lower()
        
        for i, row in pl_data.iterrows():
            # Simulate trend_state based on signal type
            if signal_type == 'buy':
                trend_state = 1 if i >= len(pl_data) - 5 else 0  # Uptrend near signal
            else:
                trend_state = -1 if i >= len(pl_data) - 5 else 0  # Downtrend near signal
            
            pl_data.loc[i, 'trend_state'] = trend_state
            pl_data.loc[i, 'in_uptrend'] = trend_state == 1
            pl_data.loc[i, 'in_downtrend'] = trend_state == -1
            
            # Simulate swing cycles
            pl_data.loc[i, 'up_swing_cycle'] = max(0, i - 10) if trend_state == 1 else 0
            pl_data.loc[i, 'down_swing_cycle'] = max(0, i - 10) if trend_state == -1 else 0
            
            # Simulate patient candles (occasionally present)
            if i == len(pl_data) - 1 and signal_type == 'buy':  # Signal bar
                pl_data.loc[i, 'up_patient_high'] = row['high'] + 0.1
            elif i == len(pl_data) - 1 and signal_type == 'sell':  # Signal bar
                pl_data.loc[i, 'down_patient_low'] = row['low'] - 0.1
            else:
                pl_data.loc[i, 'up_patient_high'] = pd.NA
                pl_data.loc[i, 'down_patient_low'] = pd.NA
        
        return pl_data
    
    async def _update_database(self, signal_id: str, llm_result: Dict) -> None:
        """
        Update database with LLM results (async, non-blocking).
        
        Args:
            signal_id: Signal ID
            llm_result: LLM processing results
        """
        try:
            if not self.db_manager:
                return
            
            # Get task ID from database
            tasks = await self.db_manager.get_pending_llm_tasks()
            task_id = None
            
            for task in tasks:
                if task['signal_id'] == signal_id:
                    task_id = task['task_id']
                    break
            
            if not task_id:
                # Create task if it doesn't exist - need to provide session_id
                # Use the session_id from LLM result if available, otherwise generate one
                session_id = llm_result.get('session_id', f"llm_session_{signal_id}")
                task_id = await self.db_manager.create_llm_enhancement_task(signal_id, session_id)
            
            # Update task status
            await self.db_manager.update_task_status(task_id, 'completed')
            
            # Save LLM results
            await self.db_manager.save_llm_analysis_result(task_id, signal_id, llm_result)
            
            logger.debug(f"Updated database for signal {signal_id}")
            
        except Exception as e:
            logger.error(f"Failed to update database for signal {signal_id}: {str(e)}")
    
    async def _perform_cleanup(self) -> None:
        """
        Perform periodic cleanup tasks.
        """
        try:
            logger.info("Performing periodic cleanup")
            
            # Clean old signals from memory
            cleaned = self.memory_store.cleanup_old_signals()
            
            # Export batch statistics
            stats = self.memory_store.get_stats()
            stats['processed_total'] = self.processed_count
            stats['failed_total'] = self.failed_count
            stats['cleanup_performed_at'] = datetime.now().isoformat()
            
            self.csv_exporter.export_summary_report(stats)
            
            self.last_cleanup_time = datetime.now()
            
            logger.info(f"Cleanup complete. Removed {cleaned} old signals. "
                       f"Memory stats: {stats}")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
    
    def stop(self) -> None:
        """
        Signal the worker to stop processing.
        """
        logger.info("Stopping LLM worker")
        self.is_running = False
    
    def get_status(self) -> Dict:
        """
        Get current worker status.
        
        Returns:
            Dictionary with worker status information
        """
        memory_stats = self.memory_store.get_stats()
        llm_status = self.llm_processor.get_rate_limit_status() if self.llm_processor else {}
        
        return {
            'is_running': self.is_running,
            'processed_count': self.processed_count,
            'failed_count': self.failed_count,
            'memory_stats': memory_stats,
            'llm_rate_limits': llm_status,
            'last_cleanup': self.last_cleanup_time.isoformat() if self.last_cleanup_time else None
        }