"""
LLM Background Worker

This module implements the background worker that continuously processes signals
from memory store through LLM enhancement and exports results.
"""

import asyncio
import logging
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
import traceback
import pandas as pd

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
        # Initialize LLM processor with simple Google ADK pattern (matching reference)
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
            
            # Format signal for LLM
            formatted_text = self._format_signal(signal_data)
            
            # Process through LLM
            llm_response = await self.llm_processor.process_signal(formatted_text, signal_id)
            
            if not llm_response.get('success'):
                raise Exception(f"LLM processing failed: {llm_response.get('error')}")
            
            # Parse LLM response
            parsed_result = self.response_parser.extract_json_data(llm_response.get('llm_response'))
            
            # Add metadata to parsed result
            parsed_result['llm_model_used'] = llm_response.get('llm_model_used')
            parsed_result['processing_time_ms'] = llm_response.get('processing_time_ms')
            
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
        Format signal data for LLM processing.
        
        Args:
            signal_data: Signal data from memory store
            
        Returns:
            Formatted text for LLM
        """
        # Extract components
        signal_info = signal_data.get('signal_data', {})
        market_snapshot = signal_data.get('market_snapshot', {})
        
        # TODO: Get market data from data source if available
        # For now, create minimal market data from snapshot
        market_data = self._create_market_dataframe(market_snapshot)
        
        # Format using Patient Labels signal formatter
        # Add debug logging to understand the data structure
        logger.debug(f"Signal info keys: {signal_info.keys()}")
        logger.debug(f"Signal info bar_index: {signal_info.get('bar_index', 'NOT_FOUND')}")
        logger.debug(f"Market data shape: {market_data.shape}")
        
        try:
            formatted_text = format_signal_data(
                market_data,
                market_data,  # Use market_data as placeholder for patient_labels_data
                signal_info
            )
        except Exception as e:
            logger.error(f"Signal formatting error: {str(e)}")
            logger.error(f"Signal info: {signal_info}")
            logger.error(f"Market data columns: {market_data.columns.tolist()}")
            logger.error(f"Market data head: {market_data.head()}")
            # Create a fallback formatted text
            formatted_text = f"""
Signal Type: {signal_info.get('type', 'UNKNOWN')}
Entry Price: ${signal_info.get('entry_price', 0):.2f}
Stop Loss: ${signal_info.get('stop_loss', 0):.2f}
Target Price: ${signal_info.get('target_price', 0):.2f}
Risk/Reward Ratio: {signal_info.get('risk_reward_ratio', 0):.2f}

Market Data: Limited data available due to formatting error.
Current Price: ${market_data.iloc[-1]['close'] if not market_data.empty else 0:.2f}
Recent Price Action: Unable to format detailed price action.
Average Volume: {market_data['volume'].mean() if not market_data.empty and 'volume' in market_data.columns else 0:.0f}

Analysis Context: The signal is requesting analysis but encountered formatting issues.
Please provide analysis based on the basic signal information provided.
"""
        
        return formatted_text
    
    def _create_market_dataframe(self, market_snapshot: Dict) -> "pd.DataFrame":
        """
        Create a minimal market DataFrame from snapshot data.
        
        Args:
            market_snapshot: Market snapshot dictionary
            
        Returns:
            DataFrame with market data
        """
        import pandas as pd
        
        # Create minimal DataFrame from snapshot
        # In production, this would pull from actual market data
        data = []
        
        # Add current bar
        if market_snapshot:
            data.append({
                'timestamp': market_snapshot.get('timestamp', datetime.now()),
                'open': market_snapshot.get('open', 0),
                'high': market_snapshot.get('high', 0),
                'low': market_snapshot.get('low', 0),
                'close': market_snapshot.get('close', 0),
                'volume': market_snapshot.get('volume', 0)
            })
        
        return pd.DataFrame(data)
    
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
                # Create task if it doesn't exist
                task_id = await self.db_manager.create_llm_enhancement_task(signal_id)
            
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