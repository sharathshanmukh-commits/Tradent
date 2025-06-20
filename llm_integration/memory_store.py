"""
In-Memory Signal Store for LLM Processing

This module provides a thread-safe in-memory storage system for trading signals
awaiting LLM enhancement. It supports O(1) operations for adding, retrieving,
and updating signals.
"""

import threading
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from collections import OrderedDict
import logging

logger = logging.getLogger(__name__)


class SignalMemoryStore:
    """
    Thread-safe in-memory storage for trading signals and LLM processing.
    
    This class manages the lifecycle of signals through various states:
    - Pending: Signals waiting for LLM processing
    - Processing: Signals currently being processed by LLM
    - Completed: Signals with LLM enhancement complete
    """
    
    def __init__(self, max_signals: int = 1000, retention_minutes: int = 60):
        """
        Initialize the signal memory store.
        
        Args:
            max_signals: Maximum number of signals to keep in memory
            retention_minutes: How long to keep completed signals in memory
        """
        # Main storage - all signal data keyed by signal_id
        self.signals_store: Dict[str, Dict] = {}
        
        # State management
        self.pending_queue: List[str] = []              # FIFO queue of signal_ids
        self.processing_set: Set[str] = set()           # Currently processing
        self.completed_store: OrderedDict[str, Dict] = OrderedDict()  # Completed signals
        
        # Configuration
        self.max_signals = max_signals
        self.retention_minutes = retention_minutes
        
        # Thread safety
        self._lock = threading.RLock()  # Re-entrant lock for nested calls
        
        # Statistics
        self.stats = {
            'total_added': 0,
            'total_processed': 0,
            'total_failed': 0,
            'total_cleaned': 0
        }
        
        logger.info(f"SignalMemoryStore initialized: max_signals={max_signals}, retention_minutes={retention_minutes}")
    
    def add_signal(self, signal_data: Dict) -> None:
        """
        Add a new signal to memory store and pending queue.
        
        Args:
            signal_data: Complete signal data including signal_id
            
        Time Complexity: O(1)
        """
        with self._lock:
            signal_id = signal_data.get('signal_id')
            if not signal_id:
                raise ValueError("signal_data must contain 'signal_id'")
            
            # Check if signal already exists
            if signal_id in self.signals_store:
                logger.warning(f"Signal {signal_id} already exists in store, skipping")
                return
            
            # Add timestamp if not present
            if 'created_at' not in signal_data:
                signal_data['created_at'] = datetime.now()
            
            # Store signal data
            self.signals_store[signal_id] = signal_data
            self.pending_queue.append(signal_id)
            
            self.stats['total_added'] += 1
            
            # Check if we need to cleanup
            if len(self.signals_store) > self.max_signals:
                self._cleanup_oldest()
            
            logger.debug(f"Added signal {signal_id} to memory store. Pending: {len(self.pending_queue)}")
    
    def pop_next_pending(self) -> Optional[str]:
        """
        Get the next signal ID for LLM processing (FIFO).
        
        Returns:
            signal_id if available, None otherwise
            
        Time Complexity: O(1)
        """
        with self._lock:
            if not self.pending_queue:
                return None
            
            signal_id = self.pending_queue.pop(0)
            return signal_id
    
    def mark_processing(self, signal_id: str) -> None:
        """
        Mark a signal as currently being processed.
        
        Args:
            signal_id: The signal to mark as processing
            
        Time Complexity: O(1)
        """
        with self._lock:
            if signal_id not in self.signals_store:
                raise ValueError(f"Signal {signal_id} not found in store")
            
            self.processing_set.add(signal_id)
            self.signals_store[signal_id]['status'] = 'processing'
            self.signals_store[signal_id]['started_at'] = datetime.now()
            
            logger.debug(f"Marked signal {signal_id} as processing")
    
    def complete_signal(self, signal_id: str, llm_result: Dict) -> None:
        """
        Mark a signal as completed with LLM results.
        
        Args:
            signal_id: The signal to mark as complete
            llm_result: LLM enhancement results
            
        Time Complexity: O(1)
        """
        with self._lock:
            if signal_id not in self.signals_store:
                raise ValueError(f"Signal {signal_id} not found in store")
            
            # Remove from processing set
            self.processing_set.discard(signal_id)
            
            # Update signal data
            signal_data = self.signals_store[signal_id]
            signal_data['status'] = 'completed'
            signal_data['completed_at'] = datetime.now()
            signal_data['llm_result'] = llm_result
            
            # Calculate processing time
            if 'started_at' in signal_data:
                processing_time = (signal_data['completed_at'] - signal_data['started_at']).total_seconds()
                signal_data['processing_time_seconds'] = processing_time
            
            # Add to completed store
            self.completed_store[signal_id] = signal_data
            
            self.stats['total_processed'] += 1
            
            logger.info(f"Completed signal {signal_id} with LLM enhancement")
    
    def fail_signal(self, signal_id: str, error_message: str) -> None:
        """
        Mark a signal as failed with error message.
        
        Args:
            signal_id: The signal that failed
            error_message: Error description
            
        Time Complexity: O(1)
        """
        with self._lock:
            if signal_id not in self.signals_store:
                raise ValueError(f"Signal {signal_id} not found in store")
            
            # Remove from processing set
            self.processing_set.discard(signal_id)
            
            # Update signal data
            signal_data = self.signals_store[signal_id]
            signal_data['status'] = 'failed'
            signal_data['failed_at'] = datetime.now()
            signal_data['error_message'] = error_message
            
            # Add to completed store (even failed signals)
            self.completed_store[signal_id] = signal_data
            
            self.stats['total_failed'] += 1
            
            logger.error(f"Failed signal {signal_id}: {error_message}")
    
    def get_signal(self, signal_id: str) -> Optional[Dict]:
        """
        Get complete signal data by ID.
        
        Args:
            signal_id: The signal to retrieve
            
        Returns:
            Signal data dict or None if not found
            
        Time Complexity: O(1)
        """
        with self._lock:
            return self.signals_store.get(signal_id)
    
    def get_completed_batch(self, limit: int = 10) -> List[Dict]:
        """
        Get a batch of completed signals for export.
        
        Args:
            limit: Maximum number of signals to return
            
        Returns:
            List of completed signal data
            
        Time Complexity: O(limit)
        """
        with self._lock:
            completed_list = []
            for signal_id, signal_data in list(self.completed_store.items())[:limit]:
                if signal_data.get('status') == 'completed':
                    completed_list.append(signal_data.copy())
            
            return completed_list
    
    def cleanup_old_signals(self, max_age_minutes: Optional[int] = None) -> int:
        """
        Remove old completed signals from memory.
        
        Args:
            max_age_minutes: Override default retention time
            
        Returns:
            Number of signals cleaned up
            
        Time Complexity: O(n) where n is number of completed signals
        """
        with self._lock:
            if max_age_minutes is None:
                max_age_minutes = self.retention_minutes
            
            cutoff_time = datetime.now() - timedelta(minutes=max_age_minutes)
            cleaned_count = 0
            
            # Find signals to remove
            signals_to_remove = []
            for signal_id, signal_data in self.completed_store.items():
                completed_at = signal_data.get('completed_at') or signal_data.get('failed_at')
                if completed_at and completed_at < cutoff_time:
                    signals_to_remove.append(signal_id)
            
            # Remove old signals
            for signal_id in signals_to_remove:
                del self.completed_store[signal_id]
                del self.signals_store[signal_id]
                cleaned_count += 1
            
            self.stats['total_cleaned'] += cleaned_count
            
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} old signals from memory")
            
            return cleaned_count
    
    def _cleanup_oldest(self) -> None:
        """
        Remove oldest completed signals when max_signals is exceeded.
        
        Time Complexity: O(1) average case
        """
        # Remove oldest completed signal
        if self.completed_store:
            oldest_id, _ = self.completed_store.popitem(last=False)
            del self.signals_store[oldest_id]
            self.stats['total_cleaned'] += 1
            logger.debug(f"Removed oldest signal {oldest_id} due to memory limit")
    
    def get_stats(self) -> Dict:
        """
        Get current statistics about the memory store.
        
        Returns:
            Dictionary with current stats
            
        Time Complexity: O(1)
        """
        with self._lock:
            return {
                'total_signals': len(self.signals_store),
                'pending': len(self.pending_queue),
                'processing': len(self.processing_set),
                'completed': len(self.completed_store),
                'total_added': self.stats['total_added'],
                'total_processed': self.stats['total_processed'],
                'total_failed': self.stats['total_failed'],
                'total_cleaned': self.stats['total_cleaned']
            }
    
    def reset_failed_signals(self) -> int:
        """
        Reset failed signals back to pending for retry.
        
        Returns:
            Number of signals reset
            
        Time Complexity: O(n) where n is number of completed signals
        """
        with self._lock:
            reset_count = 0
            
            for signal_id, signal_data in list(self.completed_store.items()):
                if signal_data.get('status') == 'failed':
                    # Remove from completed
                    del self.completed_store[signal_id]
                    
                    # Reset status and add back to pending
                    signal_data['status'] = 'pending'
                    signal_data['retry_count'] = signal_data.get('retry_count', 0) + 1
                    self.pending_queue.append(signal_id)
                    
                    reset_count += 1
            
            if reset_count > 0:
                logger.info(f"Reset {reset_count} failed signals to pending")
            
            return reset_count