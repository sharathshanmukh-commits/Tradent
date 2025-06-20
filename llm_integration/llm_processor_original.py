"""
LLM Processor with Rate Limiting

This module handles LLM API calls with proper rate limiting, retry logic,
and error handling. It's designed to work with OpenRouter API.
"""

import time
import asyncio
import logging
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import os
import aiohttp
import backoff
from collections import deque
from choppiness_agent import get_agent

logger = logging.getLogger(__name__)

# Rate limiting configuration (from TODO requirements)
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 2
MAX_RETRY_DELAY = 60
RATE_LIMIT_WINDOW = 60  # seconds
MAX_REQUESTS_PER_MINUTE = 14
ESTIMATED_TOKENS_PER_REQUEST = 50000
MAX_TOKENS_PER_MINUTE = 950000

# Global rate limiting state
request_timestamps = deque()
token_usage_this_minute = 0
minute_start_time = time.time()


class LLMProcessor:
    """
    Handles LLM API calls with rate limiting and retry logic.
    """
    
    def __init__(self, api_key: Optional[str] = None, model: str = "openrouter/google/gemini-2.5-flash-preview-05-20"):
        """
        Initialize the LLM processor.
        
        Args:
            api_key: OpenRouter API key (or from environment)
            model: Model identifier for OpenRouter
        """
        self.api_key = api_key or os.getenv('OPENROUTER_API_KEY')
        if not self.api_key:
            raise ValueError("OpenRouter API key not provided and OPENROUTER_API_KEY env var not set")
        
        self.model = model
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        
        # Session for connection pooling
        self.session = None
        
        # Local rate limiting tracking
        self.request_history = deque()
        self.token_usage_history = deque()
        
        # Initialize choppiness agent
        self.agent = get_agent()
        
        logger.info(f"LLMProcessor initialized with model: {model}")
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    def check_and_apply_rate_limits(self, signal_index: int) -> Tuple[bool, float]:
        """
        Check if we can make a request based on rate limits.
        
        Args:
            signal_index: Index of the signal being processed (for logging)
            
        Returns:
            Tuple of (can_proceed, wait_time_seconds)
        """
        global request_timestamps, token_usage_this_minute, minute_start_time
        
        current_time = time.time()
        
        # Clean up old timestamps (older than 1 minute)
        while request_timestamps and request_timestamps[0] < current_time - RATE_LIMIT_WINDOW:
            request_timestamps.popleft()
        
        # Reset token counter if minute has passed
        if current_time - minute_start_time > RATE_LIMIT_WINDOW:
            token_usage_this_minute = 0
            minute_start_time = current_time
        
        # Check request rate limit
        if len(request_timestamps) >= MAX_REQUESTS_PER_MINUTE:
            wait_time = RATE_LIMIT_WINDOW - (current_time - request_timestamps[0])
            logger.warning(f"Signal {signal_index}: Request rate limit reached. Waiting {wait_time:.1f}s")
            return False, wait_time
        
        # Check token rate limit
        if token_usage_this_minute + ESTIMATED_TOKENS_PER_REQUEST > MAX_TOKENS_PER_MINUTE:
            wait_time = RATE_LIMIT_WINDOW - (current_time - minute_start_time)
            logger.warning(f"Signal {signal_index}: Token rate limit reached. Waiting {wait_time:.1f}s")
            return False, wait_time
        
        return True, 0
    
    async def rate_limited_api_call(self, formatted_signal: str, signal_index: int = 0) -> Dict:
        """
        Make a rate-limited API call to the LLM.
        
        Args:
            formatted_signal: Formatted signal text for LLM
            signal_index: Index for logging purposes
            
        Returns:
            LLM response dictionary
        """
        global request_timestamps, token_usage_this_minute
        
        # Apply rate limiting
        can_proceed, wait_time = self.check_and_apply_rate_limits(signal_index)
        if not can_proceed:
            await asyncio.sleep(wait_time)
            # Recheck after waiting
            can_proceed, wait_time = self.check_and_apply_rate_limits(signal_index)
            if not can_proceed:
                raise Exception(f"Still rate limited after waiting {wait_time}s")
        
        # Record this request
        request_timestamps.append(time.time())
        token_usage_this_minute += ESTIMATED_TOKENS_PER_REQUEST
        
        # Make the API call with retry logic
        try:
            response = await self._make_api_call_with_retry(formatted_signal, signal_index)
            
            # Update token usage based on actual usage if available
            if 'usage' in response:
                actual_tokens = response['usage'].get('total_tokens', ESTIMATED_TOKENS_PER_REQUEST)
                token_usage_this_minute = token_usage_this_minute - ESTIMATED_TOKENS_PER_REQUEST + actual_tokens
            
            return response
            
        except Exception as e:
            logger.error(f"Signal {signal_index}: API call failed: {str(e)}")
            raise
    
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=MAX_RETRIES,
        max_time=300
    )
    async def _make_api_call_with_retry(self, formatted_signal: str, signal_index: int) -> Dict:
        """
        Make the actual API call with exponential backoff retry.
        
        Args:
            formatted_signal: Formatted signal text
            signal_index: Signal index for logging
            
        Returns:
            API response dictionary
        """
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/tradent-ai/tradent",
            "X-Title": "Tradent Trading System"
        }
        
        # Use agent to create messages
        messages = self.agent.create_analysis_request(formatted_signal)
        
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.3,  # Lower temperature for more consistent analysis
            "max_tokens": 1000,
            "response_format": {"type": "json_object"}  # Request JSON response
        }
        
        logger.debug(f"Signal {signal_index}: Making API call to {self.model}")
        logger.debug(f"Payload: {json.dumps(payload, indent=2)[:500]}...")  # Log first 500 chars
        
        async with self.session.post(
            self.base_url,
            headers=headers,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            
            if response.status == 429:  # Rate limit error
                retry_after = response.headers.get('Retry-After', '60')
                logger.warning(f"Signal {signal_index}: Rate limited by API, retry after {retry_after}s")
                raise aiohttp.ClientError(f"Rate limited, retry after {retry_after}s")
            
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"API error {response.status}: {error_text}")
            
            response.raise_for_status()
            
            result = await response.json()
            
            logger.info(f"Signal {signal_index}: Successfully received LLM response")
            
            return result
    
    async def process_signal(self, formatted_signal: str, signal_id: str) -> Dict:
        """
        Process a single signal through the LLM.
        
        Args:
            formatted_signal: Pre-formatted signal text
            signal_id: Signal identifier
            
        Returns:
            Dictionary with LLM results and metadata
        """
        start_time = time.time()
        
        try:
            # Make the API call
            response = await self.rate_limited_api_call(formatted_signal, signal_index=0)
            
            # Extract the content
            content = response['choices'][0]['message']['content']
            
            # Calculate processing time
            processing_time_ms = int((time.time() - start_time) * 1000)
            
            return {
                'success': True,
                'signal_id': signal_id,
                'llm_response': content,
                'llm_raw_response': response,
                'llm_model_used': self.model,
                'processing_time_ms': processing_time_ms,
                'processed_at': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"Failed to process signal {signal_id}: {str(e)}")
            
            return {
                'success': False,
                'signal_id': signal_id,
                'error': str(e),
                'llm_model_used': self.model,
                'processing_time_ms': int((time.time() - start_time) * 1000),
                'processed_at': datetime.now()
            }
    
    async def process_batch(self, signal_batch: List[Tuple[str, str]]) -> List[Dict]:
        """
        Process multiple signals in a batch with rate limiting.
        
        Args:
            signal_batch: List of (signal_id, formatted_signal) tuples
            
        Returns:
            List of result dictionaries
        """
        results = []
        
        for i, (signal_id, formatted_signal) in enumerate(signal_batch):
            try:
                result = await self.process_signal(formatted_signal, signal_id)
                results.append(result)
                
                # Add small delay between requests to be nice to the API
                if i < len(signal_batch) - 1:
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Batch processing error for signal {signal_id}: {str(e)}")
                results.append({
                    'success': False,
                    'signal_id': signal_id,
                    'error': str(e)
                })
        
        return results
    
    # Context manager support
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass
    
    def get_rate_limit_status(self) -> Dict:
        """
        Get current rate limiting status.
        
        Returns:
            Dictionary with rate limit information
        """
        global request_timestamps, token_usage_this_minute, minute_start_time
        
        current_time = time.time()
        
        # Clean old timestamps
        while request_timestamps and request_timestamps[0] < current_time - RATE_LIMIT_WINDOW:
            request_timestamps.popleft()
        
        # Reset token counter if needed
        if current_time - minute_start_time > RATE_LIMIT_WINDOW:
            token_usage_this_minute = 0
            minute_start_time = current_time
        
        return {
            'requests_this_minute': len(request_timestamps),
            'max_requests_per_minute': MAX_REQUESTS_PER_MINUTE,
            'tokens_this_minute': token_usage_this_minute,
            'max_tokens_per_minute': MAX_TOKENS_PER_MINUTE,
            'can_make_request': len(request_timestamps) < MAX_REQUESTS_PER_MINUTE and 
                               token_usage_this_minute + ESTIMATED_TOKENS_PER_REQUEST <= MAX_TOKENS_PER_MINUTE
        }