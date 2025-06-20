"""
Simple LLM Processor using Google ADK Runner (matching reference implementation)

This module handles LLM API calls using the exact same pattern as the reference code
with simple rate limiting and direct Google ADK Runner usage.
"""

import time
import os
import re
import json
import uuid
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from collections import deque

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from google.genai.errors import ServerError, ClientError
from choppiness_agent.adk_agent import root_agent

logger = logging.getLogger(__name__)

# Rate limiting configuration (matching reference)
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 2  # seconds
MAX_RETRY_DELAY = 60     # seconds
RATE_LIMIT_WINDOW = 60   # 1 minute window
MAX_REQUESTS_PER_MINUTE = 14  # Stay just under the 15 limit
ESTIMATED_TOKENS_PER_REQUEST = 50000
MAX_TOKENS_PER_MINUTE = 950000

# Global rate limiting state (matching reference)
request_timestamps = []
token_usage_this_minute = 0
minute_start_time = time.time()


class SimpleLLMProcessor:
    """
    Simple LLM processor using Google ADK Runner (matching reference implementation).
    """
    
    def __init__(self, api_key: Optional[str] = None, model: str = "openrouter/google/gemini-2.5-flash-preview-05-20"):
        """
        Initialize the simple LLM processor.
        
        Args:
            api_key: OpenRouter API key (or from environment)
            model: Model identifier (for compatibility, actual model is in agent)
        """
        self.api_key = api_key or os.getenv('OPENROUTER_API_KEY')
        if not self.api_key:
            raise ValueError("OpenRouter API key not provided and OPENROUTER_API_KEY env var not set")
        
        # Set the API key in environment for LiteLLM
        os.environ['OPENROUTER_API_KEY'] = self.api_key
        
        self.model = model
        
        # Initialize session service and runner (matching reference)
        self.session_service = InMemorySessionService()
        self.base_session_id = str(uuid.uuid4())
        self.app_name = "signal_analysis_app"
        self.user_id = "trader_1"
        
        self.runner = Runner(
            agent=root_agent,  # Direct import from agent.py like reference
            session_service=self.session_service,
            app_name=self.app_name
        )
        
        logger.info(f"SimpleLLMProcessor initialized with Google ADK Agent using model: {model}")
    
    def check_and_apply_rate_limits(self, signal_index: int) -> None:
        """Check and apply both request rate and token rate limits (matching reference)"""
        global request_timestamps, token_usage_this_minute, minute_start_time
        
        # Clean up old timestamps outside the window
        current_time = time.time()
        request_timestamps = [ts for ts in request_timestamps if current_time - ts < RATE_LIMIT_WINDOW]
        
        # Check if we're over the request rate limit
        if len(request_timestamps) >= MAX_REQUESTS_PER_MINUTE:
            # Calculate time to wait until the oldest request falls out of the window
            oldest_timestamp = min(request_timestamps)
            sleep_time = oldest_timestamp + RATE_LIMIT_WINDOW - current_time
            if sleep_time > 0:
                print(f"Request rate limit reached. Waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time + 0.5)  # Add a small buffer
                # Refresh timestamps after waiting
                current_time = time.time()
                request_timestamps = [ts for ts in request_timestamps if current_time - ts < RATE_LIMIT_WINDOW]
        
        # Check token rate limits
        if current_time - minute_start_time >= 60:
            # Reset token usage for the new minute
            token_usage_this_minute = 0
            minute_start_time = current_time
        
        # If we would exceed token limit, wait until the next minute
        if token_usage_this_minute + ESTIMATED_TOKENS_PER_REQUEST > MAX_TOKENS_PER_MINUTE:
            wait_time = 60 - (current_time - minute_start_time)
            print(f"Token limit approaching for signal {signal_index}. Waiting {wait_time:.1f} seconds...")
            time.sleep(wait_time + 1)  # Wait until next minute plus a small buffer
            token_usage_this_minute = 0
            minute_start_time = time.time()
        
        # Update the token usage and add request timestamp
        token_usage_this_minute += ESTIMATED_TOKENS_PER_REQUEST
        request_timestamps.append(time.time())

    async def rate_limited_api_call_async(self, formatted_signal: str, signal_index: int) -> str:
        """Make API call with rate limiting and backoff (properly async version)"""
        # Check rate limits before making the call
        self.check_and_apply_rate_limits(signal_index)
        
        # Create session ID for this signal
        session_id = f"{self.base_session_id}_signal_{signal_index}"
        
        # ✅ CREATE SESSION BEFORE USING IT (properly async)
        initial_state = {
            "signals_analyzed": 0,
            "last_signal_type": None,
            "signal_index": signal_index
        }
        
        try:
            # Create the session using await (since it's async)
            await self.session_service.create_session(
                session_id=session_id,
                user_id=self.user_id,
                app_name=self.app_name,
                state=initial_state
            )
            print(f"[LLM] ✅ Created async session: {session_id}")
        except Exception as e:
            print(f"[LLM] Session creation error (may already exist): {str(e)}")
        
        # Retry logic with exponential backoff
        retries = 0
        analysis_result = ""
        
        while retries <= MAX_RETRIES:
            try:
                # Create the message
                new_message = types.Content(role="user", parts=[types.Part(text=formatted_signal)])
                
                # Make the API call using runner (matching reference exactly)
                print(f"[LLM] Making API call for signal {signal_index}")
                print(f"[LLM] Using model configuration: {self.model}")
                print(f"[LLM] Session ID: {session_id}")
                print(f"[LLM] User ID: {self.user_id}")
                print(f"[LLM] Signal text length: {len(formatted_signal)} chars")
                
                # Use the async runner
                async for event in self.runner.run_async(
                    user_id=self.user_id,
                    session_id=session_id,
                    new_message=new_message,
                ):
                    print(f"[LLM] Event type: {type(event)}")
                    if hasattr(event, 'content'):
                        print(f"[LLM] Event has content: {hasattr(event.content, 'parts') if event.content else False}")
                    
                    if event.is_final_response():
                        print(f"[LLM] Final response received")
                        if event.content and event.content.parts:
                            analysis_result = event.content.parts[0].text
                            print(f"[LLM] Response length: {len(analysis_result)} chars")
                            print(f"[LLM] First 200 chars: {analysis_result[:200]}...")
                            
                            # Save detailed logs to test_run folder
                            import json
                            import os
                            log_data = {
                                'signal_index': signal_index,
                                'session_id': session_id,
                                'configured_model': self.model,
                                'input_text': formatted_signal,
                                'response_text': analysis_result,
                                'event_type': str(type(event)),
                                'event_attributes': [attr for attr in dir(event) if not attr.startswith('_')]
                            }
                            
                            log_file = f"test_run/llm_call_{signal_index}.json"
                            os.makedirs("test_run", exist_ok=True)
                            with open(log_file, 'w') as f:
                                json.dump(log_data, f, indent=2, default=str)
                            print(f"[LLM] Saved detailed log to {log_file}")
                
                # If we get here without an exception, we're done
                return analysis_result
                
            except ClientError as e:
                if e.status_code == 429:  # Rate limit error
                    if retries < MAX_RETRIES:
                        # Calculate retry delay with exponential backoff
                        retry_delay = min(INITIAL_RETRY_DELAY * (2 ** retries), MAX_RETRY_DELAY)
                        
                        print(f"Rate limit exceeded for signal {signal_index}. Retry {retries+1}/{MAX_RETRIES} after {retry_delay:.1f}s")
                        import asyncio
                        await asyncio.sleep(retry_delay)
                        retries += 1
                        # Remove the timestamp of the failed request
                        if request_timestamps:
                            request_timestamps.pop()
                    else:
                        print(f"Max retries exceeded for signal {signal_index}")
                        return f"Analysis failed after {MAX_RETRIES} retries due to rate limits"
                else:
                    # Other API errors
                    print(f"API error processing signal {signal_index}: {str(e)}")
                    return f"Analysis failed due to API error: {str(e)}"
                    
            except Exception as e:
                print(f"Unexpected error processing signal {signal_index}: {str(e)}")
                return f"Analysis failed due to unexpected error: {str(e)}"
        
        return analysis_result

    def rate_limited_api_call(self, formatted_signal: str, signal_index: int) -> str:
        """Sync wrapper for the async API call"""
        import asyncio
        
        try:
            # Check if we're already in an event loop
            try:
                loop = asyncio.get_running_loop()
                # We're in an async context, can't create new loop
                raise RuntimeError("Cannot create new event loop from async context - use await instead")
            except RuntimeError:
                # No loop running, safe to create one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(self.rate_limited_api_call_async(formatted_signal, signal_index))
                loop.close()
                return result
        except Exception as e:
            print(f"Error in sync wrapper: {str(e)}")
            return f"Analysis failed due to async error: {str(e)}"

    def extract_json_data(self, analysis_text: str) -> Dict:
        """Extract JSON data from analysis text (matching reference)"""
        # Find the JSON part between ```json and ```
        json_match = re.search(r'```json\s+(.*?)\s+```', analysis_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass  # Try alternative methods if this fails
                
        # Alternative: Try to extract values directly using regex if JSON parsing fails
        choppy_match = re.search(r'choppiness_yes_or_no["\s:]+(["\w]+)', analysis_text, re.IGNORECASE)
        rating_match = re.search(r'signal_rating["\s:]+(["\d]+)', analysis_text, re.IGNORECASE)
        analysis_match = re.search(r'overall_analysis["\s:]+"([^"]+)', analysis_text, re.IGNORECASE)
        
        if choppy_match or rating_match or analysis_match:
            result = {}
            if choppy_match:
                result["choppiness_yes_or_no"] = choppy_match.group(1).strip('"')
            if rating_match:
                result["signal_rating"] = rating_match.group(1).strip('"')
            if analysis_match:
                result["overall_analysis"] = analysis_match.group(1)
            return result
            
        # Final fallback: Look for key phrases in the raw text
        if "market is choppy" in analysis_text.lower() or "choppy market" in analysis_text.lower():
            is_choppy = "yes"
        elif "market is not choppy" in analysis_text.lower() or "market is trending" in analysis_text.lower():
            is_choppy = "no"
        else:
            is_choppy = "unknown"
            
        if "poor quality" in analysis_text.lower() or "low quality" in analysis_text.lower():
            rating = "0" 
        elif "good quality" in analysis_text.lower() or "reliable signal" in analysis_text.lower():
            rating = "1"
        elif "excellent" in analysis_text.lower() or "high quality" in analysis_text.lower():
            rating = "3"
        else:
            rating = "unknown"
            
        # Extract a summary sentence for analysis
        sentences = re.split(r'[.!?]+', analysis_text)
        analysis = next((s.strip() for s in sentences if "overall" in s.lower() or "summary" in s.lower() or "conclude" in s.lower()), "No clear analysis found")
            
        return {
            "choppiness_yes_or_no": is_choppy,
            "signal_rating": rating,
            "overall_analysis": analysis
        }

    async def process_signal(self, formatted_signal: str, signal_id: str, signal_timestamp: str = None) -> Dict:
        """
        Process a single signal through the LLM (properly async version).
        
        Args:
            formatted_signal: Pre-formatted signal text
            signal_id: Signal identifier
            signal_timestamp: Signal timestamp (for compatibility, not used)
            
        Returns:
            Dictionary with LLM results and metadata
        """
        start_time = time.time()
        
        try:
            # Extract signal index from signal_id for rate limiting
            signal_index = hash(signal_id) % 10000  # Simple way to get a number
            
            # Store the session_id we'll create for this signal
            session_id = f"{self.base_session_id}_signal_{signal_index}"
            
            # Make the async API call directly (no sync wrapper needed)
            llm_response = await self.rate_limited_api_call_async(formatted_signal, signal_index)
            
            if llm_response.startswith("Analysis failed"):
                raise Exception(llm_response)
            
            # Parse the response using reference method
            parsed_result = self.extract_json_data(llm_response)
            
            # Map to expected format
            result = {
                'rating': parsed_result.get('signal_rating', '0'),
                'choppiness': parsed_result.get('choppiness_yes_or_no', 'yes'),
                'analysis': parsed_result.get('overall_analysis', llm_response)
            }
            
            # Calculate processing time
            processing_time_ms = int((time.time() - start_time) * 1000)
            
            return {
                'success': True,
                'signal_id': signal_id,
                'session_id': session_id,  # ✅ ENSURE session_id is returned
                'llm_response': json.dumps(result),
                'llm_raw_response': {'choices': [{'message': {'content': llm_response}}]},
                'llm_model_used': self.model,
                'processing_time_ms': processing_time_ms,
                'processed_at': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"Failed to process signal {signal_id}: {str(e)}")
            
            # Also ensure session_id is returned in error case
            signal_index = hash(signal_id) % 10000
            session_id = f"{self.base_session_id}_signal_{signal_index}"
            
            return {
                'success': False,
                'signal_id': signal_id,
                'session_id': session_id,  # ✅ ENSURE session_id is returned even on error
                'error': str(e),
                'llm_model_used': self.model,
                'processing_time_ms': int((time.time() - start_time) * 1000),
                'processed_at': datetime.now()
            }

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
            request_timestamps.pop(0)
        
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

    # Context manager support for compatibility
    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass