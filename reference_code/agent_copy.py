"""
Choppiness Detection Agent for Trading Signals

This agent analyzes trading signals for market choppiness and provides
quality ratings using LLM analysis.
"""

import os
import logging
from pathlib import Path
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)


class ChoppinessAgent:
    """
    Agent for analyzing market choppiness and signal quality.
    """
    
    def __init__(self, prompt_path: Optional[str] = None):
        """
        Initialize the choppiness detection agent.
        
        Args:
            prompt_path: Path to the prompt file (defaults to prompts/choppiness_prompt.txt)
        """
        if prompt_path is None:
            # Default to prompts directory relative to project root
            project_root = Path(__file__).parent.parent
            prompt_path = project_root / "prompts" / "choppiness_prompt.txt"
        
        self.prompt_path = Path(prompt_path)
        self.system_prompt = self._load_prompt()
        
        logger.info(f"ChoppinessAgent initialized with prompt from {self.prompt_path}")
    
    def _load_prompt(self) -> str:
        """
        Load the system prompt from file.
        
        Returns:
            System prompt text
        """
        try:
            if self.prompt_path.exists():
                with open(self.prompt_path, 'r') as f:
                    return f.read().strip()
            else:
                logger.warning(f"Prompt file not found at {self.prompt_path}, using default")
                return self._get_default_prompt()
        except Exception as e:
            logger.error(f"Error loading prompt: {e}")
            return self._get_default_prompt()
    
    def _get_default_prompt(self) -> str:
        """
        Get default prompt if file not found.
        
        Returns:
            Default system prompt
        """
        return """You are a trading signal analysis expert. Analyze the provided trading signal and market data.

Provide your analysis in JSON format with these fields:
- rating: integer 0-4 (0=worst, 4=best)
- choppiness: "yes" or "no"
- analysis: detailed analysis text
- recommendations: specific recommendations or null

Focus on:
1. Market choppiness indicators
2. Signal quality and confluence
3. Risk/reward assessment
4. Actionable insights"""
    
    def format_message(self, signal_data: str) -> Dict[str, str]:
        """
        Format the message for LLM processing.
        
        Args:
            signal_data: Formatted signal data string
            
        Returns:
            Dictionary with role and content for LLM
        """
        return {
            "role": "user",
            "content": signal_data
        }
    
    def get_system_message(self) -> Dict[str, str]:
        """
        Get the system message for LLM.
        
        Returns:
            Dictionary with system role and prompt
        """
        return {
            "role": "system",
            "content": self.system_prompt
        }
    
    def create_analysis_request(self, formatted_signal: str) -> list:
        """
        Create a complete analysis request for LLM.
        
        Args:
            formatted_signal: Pre-formatted signal data
            
        Returns:
            List of messages for LLM API
        """
        return [
            self.get_system_message(),
            self.format_message(formatted_signal)
        ]
    
    def validate_response(self, response: Dict) -> bool:
        """
        Validate that LLM response contains required fields.
        
        Args:
            response: Parsed LLM response
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['rating', 'choppiness', 'analysis']
        
        # Check required fields exist
        for field in required_fields:
            if field not in response:
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Validate rating
        try:
            rating = int(response['rating'])
            if not 0 <= rating <= 4:
                logger.warning(f"Invalid rating: {rating}")
                return False
        except (ValueError, TypeError):
            logger.warning(f"Invalid rating type: {response['rating']}")
            return False
        
        # Validate choppiness
        if response['choppiness'] not in ['yes', 'no', 'unknown']:
            logger.warning(f"Invalid choppiness value: {response['choppiness']}")
            return False
        
        return True
    
    def enhance_response(self, response: Dict) -> Dict:
        """
        Enhance/normalize the LLM response.
        
        Args:
            response: Raw parsed response
            
        Returns:
            Enhanced response with normalized values
        """
        # Ensure rating is integer
        if 'rating' in response:
            try:
                response['rating'] = int(response['rating'])
            except:
                response['rating'] = 2  # Default to neutral
        
        # Normalize choppiness
        if 'choppiness' in response:
            chop = str(response['choppiness']).lower()
            if chop in ['yes', 'true', '1']:
                response['choppiness'] = 'yes'
            elif chop in ['no', 'false', '0']:
                response['choppiness'] = 'no'
            else:
                response['choppiness'] = 'unknown'
        
        # Add agent metadata
        response['agent_version'] = '1.0'
        response['prompt_type'] = 'choppiness_analysis'
        
        return response


# Create a singleton instance for easy import
default_agent = ChoppinessAgent()


def get_agent(prompt_path: Optional[str] = None) -> ChoppinessAgent:
    """
    Get a ChoppinessAgent instance.
    
    Args:
        prompt_path: Optional custom prompt path
        
    Returns:
        ChoppinessAgent instance
    """
    if prompt_path:
        return ChoppinessAgent(prompt_path)
    return default_agent