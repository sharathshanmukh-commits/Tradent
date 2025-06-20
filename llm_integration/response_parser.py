"""
LLM Response Parser

This module parses LLM responses and extracts structured data with fallback logic.
It handles various response formats and ensures robust data extraction.
"""

import json
import re
import logging
from typing import Dict, Optional, Any, Union, List
from datetime import datetime

logger = logging.getLogger(__name__)


class LLMResponseParser:
    """
    Parses LLM responses and extracts structured trading signal analysis.
    """
    
    def __init__(self):
        """Initialize the response parser with regex patterns."""
        # Regex patterns for fallback parsing
        self.rating_pattern = re.compile(r'rating["\s]*[:=]\s*(\d+)', re.IGNORECASE)
        self.choppiness_pattern = re.compile(r'choppiness["\s]*[:=]\s*["\']?(yes|no)["\']?', re.IGNORECASE)
        self.analysis_pattern = re.compile(r'analysis["\s]*[:=]\s*["\']([^"\']+)["\']', re.IGNORECASE | re.DOTALL)
        
        # Alternative patterns
        self.alt_rating_pattern = re.compile(r'score["\s]*[:=]\s*(\d+)', re.IGNORECASE)
        self.alt_choppiness_pattern = re.compile(r'choppy["\s]*[:=]\s*["\']?(true|false|yes|no)["\']?', re.IGNORECASE)
    
    def extract_json_data(self, llm_response: Union[str, Dict]) -> Dict:
        """
        Extract structured data from LLM response with multiple fallback strategies.
        
        Args:
            llm_response: Raw LLM response (string or dict)
            
        Returns:
            Dictionary with extracted data including rating, choppiness, analysis
        """
        try:
            # If response is already a dict, extract content
            if isinstance(llm_response, dict):
                if 'choices' in llm_response and len(llm_response['choices']) > 0:
                    content = llm_response['choices'][0]['message']['content']
                elif 'content' in llm_response:
                    content = llm_response['content']
                else:
                    content = str(llm_response)
            else:
                content = str(llm_response)
            
            # Try to parse as JSON first
            parsed_data = self._try_json_parse(content)
            if parsed_data:
                return self._validate_and_normalize(parsed_data)
            
            # Fallback to regex parsing
            logger.debug("JSON parsing failed, attempting regex extraction")
            parsed_data = self._regex_extraction(content)
            if parsed_data:
                return self._validate_and_normalize(parsed_data)
            
            # Final fallback - extract any useful information
            logger.warning("All parsing methods failed, using minimal extraction")
            return self._minimal_extraction(content)
            
        except Exception as e:
            logger.error(f"Error parsing LLM response: {str(e)}")
            return self._get_default_response()
    
    def _try_json_parse(self, content: str) -> Optional[Dict]:
        """
        Attempt to parse content as JSON with various strategies.
        
        Args:
            content: String content to parse
            
        Returns:
            Parsed dictionary or None
        """
        # Direct JSON parse
        try:
            return json.loads(content)
        except:
            pass
        
        # Try to extract JSON from markdown code blocks
        json_match = re.search(r'```(?:json)?\s*(\{[^`]+\})\s*```', content, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except:
                pass
        
        # Try to find JSON object in content
        json_match = re.search(r'\{[^{}]*"(?:rating|choppiness|analysis)"[^{}]*\}', content, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(0))
            except:
                pass
        
        # Try to fix common JSON errors
        fixed_content = self._fix_json_errors(content)
        if fixed_content != content:
            try:
                return json.loads(fixed_content)
            except:
                pass
        
        return None
    
    def _fix_json_errors(self, content: str) -> str:
        """
        Attempt to fix common JSON formatting errors.
        
        Args:
            content: Potentially malformed JSON string
            
        Returns:
            Fixed JSON string
        """
        # Remove trailing commas
        content = re.sub(r',\s*}', '}', content)
        content = re.sub(r',\s*]', ']', content)
        
        # Fix single quotes to double quotes
        content = re.sub(r"'([^']+)':", r'"\1":', content)
        
        # Ensure boolean values are lowercase
        content = content.replace('True', 'true').replace('False', 'false')
        
        # Try to extract just the JSON object if there's extra text
        json_start = content.find('{')
        json_end = content.rfind('}')
        if json_start >= 0 and json_end > json_start:
            content = content[json_start:json_end + 1]
        
        return content
    
    def _regex_extraction(self, content: str) -> Dict:
        """
        Extract data using regex patterns.
        
        Args:
            content: Text content to parse
            
        Returns:
            Dictionary with extracted values
        """
        result = {}
        
        # Extract rating
        rating_match = self.rating_pattern.search(content) or self.alt_rating_pattern.search(content)
        if rating_match:
            try:
                rating = int(rating_match.group(1))
                result['rating'] = max(0, min(4, rating))  # Clamp to 0-4
            except:
                pass
        
        # Extract choppiness
        chop_match = self.choppiness_pattern.search(content) or self.alt_choppiness_pattern.search(content)
        if chop_match:
            chop_value = chop_match.group(1).lower()
            if chop_value in ['yes', 'true']:
                result['choppiness'] = 'yes'
            elif chop_value in ['no', 'false']:
                result['choppiness'] = 'no'
        
        # Extract analysis text
        analysis_match = self.analysis_pattern.search(content)
        if analysis_match:
            result['analysis'] = analysis_match.group(1).strip()
        else:
            # Try to extract any substantial text as analysis
            sentences = re.findall(r'[A-Z][^.!?]+[.!?]', content)
            if sentences:
                result['analysis'] = ' '.join(sentences[:3])  # First 3 sentences
        
        # Look for recommendations
        rec_pattern = re.compile(r'recommend[a-z]*["\s]*[:=]\s*["\']([^"\']+)["\']', re.IGNORECASE)
        rec_match = rec_pattern.search(content)
        if rec_match:
            result['recommendations'] = rec_match.group(1).strip()
        
        return result
    
    def _minimal_extraction(self, content: str) -> Dict:
        """
        Extract minimal useful information when other methods fail.
        
        Args:
            content: Text content
            
        Returns:
            Dictionary with any extractable information
        """
        result = self._get_default_response()
        
        # Try to determine sentiment/quality from keywords
        content_lower = content.lower()
        
        # Rating heuristics
        if any(word in content_lower for word in ['excellent', 'strong', 'very good']):
            result['rating'] = 4
        elif any(word in content_lower for word in ['good', 'positive', 'favorable']):
            result['rating'] = 3
        elif any(word in content_lower for word in ['neutral', 'moderate', 'average']):
            result['rating'] = 2
        elif any(word in content_lower for word in ['weak', 'poor', 'negative']):
            result['rating'] = 1
        elif any(word in content_lower for word in ['avoid', 'bad', 'terrible']):
            result['rating'] = 0
        
        # Choppiness detection
        if any(word in content_lower for word in ['choppy', 'volatile', 'unstable', 'erratic']):
            result['choppiness'] = 'yes'
        elif any(word in content_lower for word in ['trending', 'stable', 'smooth']):
            result['choppiness'] = 'no'
        
        # Use full content as analysis if nothing else
        if len(content) > 10:
            result['analysis'] = content[:500]  # Limit length
        
        return result
    
    def _validate_and_normalize(self, data: Dict) -> Dict:
        """
        Validate and normalize extracted data.
        
        Args:
            data: Extracted data dictionary
            
        Returns:
            Normalized dictionary with all required fields
        """
        result = self._get_default_response()
        
        # Rating validation
        if 'rating' in data:
            try:
                rating = int(data['rating'])
                result['rating'] = max(0, min(4, rating))
            except:
                logger.warning(f"Invalid rating value: {data['rating']}")
        
        # Choppiness validation
        if 'choppiness' in data:
            chop_value = str(data['choppiness']).lower()
            if chop_value in ['yes', 'true', '1']:
                result['choppiness'] = 'yes'
            elif chop_value in ['no', 'false', '0']:
                result['choppiness'] = 'no'
            else:
                result['choppiness'] = 'unknown'
        
        # Analysis text
        if 'analysis' in data and data['analysis']:
            result['analysis'] = str(data['analysis'])[:2000]  # Limit length
        
        # Recommendations
        if 'recommendations' in data and data['recommendations']:
            result['recommendations'] = str(data['recommendations'])[:1000]
        
        # Market context (if provided)
        if 'market_context' in data:
            result['market_context'] = data['market_context']
        
        # Add metadata
        result['parsed_at'] = datetime.now().isoformat()
        result['parser_version'] = '1.0'
        
        return result
    
    def _get_default_response(self) -> Dict:
        """
        Get default response structure.
        
        Returns:
            Dictionary with default values
        """
        return {
            'rating': 2,  # Neutral default
            'choppiness': 'unknown',
            'analysis': 'Unable to parse LLM response',
            'recommendations': None,
            'market_context': {},
            'parsing_error': True
        }
    
    def parse_batch_responses(self, responses: List[Union[str, Dict]]) -> List[Dict]:
        """
        Parse multiple LLM responses.
        
        Args:
            responses: List of LLM responses
            
        Returns:
            List of parsed dictionaries
        """
        parsed_results = []
        
        for i, response in enumerate(responses):
            try:
                parsed = self.extract_json_data(response)
                parsed['response_index'] = i
                parsed_results.append(parsed)
            except Exception as e:
                logger.error(f"Error parsing response {i}: {str(e)}")
                default = self._get_default_response()
                default['response_index'] = i
                default['error'] = str(e)
                parsed_results.append(default)
        
        return parsed_results