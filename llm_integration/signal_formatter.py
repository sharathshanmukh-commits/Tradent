"""
Signal Formatter for LLM Processing

This module formats trading signals and market data into text suitable for LLM analysis.
It prepares structured prompts that include signal details, market context, and technical indicators.
"""

import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)


class SignalFormatter:
    """
    Formats trading signals and market data for LLM processing.
    """
    
    def __init__(self, lookback_periods: int = 20, indicators_to_include: Optional[List[str]] = None):
        """
        Initialize the signal formatter.
        
        Args:
            lookback_periods: Number of periods to include in market context
            indicators_to_include: List of indicator names to include in formatting
        """
        self.lookback_periods = lookback_periods
        self.indicators_to_include = indicators_to_include or [
            'rsi', 'atr', 'sma_20', 'sma_50', 'volume_ratio',
            'bb_upper', 'bb_lower', 'bb_middle', 'stochastic_k', 'stochastic_d'
        ]
        
    def format_signal_data(self, 
                          signal_data: Dict,
                          market_data: pd.DataFrame,
                          market_snapshot: Optional[Dict] = None) -> str:
        """
        Format a trading signal with market context for LLM analysis.
        
        Args:
            signal_data: Signal information including entry, stop loss, target
            market_data: Recent market data DataFrame
            market_snapshot: Additional market context at signal time
            
        Returns:
            Formatted text string for LLM processing
        """
        try:
            # Extract signal details
            signal_type = signal_data.get('signal_type', 'UNKNOWN')
            entry_price = signal_data.get('entry_price', 0)
            stop_loss = signal_data.get('stop_loss', 0)
            target_price = signal_data.get('target_price', 0)
            timestamp = signal_data.get('timestamp', datetime.now())
            
            # Calculate risk/reward metrics
            risk = abs(entry_price - stop_loss)
            reward = abs(target_price - entry_price)
            risk_reward_ratio = reward / risk if risk > 0 else 0
            
            # Format basic signal information
            formatted_text = f"""TRADING SIGNAL ANALYSIS REQUEST

Signal Details:
- Type: {signal_type}
- Entry Price: ${entry_price:.2f}
- Stop Loss: ${stop_loss:.2f}
- Target Price: ${target_price:.2f}
- Risk/Reward Ratio: {risk_reward_ratio:.2f}
- Timestamp: {timestamp}

"""
            
            # Add recent price action
            if not market_data.empty:
                formatted_text += self._format_price_action(market_data)
            
            # Add technical indicators
            if market_snapshot:
                formatted_text += self._format_technical_indicators(market_snapshot)
            
            # Add market context
            formatted_text += self._format_market_context(market_data, signal_data)
            
            # Add analysis request
            formatted_text += """
ANALYSIS REQUEST:
Please analyze this trading signal considering:
1. Market choppiness - Is the market showing choppy behavior?
2. Signal quality - Rate this signal from 0 (worst) to 4 (best)
3. Key risks and opportunities
4. Recommended adjustments to entry/exit levels if any

Provide your analysis in JSON format with the following fields:
- rating: integer 0-4
- choppiness: "yes" or "no"
- analysis: your detailed analysis text
- recommendations: any specific recommendations
"""
            
            return formatted_text
            
        except Exception as e:
            logger.error(f"Error formatting signal data: {str(e)}")
            return self._get_fallback_format(signal_data)
    
    def _format_price_action(self, market_data: pd.DataFrame) -> str:
        """Format recent price action summary."""
        try:
            recent_data = market_data.tail(self.lookback_periods)
            
            # Calculate price statistics
            high = recent_data['high'].max()
            low = recent_data['low'].min()
            current = recent_data['close'].iloc[-1]
            avg_volume = recent_data['volume'].mean()
            
            # Calculate price changes
            price_change_1d = (current - recent_data['close'].iloc[-2]) / recent_data['close'].iloc[-2] * 100 if len(recent_data) >= 2 else 0
            price_change_5d = (current - recent_data['close'].iloc[-6]) / recent_data['close'].iloc[-6] * 100 if len(recent_data) >= 6 else 0
            
            return f"""Recent Price Action ({self.lookback_periods} periods):
- Current Price: ${current:.2f}
- {self.lookback_periods}-Period High: ${high:.2f}
- {self.lookback_periods}-Period Low: ${low:.2f}
- 1-Period Change: {price_change_1d:+.2f}%
- 5-Period Change: {price_change_5d:+.2f}%
- Average Volume: {avg_volume:,.0f}

"""
        except Exception as e:
            logger.error(f"Error formatting price action: {str(e)}")
            return "Recent Price Action: Data unavailable\n\n"
    
    def _format_technical_indicators(self, market_snapshot: Dict) -> str:
        """Format technical indicators from market snapshot."""
        try:
            indicators_text = "Technical Indicators:\n"
            
            for indicator in self.indicators_to_include:
                if indicator in market_snapshot:
                    value = market_snapshot[indicator]
                    if isinstance(value, (int, float)):
                        indicators_text += f"- {indicator.upper()}: {value:.2f}\n"
                    else:
                        indicators_text += f"- {indicator.upper()}: {value}\n"
            
            # Add any additional calculated metrics
            if 'rsi' in market_snapshot:
                rsi = market_snapshot['rsi']
                if rsi > 70:
                    indicators_text += "- RSI Status: OVERBOUGHT\n"
                elif rsi < 30:
                    indicators_text += "- RSI Status: OVERSOLD\n"
                else:
                    indicators_text += "- RSI Status: NEUTRAL\n"
            
            return indicators_text + "\n"
            
        except Exception as e:
            logger.error(f"Error formatting technical indicators: {str(e)}")
            return "Technical Indicators: Data unavailable\n\n"
    
    def _format_market_context(self, market_data: pd.DataFrame, signal_data: Dict) -> str:
        """Format broader market context."""
        try:
            context_text = "Market Context:\n"
            
            # Volume analysis
            if not market_data.empty and 'volume' in market_data.columns:
                recent_volume = market_data['volume'].tail(5).mean()
                avg_volume = market_data['volume'].mean()
                volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1
                
                if volume_ratio > 1.5:
                    context_text += "- Volume: HIGH (above average)\n"
                elif volume_ratio < 0.5:
                    context_text += "- Volume: LOW (below average)\n"
                else:
                    context_text += "- Volume: NORMAL\n"
            
            # Volatility analysis
            if not market_data.empty and 'close' in market_data.columns:
                returns = market_data['close'].pct_change().dropna()
                volatility = returns.std() * 100
                
                if volatility > 3:
                    context_text += "- Volatility: HIGH\n"
                elif volatility < 1:
                    context_text += "- Volatility: LOW\n"
                else:
                    context_text += "- Volatility: MODERATE\n"
            
            # Trend analysis
            if not market_data.empty and len(market_data) >= 20:
                sma_20 = market_data['close'].tail(20).mean()
                current_price = market_data['close'].iloc[-1]
                
                if current_price > sma_20 * 1.02:
                    context_text += "- Trend: BULLISH (above 20-SMA)\n"
                elif current_price < sma_20 * 0.98:
                    context_text += "- Trend: BEARISH (below 20-SMA)\n"
                else:
                    context_text += "- Trend: NEUTRAL (near 20-SMA)\n"
            
            return context_text + "\n"
            
        except Exception as e:
            logger.error(f"Error formatting market context: {str(e)}")
            return "Market Context: Analysis unavailable\n\n"
    
    def _get_fallback_format(self, signal_data: Dict) -> str:
        """Provide a minimal format when full formatting fails."""
        return f"""TRADING SIGNAL ANALYSIS REQUEST

Signal Type: {signal_data.get('signal_type', 'UNKNOWN')}
Entry Price: {signal_data.get('entry_price', 'N/A')}

Please analyze this signal and provide:
- rating: integer 0-4
- choppiness: "yes" or "no"
- analysis: your assessment
- recommendations: any suggestions
"""
    
    def format_batch_signals(self, signals: List[Dict], market_data: pd.DataFrame) -> List[str]:
        """
        Format multiple signals for batch processing.
        
        Args:
            signals: List of signal dictionaries
            market_data: Market data DataFrame
            
        Returns:
            List of formatted text strings
        """
        formatted_signals = []
        
        for signal in signals:
            try:
                # Get market data around signal time
                signal_time = signal.get('timestamp')
                if signal_time and isinstance(market_data.index, pd.DatetimeIndex):
                    # Get data within lookback period of signal
                    end_time = pd.to_datetime(signal_time)
                    start_time = end_time - pd.Timedelta(hours=self.lookback_periods)
                    signal_market_data = market_data[start_time:end_time]
                else:
                    signal_market_data = market_data
                
                formatted_text = self.format_signal_data(
                    signal,
                    signal_market_data,
                    signal.get('market_snapshot')
                )
                formatted_signals.append(formatted_text)
                
            except Exception as e:
                logger.error(f"Error formatting signal {signal.get('signal_id')}: {str(e)}")
                formatted_signals.append(self._get_fallback_format(signal))
        
        return formatted_signals