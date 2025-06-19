from .base_strategy import BaseStrategy
from .patient_labels_indicator import PatientLabelsIndicator, PatientLabelsConfig
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import time as dt_time

class PatientLabelsStrategy(BaseStrategy):
    """Trading strategy based on Patient Labels Indicator."""
    
    def __init__(self, 
                risk_reward_ratio: float = 1,
                cycles_required: int = 3,
                display_duration: int = 1,
                use_hod_lod_breaks: bool = True,
                use_confirmed_swings: bool = True,
                preserve_dominant_trend: bool = True,
                session_start_time: str = "09:30",
                session_end_time: str = "16:00",
                session_timezone: str = "America/New_York",
                strategy_buffer: float = 0.10,
                **kwargs):
        """
        Initialize PatientLabelsStrategy.
        
        Args:
            risk_reward_ratio: Target risk/reward ratio for trades
            cycles_required: Number of swing cycles required for trend confirmation
            display_duration: How long to display candidate swings
            use_hod_lod_breaks: Whether to use HOD/LOD breaks for trend detection
            use_confirmed_swings: Whether to use confirmed swings for trend detection  
            preserve_dominant_trend: Preserve dominant trend in dual trend mode
            session_start_time: Market session start time (HH:MM)
            session_end_time: Market session end time (HH:MM)
            session_timezone: Timezone for session times
            strategy_buffer: Buffer for SL/TP calculation (default 0.10)
        """
        super().__init__(**kwargs)
        
        # Store all parameters for easy access and saving
        self.params = {
            'risk_reward_ratio': risk_reward_ratio,
            'cycles_required': cycles_required,
            'display_duration': display_duration,
            'use_hod_lod_breaks': use_hod_lod_breaks,
            'use_confirmed_swings': use_confirmed_swings,
            'preserve_dominant_trend': preserve_dominant_trend,
            'session_start_time': session_start_time,
            'session_end_time': session_end_time,
            'session_timezone': session_timezone,
            'strategy_buffer': strategy_buffer,
            **kwargs # Include any other passthrough kwargs
        }
        
        # Store strategy-specific parameters individually if needed for direct access
        self.risk_reward_ratio = risk_reward_ratio
        self.strategy_buffer = strategy_buffer
        
        # Create indicator config
        self.indicator_config = PatientLabelsConfig(
            cycles_required=cycles_required,
            display_duration=display_duration,
            session_start_time=session_start_time,
            session_end_time=session_end_time,
            session_timezone=session_timezone,
            use_hod_lod_breaks=use_hod_lod_breaks,
            use_confirmed_swings=use_confirmed_swings,
            preserve_dominant_trend=preserve_dominant_trend
        )
        
        # Initialize components
        self.indicator = PatientLabelsIndicator(self.indicator_config)
        
        # Signal generation state variables - moved from trade_signal_generator.py
        self.prev_trend_state = 0
        self.last_valid_up_patient_high = None
        self.last_valid_up_patient_low = None
        self.last_valid_down_patient_high = None
        self.last_valid_down_patient_low = None
        self.had_patient_candle_up = False
        self.had_patient_candle_down = False
        self.waiting_for_patient_break_up = False
        self.waiting_for_patient_break_down = False
        self.had_initial_patient_break_up = False
        self.had_initial_patient_break_down = False
        self.initial_entry_done = False
        self.signals = []
    
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Process market data to generate trading signals.
        
        Args:
            data: DataFrame with OHLCV data
                
        Returns:
            DataFrame with added signal columns
        """
        # Calculate patient labels indicator
        patient_labels_result = self.indicator.process_data(data)
        
        # Generate trading signals based on the indicator
        signals_df = self._generate_signals(data, patient_labels_result)
        
        return signals_df
    
    def get_signals(self) -> List[Dict[str, Any]]:
        """Return list of generated trading signals."""
        return self.signals
    
    def _is_within_session(self, bar_datetime: pd.Timestamp) -> bool:
        """Check if the current bar is within the configured trading session."""
        if not self.indicator_config.session_start_time or not self.indicator_config.session_end_time:
            print(f"DEBUG: No session times configured, allowing all bars")
            return True  # If session times are not configured, assume always in session

        try:
            # Handle both "HH:MM" and "HH" formats
            start_parts = self.indicator_config.session_start_time.split(':')
            if len(start_parts) == 1:
                start_h, start_m = int(start_parts[0]), 0
            else:
                start_h, start_m = map(int, start_parts)
            
            end_parts = self.indicator_config.session_end_time.split(':')
            if len(end_parts) == 1:
                end_h, end_m = int(end_parts[0]), 0
            else:
                end_h, end_m = map(int, end_parts)
            
            session_start = dt_time(start_h, start_m)
            session_end = dt_time(end_h, end_m)
            
            # Handle timezone-naive datetimes (DataLoader strips timezone info)
            if bar_datetime.tzinfo is None:
                # Assume the datetime is already in the target timezone (ET from original data)
                current_bar_time = bar_datetime.time()
            else:
                # Convert timezone-aware datetime to configured timezone
                bar_datetime_localized = bar_datetime.tz_convert(self.indicator_config.session_timezone)
                current_bar_time = bar_datetime_localized.time()

            if session_start <= session_end: # Normal session (e.g., 09:30 to 16:00)
                result = session_start <= current_bar_time < session_end # End time is now exclusive
                return result
            else: # Overnight session (e.g., 22:00 to 04:00 next day)
                return current_bar_time >= session_start or current_bar_time <= session_end
        except ValueError:
            print(f"Invalid session_start_time ('{self.indicator_config.session_start_time}') or session_end_time ('{self.indicator_config.session_end_time}') format. Expected HH:MM or HH.")
            return True # Default to in-session if parsing fails
        except Exception as e:
            print(f"Error in _is_within_session for datetime {bar_datetime}: {e}")
            return True # Default to in-session on other errors
    
    def set_parameters(self, **params):
        """Update strategy parameters."""
        for key, value in params.items():
            setattr(self, key, value)
            # Update self.params as well
            if key in self.params: # Only update if it was an initial param
                self.params[key] = value
            elif key in self.indicator_config.__dict__: # Or if it's an indicator config param handled below
                 pass # will be updated in self.params if it matches an indicator_config key
            else: # If it's a new param not originally in __init__ or indicator_config, add it
                self.params[key] = value
        
        # Update indicator config if relevant parameters changed
        config_params = ['cycles_required', 'display_duration', 'use_hod_lod_breaks', 
                        'use_confirmed_swings', 'preserve_dominant_trend',
                        'session_start_time', 'session_end_time', 'session_timezone']
        
        indicator_config_updated = False
        if 'strategy_buffer' in params:
            self.strategy_buffer = params['strategy_buffer']
            self.params['strategy_buffer'] = params['strategy_buffer'] # Ensure self.params is updated

        for param_key in config_params:
            if param_key in params:
                setattr(self.indicator_config, param_key, params[param_key])
                self.params[param_key] = params[param_key] # Update self.params
                indicator_config_updated = True
        
        if indicator_config_updated:
            # Reinitialize indicator with new config
            self.indicator = PatientLabelsIndicator(self.indicator_config)
    
    def reset(self):
        """Reset all state variables."""
        self.prev_trend_state = 0
        self.last_valid_up_patient_high = None
        self.last_valid_up_patient_low = None
        self.last_valid_down_patient_high = None
        self.last_valid_down_patient_low = None
        self.had_patient_candle_up = False
        self.had_patient_candle_down = False
        self.waiting_for_patient_break_up = False
        self.waiting_for_patient_break_down = False
        self.had_initial_patient_break_up = False
        self.had_initial_patient_break_down = False
        self.initial_entry_done = False
        self.signals = []
    
    # Methods from TradeSignalGenerator, renamed _process_result to _generate_signals
    def _generate_signals(self, data: pd.DataFrame, patient_labels_result: pd.DataFrame) -> pd.DataFrame:
        """
        Process patient labels result to generate trading signals.
        
        Args:
            data: DataFrame with OHLCV data
            patient_labels_result: Result DataFrame from PatientLabelsIndicator
        
        Returns:
            DataFrame with added signal columns
        """
        # Make a copy to avoid modifying the original DataFrame and merge with patient_labels_result
        result = data.copy()
        
        # Merge with patient labels analysis data to include up_patient_high, down_patient_low, etc.
        if patient_labels_result is not None and not patient_labels_result.empty:
            # Merge on index to include all patient labels analysis columns
            result = result.merge(patient_labels_result, left_index=True, right_index=True, how='left')
        
        # Add signal columns
        result['signal'] = 0  # 1=buy, -1=sell, 0=none
        result['entry_price'] = np.nan
        result['stop_loss'] = np.nan
        result['target_price'] = np.nan
        
        # Clear existing signals
        self.signals = []
        
        # Process each bar
        for i in range(1, len(data)):
            # Check if current bar is within trading session
            current_datetime = pd.to_datetime(data.iloc[i]['datetime'])
            is_in_session = self._is_within_session(current_datetime)
            
            # Skip bars outside session window
            if not is_in_session:
                continue
            
            # Extract trend information from current bar
            current_bar = patient_labels_result.iloc[i]
            previous_bar = patient_labels_result.iloc[i-1] if i > 0 else None
            
            # Extract OHLC data
            price_data = data.iloc[i]
            
            # Get patient candle values
            up_patient_high = np.nan
            down_patient_low = np.nan
            
            if 'up_patient_high' in current_bar:
                up_patient_high = current_bar['up_patient_high']
            
            if 'down_patient_low' in current_bar:
                down_patient_low = current_bar['down_patient_low']
            
            # Extract trend states
            trend_state = current_bar['trend_state']
            
            # Check for trend changes - MODIFIED TO MATCH PINE SCRIPT LOGIC
            trend_changed = trend_state != self.prev_trend_state
            uptrend_start = trend_state == 1 and self.prev_trend_state != 1
            downtrend_start = trend_state == -1 and self.prev_trend_state != -1
            
            # Track patient candle values before they disappear
            self._track_patient_candles(current_bar)
            
            # Detect patient candle breaks
            up_patient_break = self.had_patient_candle_up and np.isnan(up_patient_high)
            down_patient_break = self.had_patient_candle_down and np.isnan(down_patient_low)
            
            # Reset the flags after detecting breaks
            if up_patient_break:
                self.had_patient_candle_up = False
            
            if down_patient_break:
                self.had_patient_candle_down = False
            
            # Reset on trend changes or when trends start/change
            if trend_changed or uptrend_start or downtrend_start:
                self._reset_on_trend_change(trend_state)
            
            # MODIFIED: Only set waiting flags on trend state changes, not on in_uptrend/in_downtrend changes
            if uptrend_start:
                self.waiting_for_patient_break_up = True
                self.had_initial_patient_break_up = False
            
            if downtrend_start:
                self.waiting_for_patient_break_down = True
                self.had_initial_patient_break_down = False
                
            # GENERATE SIGNALS
            
            # Initial buy signal - MATCHING PINE SCRIPT LOGIC
            if self.waiting_for_patient_break_up and up_patient_break and not self.had_initial_patient_break_up:
                signal = self._generate_buy_signal(i, price_data, "initial_up_patient_break", current_datetime)
                self.signals.append(signal)
                
                # Update result DataFrame
                result.iloc[i, result.columns.get_loc('signal')] = 1
                result.iloc[i, result.columns.get_loc('entry_price')] = signal['entry_price']
                result.iloc[i, result.columns.get_loc('stop_loss')] = signal['stop_loss']
                result.iloc[i, result.columns.get_loc('target_price')] = signal['target_price']
                
                # Mark that we've had initial break
                self.had_initial_patient_break_up = True
                self.initial_entry_done = True
            
            # Initial sell signal - MATCHING PINE SCRIPT LOGIC
            elif self.waiting_for_patient_break_down and down_patient_break and not self.had_initial_patient_break_down:
                signal = self._generate_sell_signal(i, price_data, "initial_down_patient_break", current_datetime)
                self.signals.append(signal)
                
                # Update result DataFrame
                result.iloc[i, result.columns.get_loc('signal')] = -1
                result.iloc[i, result.columns.get_loc('entry_price')] = signal['entry_price']
                result.iloc[i, result.columns.get_loc('stop_loss')] = signal['stop_loss']
                result.iloc[i, result.columns.get_loc('target_price')] = signal['target_price']
                
                # Mark that we've had initial break
                self.had_initial_patient_break_down = True
                self.initial_entry_done = True
            
            # Additional buy signals - require confirmed uptrend state
            elif self.initial_entry_done and trend_state == 1 and up_patient_break:
                signal = self._generate_buy_signal(i, price_data, "additional_up_patient_break", current_datetime)
                self.signals.append(signal)
                
                # Update result DataFrame
                result.iloc[i, result.columns.get_loc('signal')] = 1
                result.iloc[i, result.columns.get_loc('entry_price')] = signal['entry_price']
                result.iloc[i, result.columns.get_loc('stop_loss')] = signal['stop_loss']
                result.iloc[i, result.columns.get_loc('target_price')] = signal['target_price']
            
            # Additional sell signals - require confirmed downtrend state
            elif self.initial_entry_done and trend_state == -1 and down_patient_break:
                signal = self._generate_sell_signal(i, price_data, "additional_down_patient_break", current_datetime)
                self.signals.append(signal)
                
                # Update result DataFrame
                result.iloc[i, result.columns.get_loc('signal')] = -1
                result.iloc[i, result.columns.get_loc('entry_price')] = signal['entry_price']
                result.iloc[i, result.columns.get_loc('stop_loss')] = signal['stop_loss']
                result.iloc[i, result.columns.get_loc('target_price')] = signal['target_price']
            
            # Save current trend state for next iteration
            self.prev_trend_state = trend_state
        
        return result
    
    def _track_patient_candles(self, current_bar):
        """Track patient candle values before they disappear."""
        # Get values from current bar
        up_patient_high = current_bar.get('up_patient_high', np.nan)
        down_patient_low = current_bar.get('down_patient_low', np.nan)
        
        # Track uptrend patient candle
        if not np.isnan(up_patient_high):
            self.last_valid_up_patient_high = up_patient_high
            self.last_valid_up_patient_low = current_bar['low']
            self.had_patient_candle_up = True
        
        # Track downtrend patient candle
        if not np.isnan(down_patient_low):
            self.last_valid_down_patient_low = down_patient_low
            self.last_valid_down_patient_high = current_bar['high']
            self.had_patient_candle_down = True
    
    def _reset_on_trend_change(self, new_trend_state):
        """Reset signal state when trend changes."""
        self.initial_entry_done = False
        
        # Set appropriate waiting flags based on new trend state
        if new_trend_state == 1:  # Uptrend
            self.waiting_for_patient_break_up = True
            self.waiting_for_patient_break_down = False
        elif new_trend_state == -1:  # Downtrend
            self.waiting_for_patient_break_down = True
            self.waiting_for_patient_break_up = False
        else:  # Neutral
            # In neutral state, reset both waiting flags
            self.waiting_for_patient_break_up = False
            self.waiting_for_patient_break_down = False
        
        self.had_initial_patient_break_up = False
        self.had_initial_patient_break_down = False
    
    def _generate_buy_signal(self, bar_index, price_data, reason, market_datetime=None):
        """Generate a buy signal at the current bar."""
        raw_entry_price = self.last_valid_up_patient_high
        
        # Conceptual entry and SL using strategy_buffer
        conceptual_entry = raw_entry_price + self.strategy_buffer
        stop_loss = self.last_valid_up_patient_low - self.strategy_buffer
        
        # Calculate target price based on risk/reward ratio
        risk = conceptual_entry - stop_loss
        
        if risk <= 0: # Ensure risk is positive
            print(f"Warning: BUY signal at bar {bar_index}, non-positive risk ({risk:.2f}). SL: {stop_loss:.2f}, Conceptual Entry: {conceptual_entry:.2f}. Setting SL/TP to NaN.")
            target_price = np.nan
            stop_loss = np.nan # Also set SL to NaN if risk is invalid
            risk = np.nan
        else:
            target_price = conceptual_entry + (risk * self.risk_reward_ratio)
        
        # Create signal dictionary
        signal = {
            'bar_index': bar_index,
            'type': 'buy',
            'entry_price': raw_entry_price, # Store raw entry price
            'stop_loss': stop_loss,
            'target_price': target_price,
            'risk': risk,
            'risk_reward_ratio': self.risk_reward_ratio,
            'reason': reason
        }
        
        # Add datetime - prefer market_datetime parameter, fallback to price_data
        if market_datetime is not None:
            signal['datetime'] = market_datetime
        elif hasattr(price_data, 'datetime') or 'datetime' in price_data:
            dt_value = price_data['datetime']
            if pd.isna(dt_value):
                # Use current time if datetime is NaT
                signal['datetime'] = pd.Timestamp.now()
            else:
                signal['datetime'] = dt_value
        else:
            signal['datetime'] = pd.Timestamp.now()
        
        return signal
    
    def _generate_sell_signal(self, bar_index, price_data, reason, market_datetime=None):
        """Generate a sell signal at the current bar."""
        raw_entry_price = self.last_valid_down_patient_low
        
        # Conceptual entry and SL using strategy_buffer
        conceptual_entry = raw_entry_price - self.strategy_buffer
        stop_loss = self.last_valid_down_patient_high + self.strategy_buffer
        
        # Calculate target price based on risk/reward ratio
        risk = stop_loss - conceptual_entry
        
        if risk <= 0: # Ensure risk is positive
            print(f"Warning: SELL signal at bar {bar_index}, non-positive risk ({risk:.2f}). SL: {stop_loss:.2f}, Conceptual Entry: {conceptual_entry:.2f}. Setting SL/TP to NaN.")
            target_price = np.nan
            stop_loss = np.nan # Also set SL to NaN if risk is invalid
            risk = np.nan
        else:
            target_price = conceptual_entry - (risk * self.risk_reward_ratio)
        
        # Create signal dictionary
        signal = {
            'bar_index': bar_index,
            'type': 'sell',
            'entry_price': raw_entry_price, # Store raw entry price
            'stop_loss': stop_loss,
            'target_price': target_price,
            'risk': risk,
            'risk_reward_ratio': self.risk_reward_ratio,
            'reason': reason
        }
        
        # Add datetime - prefer market_datetime parameter, fallback to price_data
        if market_datetime is not None:
            signal['datetime'] = market_datetime
        elif hasattr(price_data, 'datetime') or 'datetime' in price_data:
            dt_value = price_data['datetime']
            if pd.isna(dt_value):
                # Use current time if datetime is NaT
                signal['datetime'] = pd.Timestamp.now()
            else:
                signal['datetime'] = dt_value
        else:
            signal['datetime'] = pd.Timestamp.now()
        
        return signal 