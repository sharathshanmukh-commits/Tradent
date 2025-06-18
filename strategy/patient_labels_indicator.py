"""
Patient Labels Indicator - Python Implementation

A trend detection indicator that tracks swing highs/lows, patient candles,
and provides trend state information based on price action.

Original code translated from Pine Script to Python with production-level structure.
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Union
import logging
from enum import Enum
from datetime import time as dt_time, datetime # Added datetime for dt_time alias and datetime objects

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('patient_labels')


class TrendState(Enum):
    """Enumeration for trend states."""
    NEUTRAL = 0
    UPTREND = 1
    DOWNTREND = -1


@dataclass
class PatientLabelsConfig:
    """Configuration parameters for the Patient Labels indicator."""
    show_downtrend: bool = True
    show_uptrend: bool = True
    show_trend_state: bool = True
    use_hod_lod_breaks: bool = True
    use_confirmed_swings: bool = True
    cycles_required: int = 3
    debug_mode: bool = False
    display_duration: int = 1
    preserve_dominant_trend: bool = True
    session_start_time: Optional[str] = None  # e.g., "09:30"
    session_end_time: Optional[str] = None    # e.g., "16:00"
    session_timezone: str = "America/New_York"  # Default to ET instead of UTC


@dataclass
class SwingPoint:
    """Represents a swing high or low point."""
    price: float
    bar_index: int
    cycle: int
    label_id: Optional[str] = None


@dataclass
class PatientCandle:
    """Represents a patient candle in a trend."""
    high: float
    low: float
    bar_index: int
    label_id: Optional[str] = None
    high_label_id: Optional[str] = None
    low_label_id: Optional[str] = None


@dataclass
class CandidateSwing:
    """Represents a candidate swing point not yet confirmed."""
    price: float
    bar_index: int
    creation_bar: int
    label_id: Optional[str] = None
    confirmed: bool = False


class PatientLabelsState:
    """Maintains the state for the Patient Labels indicator."""
    
    def __init__(self, config: PatientLabelsConfig):
        self.config = config
        self.reset_state()
        
    def reset_state(self):
        """Reset all state variables."""
        # General trend state
        self.trend_state = TrendState.NEUTRAL
        self.dominant_trend = TrendState.NEUTRAL
        self.is_dual_trend_active = False
        
        # Downtrend variables
        self.in_downtrend = False
        self.down_swing_cycle = 0
        self.confirmed_lh: Optional[SwingPoint] = None
        self.down_trend_broken = False
        self.down_trend_break_bar: Optional[int] = None
        self.lod_broken = False
        self.day_low: Optional[float] = None
        self.day_low_bar: Optional[int] = None
        self.low_at_trend_break: Optional[float] = None
        self.latest_confirmed_ll: Optional[float] = None
        self.latest_confirmed_ll_bar: Optional[int] = None
        self.lower_low: Optional[SwingPoint] = None  # Renamed from LLman
        self.down_patient: Optional[PatientCandle] = None
        self.candidate_lh: Optional[CandidateSwing] = None
        self.highest_since_ll: Optional[float] = None
        self.highest_since_ll_bar_index: Optional[int] = None
        
        # Uptrend variables
        self.in_uptrend = False
        self.up_swing_cycle = 0
        self.confirmed_hl: Optional[SwingPoint] = None
        self.up_trend_broken = False
        self.up_trend_break_bar: Optional[int] = None
        self.hod_broken = False
        self.day_high: Optional[float] = None
        self.day_high_bar: Optional[int] = None
        self.high_at_trend_break: Optional[float] = None
        self.higher_high: Optional[SwingPoint] = None  # Renamed from HHman
        self.up_patient: Optional[PatientCandle] = None
        self.candidate_hl: Optional[CandidateSwing] = None
        self.lowest_since_hh: Optional[float] = None
        self.lowest_since_hh_bar_index: Optional[int] = None
        
        # Session tracking
        self.reset_occurred = False
        self.labels: Dict[str, Dict] = {}  # Store all labels for visualization
        self.previous_is_regular_session: bool = False # Added for session logic


class PatientLabelsIndicator:
    """Main class for the Patient Labels indicator implementation."""
    
    def __init__(self, config: Optional[PatientLabelsConfig] = None):
        """
        Initialize the indicator with configuration.
        
        Args:
            config: Configuration parameters for the indicator
        """
        self.config = config or PatientLabelsConfig()
        self.state = PatientLabelsState(self.config)
        
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Process market data to calculate the indicator values.
        
        Args:
            data: DataFrame with OHLCV data
                Required columns: ['open', 'high', 'low', 'close', 'volume', 'datetime']
                
        Returns:
            DataFrame with added indicator columns
        """
        # Ensure required columns exist
        required_columns = ['open', 'high', 'low', 'close', 'datetime']
        for col in required_columns:
            if col not in data.columns:
                raise ValueError(f"Required column '{col}' missing from data")
        
        # Make a copy to avoid modifying the original DataFrame
        result = data.copy()
        
        # Initialize columns for indicator outputs
        result['trend_state'] = 0
        result['up_patient_high'] = np.nan
        result['down_patient_low'] = np.nan
        result['day_high'] = np.nan
        result['day_low'] = np.nan
        
        # Add columns for dual trend information
        result['is_dual_trend_active'] = False
        result['in_uptrend'] = False
        result['up_swing_cycle'] = 0
        result['in_downtrend'] = False
        result['down_swing_cycle'] = 0
        result['dominant_trend'] = 0 # Will store TrendState.value
        
        # Ensure datetime column is in datetime format
        if 'datetime' in result.columns and not pd.api.types.is_datetime64_any_dtype(result['datetime']):
            result['datetime'] = pd.to_datetime(result['datetime'])

        # Create a labels dictionary to store visualization info
        labels = []
        
        # Reset state before starting
        self.state.reset_state()
        
        # Process each bar
        for i in range(1, len(data)):
            # Current bar data
            current = data.iloc[i]
            previous = data.iloc[i-1]
            
            # Check if it's a new session
            is_new_session = self._is_new_session(current, previous)
            
            # Update state based on current bar
            self._update_state(i, current, previous, is_new_session)
            
            # Update result dataframe
            result.iloc[i, result.columns.get_loc('trend_state')] = self.state.trend_state.value
            
            if self.state.up_patient is not None:
                result.iloc[i, result.columns.get_loc('up_patient_high')] = self.state.up_patient.high
                
            if self.state.down_patient is not None:
                result.iloc[i, result.columns.get_loc('down_patient_low')] = self.state.down_patient.low
                
            if self.state.day_high is not None:
                result.iloc[i, result.columns.get_loc('day_high')] = self.state.day_high
                
            if self.state.day_low is not None:
                result.iloc[i, result.columns.get_loc('day_low')] = self.state.day_low
                
            # Add dual trend information from state to result DataFrame
            result.iloc[i, result.columns.get_loc('is_dual_trend_active')] = self.state.is_dual_trend_active
            result.iloc[i, result.columns.get_loc('in_uptrend')] = self.state.in_uptrend
            result.iloc[i, result.columns.get_loc('up_swing_cycle')] = self.state.up_swing_cycle
            result.iloc[i, result.columns.get_loc('in_downtrend')] = self.state.in_downtrend
            result.iloc[i, result.columns.get_loc('down_swing_cycle')] = self.state.down_swing_cycle
            result.iloc[i, result.columns.get_loc('dominant_trend')] = self.state.dominant_trend.value
            
            # Collect current labels
            if i == len(data) - 1:
                labels = self._collect_labels()
        
        # Add labels to the result metadata
        result.attrs['labels'] = labels
        
        return result
    
    def _is_new_session(self, current: pd.Series, previous: pd.Series) -> bool:
        """Determine if the current bar is the start of a new session."""
        # Check if day has changed
        current_date = pd.to_datetime(current['datetime']).date()
        previous_date = pd.to_datetime(previous['datetime']).date()
        
        return current_date != previous_date

    def _is_within_session(self, bar_datetime: pd.Timestamp) -> bool:
        if not self.config.session_start_time or not self.config.session_end_time:
            return True  # If session times are not configured, assume always in session

        try:
            # Handle both "HH:MM" and "HH" formats
            start_parts = self.config.session_start_time.split(':')
            if len(start_parts) == 1:
                start_h, start_m = int(start_parts[0]), 0
            else:
                start_h, start_m = map(int, start_parts)
            
            end_parts = self.config.session_end_time.split(':')
            if len(end_parts) == 1:
                end_h, end_m = int(end_parts[0]), 0
            else:
                end_h, end_m = map(int, end_parts)
            
            session_start = dt_time(start_h, start_m)
            session_end = dt_time(end_h, end_m)
            
            # Ensure bar_datetime is timezone-aware using the configured timezone
            if bar_datetime.tzinfo is None:
                bar_datetime_localized = bar_datetime.tz_localize(self.config.session_timezone)
            else:
                bar_datetime_localized = bar_datetime.tz_convert(self.config.session_timezone)
            
            current_bar_time = bar_datetime_localized.time()

            if session_start <= session_end: # Normal session (e.g., 09:30 to 16:00)
                return session_start <= current_bar_time <= session_end # End time is now inclusive
            else: # Overnight session (e.g., 22:00 to 04:00 next day)
                return current_bar_time >= session_start or current_bar_time <= session_end
        except ValueError:
            logger.error(f"Invalid session_start_time ('{self.config.session_start_time}') or session_end_time ('{self.config.session_end_time}') format. Expected HH:MM or HH.")
            return True # Default to in-session if parsing fails
        except Exception as e:
            logger.error(f"Error in _is_within_session for datetime {bar_datetime}: {e}")
            return True # Default to in-session on other errors
            
    def _update_state(self, bar_index: int, current: pd.Series, previous: pd.Series, is_first_bar_of_day: bool):
        """Update the indicator state based on the current bar."""
        current_dt = pd.to_datetime(current['datetime'])
        current_is_regular_session = self._is_within_session(current_dt)

        change_in_regular_session = current_is_regular_session != self.state.previous_is_regular_session
        
        is_first_regular_bar = current_is_regular_session and \
                               (change_in_regular_session or is_first_bar_of_day)

        if is_first_regular_bar and not self.state.reset_occurred:
            self._init_new_session(bar_index, current) 
            # self.state.reset_occurred is set to True in _init_new_session
        
        if not current_is_regular_session:
            self.state.reset_occurred = False # PineScript: if not isRegularSession resetOccurred := false
        
        if current_is_regular_session:
            # Day high/low updates and main processing logic only if in regular session
            self._process_regular_session(bar_index, current, previous)
        # else:
            # If not in regular session, PineScript effectively does nothing for main trend logic.
            # Day high/low also don't update if they are inside _process_regular_session.
            # The resetOccurred flag is managed above. Other states are frozen.
            
        self.state.previous_is_regular_session = current_is_regular_session # Store for next bar's change detection
    
    def _init_new_session(self, bar_index: int, current: pd.Series):
        """Initialize a new session with the current bar."""
        # Set day high/low
        self.state.day_high = current['high']
        self.state.day_high_bar = bar_index
        self.state.day_low = current['low']
        self.state.day_low_bar = bar_index
        
        # Reset all state variables
        self.state.reset_state()
        self.state.reset_occurred = True
    
    def _process_regular_session(self, bar_index: int, current: pd.Series, previous: pd.Series):
        """Process regular session bars."""
        # Update day high/low
        if self.state.day_high is None or current['high'] > self.state.day_high: # Initialize if None
            self.state.day_high = current['high']
        if self.state.day_low is None or current['low'] < self.state.day_low: # Initialize if None
            self.state.day_low = current['low']
            self.state.day_low_bar = bar_index
        
        # Define candle patterns
        down_candle = (current['low'] < previous['low']) and (current['close'] < previous['low'])
        up_candle = (current['high'] > previous['high']) and (current['close'] > previous['high'])
        down_contained = current['low'] >= previous['low']
        up_contained = current['high'] <= previous['high']
        
        # Check if both trends are active simultaneously - MOVED calculation down
        # self.state.is_dual_trend_active = self.state.in_uptrend and self.state.in_downtrend
        
        # Update dominant trend tracking
        if self.state.trend_state == TrendState.UPTREND and self.state.dominant_trend == TrendState.NEUTRAL:
            self.state.dominant_trend = TrendState.UPTREND
        elif self.state.trend_state == TrendState.DOWNTREND and self.state.dominant_trend == TrendState.NEUTRAL:
            self.state.dominant_trend = TrendState.DOWNTREND
        
        # For dual modes, determine if one trend is stronger than the other
        if self.state.is_dual_trend_active and self.state.dominant_trend == TrendState.NEUTRAL:
            if self.state.up_swing_cycle > self.state.down_swing_cycle:
                self.state.dominant_trend = TrendState.UPTREND
            elif self.state.down_swing_cycle > self.state.up_swing_cycle:
                self.state.dominant_trend = TrendState.DOWNTREND
        
        # Process downtrend if enabled
        if self.config.show_downtrend:
            self._process_downtrend(bar_index, current, previous, down_candle, down_contained)
        
        # Process uptrend if enabled
        if self.config.show_uptrend:
            self._process_uptrend(bar_index, current, previous, up_candle, up_contained)
        
        # <<< FIX: Calculate is_dual_trend_active AFTER processing trends for the current bar >>>
        self.state.is_dual_trend_active = self.state.in_uptrend and self.state.in_downtrend

        # Update trend state based on the outcomes of uptrend/downtrend processing
        self._update_trend_state(current) # Pass current bar data
    
    def _process_downtrend(self, bar_index: int, current: pd.Series, previous: pd.Series, 
                           down_candle: bool, down_contained: bool):
        """Process downtrend logic."""
        # Check candidate LH display duration expiry
        if (self.state.candidate_lh is not None and 
            self.state.candidate_lh.confirmed and 
            bar_index > self.state.candidate_lh.creation_bar + self.config.display_duration):
            self.state.candidate_lh = None
        
        # Check for downtrend break conditions
        if (self.state.in_downtrend and 
            not self.state.down_trend_broken and 
            self.state.confirmed_lh is not None):
            
            # Original condition: break above confirmed LH
            if current['high'] > self.state.confirmed_lh.price:
                self._handle_downtrend_break(bar_index, current)
            
            # Check if CHL becomes higher than a confirmed LH (reversal pattern broken)
            elif (self.state.candidate_lh is not None and 
                  self.state.down_swing_cycle >= self.config.cycles_required and 
                  self.state.candidate_lh.price > self.state.confirmed_lh.price):
                
                if self.config.debug_mode:
                    logger.debug(f"Downtrend PATTERN BREAK: CandLH={self.state.candidate_lh.price} > ConfirmedLH={self.state.confirmed_lh.price} at bar {bar_index}")
                
                self._handle_downtrend_break(bar_index, current)
        
        # Check for LOD Break after trend break
        if self.state.down_trend_broken and not self.state.lod_broken:
            if bar_index > self.state.down_trend_break_bar:
                if current['low'] < self.state.low_at_trend_break:
                    self._handle_lod_break(bar_index, current, previous)
        
        # Regular downtrend start (no broken trend)
        if not self.state.in_downtrend and down_candle:
            self._start_new_downtrend(bar_index, current, previous)
        
        # Core Downtrend Swing Logic
        if self.state.in_downtrend:
            # (A) Patient Candle Formation (Downtrend)
            if down_contained:
                self._handle_downtrend_patient_formation(bar_index, current, previous)
            
            # (B) Patient Candle Break (Downtrend)
            else:
                self._handle_downtrend_patient_break(bar_index, current)
            
            # (C) Candidate Confirmation - CRITICAL CHECK (Downtrend)
            if (self.state.lower_low is not None and 
                current['low'] < self.state.lower_low.price):
                self._confirm_downtrend_swing(bar_index)
    
    def _process_uptrend(self, bar_index: int, current: pd.Series, previous: pd.Series, 
                         up_candle: bool, up_contained: bool):
        """Process uptrend logic."""
        # Check candidate HL display duration expiry
        if (self.state.candidate_hl is not None and 
            self.state.candidate_hl.confirmed and 
            bar_index > self.state.candidate_hl.creation_bar + self.config.display_duration):
            self.state.candidate_hl = None
        
        # Check for uptrend break conditions
        if (self.state.in_uptrend and 
            not self.state.up_trend_broken and 
            self.state.confirmed_hl is not None):
            
            # Original condition: break below confirmed HL
            if current['low'] < self.state.confirmed_hl.price:
                self._handle_uptrend_break(bar_index, current)
        
        # Check for HOD Break after trend break
        if self.state.up_trend_broken and not self.state.hod_broken:
            if bar_index > self.state.up_trend_break_bar:
                if current['high'] > self.state.high_at_trend_break:
                    self._handle_hod_break(bar_index, current, previous)
        
        # Regular uptrend start (no broken trend)
        if not self.state.in_uptrend and up_candle:
            self._start_new_uptrend(bar_index, current, previous)
        
        # Core Uptrend Swing Logic
        if self.state.in_uptrend:
            # (A) Patient Candle Formation (Uptrend)
            if up_contained:
                self._handle_uptrend_patient_formation(bar_index, current, previous)
            
            # (B) Patient Candle Break (Uptrend)
            else:
                self._handle_uptrend_patient_break(bar_index, current)
            
            # (C) Candidate Confirmation - CRITICAL CHECK (Uptrend)
            if (self.state.higher_high is not None and 
                current['high'] > self.state.higher_high.price):
                self._confirm_uptrend_swing(bar_index)
            
            # Check if CHL is lower than previous confirmed HL (pattern break)
            elif (self.state.candidate_hl is not None and
                  self.state.confirmed_hl is not None and 
                  self.state.up_swing_cycle >= self.config.cycles_required and 
                  self.state.candidate_hl.price < self.state.confirmed_hl.price):
                
                if self.config.debug_mode:
                    logger.debug(f"Uptrend PATTERN BREAK: CandHL={self.state.candidate_hl.price} < ConfirmedHL={self.state.confirmed_hl.price} at bar {bar_index}")
                
                self._handle_uptrend_break(bar_index, current)
    
    def _handle_downtrend_break(self, bar_index: int, current: pd.Series):
        """Handle downtrend break logic."""
        self.state.down_trend_broken = True
        self.state.in_downtrend = False
        self.state.lod_broken = False
        self.state.down_trend_break_bar = bar_index
        
        # Add trend break label
        trend_break_id = f"down_trend_break_{bar_index}"
        self.state.labels[trend_break_id] = {
            'x': bar_index,
            'y': current['high'],
            'text': "TREND BREAK",
            'style': 'label_up',
            'color': 'purple'
        }
        
        # Reset trend state to neutral only if downtrend is dominant or not in dual mode
        if (not self.state.is_dual_trend_active or 
            not self.config.preserve_dominant_trend or 
            self.state.dominant_trend == TrendState.DOWNTREND):
            self.state.trend_state = TrendState.NEUTRAL
            self.state.dominant_trend = TrendState.NEUTRAL
        
        # Always use day low as reference
        self.state.low_at_trend_break = self.state.day_low
        
        # Clear patient candle state
        self.state.down_patient = None
        
        # Don't delete candidateLH immediately, just mark it as confirmed
        if self.state.candidate_lh is not None and not self.state.candidate_lh.confirmed:
            self.state.candidate_lh.confirmed = True
        
        self.state.highest_since_ll = None
        self.state.highest_since_ll_bar_index = None
    
    def _handle_lod_break(self, bar_index: int, current: pd.Series, previous: pd.Series):
        """Handle LOD break after trend break."""
        self.state.lod_broken = True
        
        # Add LOD break label
        lod_break_id = f"lod_break_{bar_index}"
        self.state.labels[lod_break_id] = {
            'x': bar_index,
            'y': current['low'],
            'text': "LOD BREAK",
            'style': 'label_up',
            'color': 'blue'
        }
        
        # Immediately start a new trend when LOD is broken
        self.state.in_downtrend = True
        self.state.down_trend_broken = False
        self.state.lod_broken = False
        self.state.down_swing_cycle = 1
        
        # LH1 is defined as the higher of current and previous candle's high
        lh_price = max(current['high'], previous['high'])
        self.state.confirmed_lh = SwingPoint(
            price=lh_price,
            bar_index=bar_index,
            cycle=self.state.down_swing_cycle
        )
        
        # Reset all other variables to start fresh
        self.state.lower_low = None
        self.state.down_patient = None
        self.state.candidate_lh = None
        self.state.highest_since_ll = None
        self.state.highest_since_ll_bar_index = None
        
        # Always set trendState when LOD is broken
        self.state.trend_state = TrendState.DOWNTREND
        # Always update dominantTrend when LOD is broken
        self.state.dominant_trend = TrendState.DOWNTREND
        
        # Clear trend break labels
        if f"down_trend_break_{self.state.down_trend_break_bar}" in self.state.labels:
            del self.state.labels[f"down_trend_break_{self.state.down_trend_break_bar}"]
        
        if self.config.debug_mode:
            debug_text = f"LOD: {self.state.low_at_trend_break}\nCurrent: {current['low']}"
            logger.debug(debug_text)
    
    def _start_new_downtrend(self, bar_index: int, current: pd.Series, previous: pd.Series):
        """Start a new downtrend."""
        self.state.in_downtrend = True
        self.state.down_trend_broken = False
        self.state.down_swing_cycle = 1
        
        # LH1 is defined as the higher of the breaker's high (current high) and previous candle's high
        lh_price = max(current['high'], previous['high'])
        self.state.confirmed_lh = SwingPoint(
            price=lh_price,
            bar_index=bar_index,
            cycle=self.state.down_swing_cycle
        )
        
        # Reset patient and candidate variables
        self.state.down_patient = None
        self.state.candidate_lh = None
        self.state.lower_low = None
        self.state.highest_since_ll = None
        self.state.highest_since_ll_bar_index = None
        self.state.latest_confirmed_ll = None
        self.state.latest_confirmed_ll_bar = None
    
    def _handle_downtrend_patient_formation(self, bar_index: int, current: pd.Series, previous: pd.Series):
        """Handle patient candle formation in downtrend."""
        # If no previous patient candle, this is the first one
        if self.state.down_patient is None:
            # Initialize lower_low as the previous candle's low (but only if not already defined)
            if self.state.lower_low is None:
                ll_price = previous['low']
                self.state.lower_low = SwingPoint(
                    price=ll_price,
                    bar_index=bar_index - 1,
                    cycle=self.state.down_swing_cycle
                )
                
                # Add LL label
                ll_label_id = f"ll_{bar_index-1}"
                self.state.labels[ll_label_id] = {
                    'x': bar_index - 1,
                    'y': ll_price,
                    'text': f"LL{self.state.down_swing_cycle} {ll_price:.2f}",
                    'style': 'label_up',
                    'color': 'green'
                }
                self.state.lower_low.label_id = ll_label_id
            
            # Initialize highest point tracking (if not already tracking)
            if self.state.highest_since_ll is None:
                self.state.highest_since_ll = current['high']
                self.state.highest_since_ll_bar_index = bar_index
            
            # Set up this patient candle
            self.state.down_patient = PatientCandle(
                high=current['high'],
                low=current['low'],
                bar_index=bar_index
            )
            
            # Add patient candle label
            pc_label_id = f"down_patient_{bar_index}"
            pc_text = "DT Patient" if self.state.is_dual_trend_active else "Patient"
            self.state.labels[pc_label_id] = {
                'x': bar_index,
                'y': current['low'],
                'text': pc_text,
                'style': 'label_up',
                'color': 'orange' if not self.state.is_dual_trend_active else 'red'
            }
            self.state.down_patient.label_id = pc_label_id
            
            # Add low point label
            low_label_id = f"dt_patient_low_{bar_index}"
            self.state.labels[low_label_id] = {
                'x': bar_index,
                'y': current['low'],
                'text': f"{current['low']:.2f}",
                'style': 'label_up',
                'color': 'red'
            }
            self.state.down_patient.low_label_id = low_label_id
            
            # Update the latest confirmed LL
            self.state.latest_confirmed_ll = self.state.lower_low.price
            self.state.latest_confirmed_ll_bar = self.state.lower_low.bar_index
        
        # If there's already a patient candle, update/roll it
        else:
            # Update the patient candle (roll it)
            self.state.down_patient.high = current['high']
            self.state.down_patient.low = current['low']
            self.state.down_patient.bar_index = bar_index
            
            # Update label positions
            if self.state.down_patient.label_id in self.state.labels:
                self.state.labels[self.state.down_patient.label_id]['x'] = bar_index
                self.state.labels[self.state.down_patient.label_id]['y'] = current['low']
            
            if self.state.down_patient.low_label_id in self.state.labels:
                self.state.labels[self.state.down_patient.low_label_id]['x'] = bar_index
                self.state.labels[self.state.down_patient.low_label_id]['y'] = current['low']
                self.state.labels[self.state.down_patient.low_label_id]['text'] = f"{current['low']:.2f}"
            
            # Track the highest point since LL formed
            if (self.state.highest_since_ll is not None and 
                current['high'] > self.state.highest_since_ll):
                self.state.highest_since_ll = current['high']
                self.state.highest_since_ll_bar_index = bar_index
            
            # If a candidateLH already exists and current candle's high is higher, update candidateLH
            if (self.state.candidate_lh is not None and 
                current['high'] > self.state.candidate_lh.price and 
                not self.state.candidate_lh.confirmed):
                self.state.candidate_lh.price = current['high']
                self.state.candidate_lh.creation_bar = bar_index
                self.state.candidate_lh.bar_index = bar_index
                
                # Update label
                if self.state.candidate_lh.label_id in self.state.labels:
                    self.state.labels[self.state.candidate_lh.label_id]['x'] = bar_index
                    self.state.labels[self.state.candidate_lh.label_id]['y'] = current['high']
                    next_cycle = self.state.down_swing_cycle + 1
                    self.state.labels[self.state.candidate_lh.label_id]['text'] = f"Cand LH{next_cycle}"
    
    def _handle_downtrend_patient_break(self, bar_index: int, current: pd.Series):
        """Handle patient candle break in downtrend."""
        # Clear patient candle state regardless of anything else
        if self.state.down_patient is not None:
            # The patient candle is broken
            # Form candidateLH as the higher of the patient candle's high and current candle's high
            new_break_high = max(self.state.down_patient.high, current['high'])
            
            # Update highest since LL if needed
            if (self.state.highest_since_ll is not None and 
                new_break_high > self.state.highest_since_ll):
                self.state.highest_since_ll = new_break_high
                self.state.highest_since_ll_bar_index = bar_index
            
            # Only update candidateLH if not confirmed yet
            if self.state.candidate_lh is None or not self.state.candidate_lh.confirmed:
                # If no candidate exists, create one
                if self.state.candidate_lh is None:
                    self.state.candidate_lh = CandidateSwing(
                        price=new_break_high,
                        bar_index=bar_index,
                        creation_bar=bar_index,
                        confirmed=False
                    )
                    
                    # Add candidate label
                    cand_lh_id = f"cand_lh_{bar_index}"
                    next_cycle = self.state.down_swing_cycle + 1
                    self.state.labels[cand_lh_id] = {
                        'x': bar_index,
                        'y': new_break_high,
                        'text': f"Cand LH{next_cycle}",
                        'style': 'label_down',
                        'color': 'cyan'
                    }
                    self.state.candidate_lh.label_id = cand_lh_id
                # Otherwise update existing one
                else:
                    self.state.candidate_lh.price = max(self.state.candidate_lh.price, new_break_high)
                    self.state.candidate_lh.bar_index = bar_index
                    self.state.candidate_lh.creation_bar = bar_index
                    
                    # Update label
                    if self.state.candidate_lh.label_id in self.state.labels:
                        self.state.labels[self.state.candidate_lh.label_id]['x'] = bar_index
                        self.state.labels[self.state.candidate_lh.label_id]['y'] = self.state.candidate_lh.price
                        next_cycle = self.state.down_swing_cycle + 1
                        self.state.labels[self.state.candidate_lh.label_id]['text'] = f"Cand LH{next_cycle}"
            
            # Clear patient candle state
            # Remove patient candle labels
            if self.state.down_patient.label_id in self.state.labels:
                del self.state.labels[self.state.down_patient.label_id]
            
            if self.state.down_patient.low_label_id in self.state.labels:
                del self.state.labels[self.state.down_patient.low_label_id]
            
            self.state.down_patient = None
        
        # If we have lower_low but no patient candle, track highest points
        elif self.state.lower_low is not None:
            # Track the highest point since LL formed (even if not in patient candle mode)
            if (self.state.highest_since_ll is not None and 
                current['high'] > self.state.highest_since_ll):
                self.state.highest_since_ll = current['high']
                self.state.highest_since_ll_bar_index = bar_index
                
                # Update candidate LH label if it exists and not confirmed
                if self.state.candidate_lh is not None and not self.state.candidate_lh.confirmed:
                    # Update existing label
                    if self.state.candidate_lh.label_id in self.state.labels:
                        self.state.labels[self.state.candidate_lh.label_id]['x'] = bar_index
                        self.state.labels[self.state.candidate_lh.label_id]['y'] = self.state.highest_since_ll
                        next_cycle = self.state.down_swing_cycle + 1
                        self.state.labels[self.state.candidate_lh.label_id]['text'] = f"Cand LH{next_cycle}"
                    # Create new label
                    else:
                        cand_lh_id = f"cand_lh_{bar_index}"
                        next_cycle = self.state.down_swing_cycle + 1
                        self.state.labels[cand_lh_id] = {
                            'x': bar_index,
                            'y': self.state.highest_since_ll,
                            'text': f"Cand LH{next_cycle}",
                            'style': 'label_down',
                            'color': 'cyan'
                        }
                        self.state.candidate_lh = CandidateSwing(
                            price=self.state.highest_since_ll,
                            bar_index=bar_index,
                            creation_bar=bar_index,
                            confirmed=False,
                            label_id=cand_lh_id
                        )
    
    def _confirm_downtrend_swing(self, bar_index: int):
        """Confirm candidate LH as new swing high."""
        self.state.down_swing_cycle += 1
        
        # Use the highest point since LL formed instead of just candidateLH
        if self.state.highest_since_ll is not None:
            confirmed_lh_price = self.state.highest_since_ll
            confirmed_lh_bar = self.state.highest_since_ll_bar_index
            
            # Add LH label at the bar where the highest point occurred
            lh_label_id = f"lh_{confirmed_lh_bar}"
            self.state.labels[lh_label_id] = {
                'x': confirmed_lh_bar,
                'y': confirmed_lh_price,
                'text': f"LH{self.state.down_swing_cycle}",
                'style': 'label_down',
                'color': 'red'
            }
            
            # Update confirmed LH
            self.state.confirmed_lh = SwingPoint(
                price=confirmed_lh_price,
                bar_index=confirmed_lh_bar,
                cycle=self.state.down_swing_cycle,
                label_id=lh_label_id
            )
            
        elif self.state.candidate_lh is not None:
            confirmed_lh_price = self.state.candidate_lh.price
            confirmed_lh_bar = self.state.candidate_lh.bar_index
            
            # Add LH label at the current bar
            lh_label_id = f"lh_{bar_index}"
            self.state.labels[lh_label_id] = {
                'x': confirmed_lh_bar,
                'y': confirmed_lh_price,
                'text': f"LH{self.state.down_swing_cycle}",
                'style': 'label_down',
                'color': 'red'
            }
            
            # Update confirmed LH
            self.state.confirmed_lh = SwingPoint(
                price=confirmed_lh_price, 
                bar_index=confirmed_lh_bar,
                cycle=self.state.down_swing_cycle,
                label_id=lh_label_id
            )
        
        # Mark candidateLH as confirmed but don't delete it yet
        if self.state.candidate_lh is not None:
            self.state.candidate_lh.confirmed = True
        
        # Update the latest confirmed LL before clearing lower_low
        self.state.latest_confirmed_ll = self.state.lower_low.price
        self.state.latest_confirmed_ll_bar = self.state.lower_low.bar_index
        
        # Clear the lower_low variable to allow a new one to form
        self.state.lower_low = None
        
        # Reset highest tracking for next cycle
        self.state.highest_since_ll = None
        self.state.highest_since_ll_bar_index = None
    
    def _handle_uptrend_break(self, bar_index: int, current: pd.Series):
        """Handle uptrend break logic."""
        self.state.up_trend_broken = True
        self.state.in_uptrend = False
        self.state.hod_broken = False
        self.state.up_trend_break_bar = bar_index
        
        # Add trend break label
        trend_break_id = f"up_trend_break_{bar_index}"
        self.state.labels[trend_break_id] = {
            'x': bar_index,
            'y': current['low'],
            'text': "TREND BREAK",
            'style': 'label_down',
            'color': 'purple'
        }
        
        # Reset trend state to neutral only if uptrend is dominant or not in dual mode
        if (not self.state.is_dual_trend_active or 
            not self.config.preserve_dominant_trend or 
            self.state.dominant_trend == TrendState.UPTREND):
            self.state.trend_state = TrendState.NEUTRAL
            self.state.dominant_trend = TrendState.NEUTRAL
        
        # Always use day high as reference
        self.state.high_at_trend_break = self.state.day_high
        
        # Clear patient candle state
        self.state.up_patient = None
        
        # Don't delete candidateHL immediately, just mark it as confirmed
        if self.state.candidate_hl is not None and not self.state.candidate_hl.confirmed:
            self.state.candidate_hl.confirmed = True
        
        self.state.lowest_since_hh = None
        self.state.lowest_since_hh_bar_index = None
    
    def _handle_hod_break(self, bar_index: int, current: pd.Series, previous: pd.Series):
        """Handle HOD break after trend break."""
        self.state.hod_broken = True
        
        # Add HOD break label
        hod_break_id = f"hod_break_{bar_index}"
        self.state.labels[hod_break_id] = {
            'x': bar_index,
            'y': current['high'],
            'text': "HOD BREAK",
            'style': 'label_down',
            'color': 'blue'
        }
        
        # Immediately start a new trend when HOD is broken
        self.state.in_uptrend = True
        self.state.up_trend_broken = False
        self.state.hod_broken = False
        self.state.up_swing_cycle = 1
        
        # HL1 is defined as the lower of current and previous candle's low
        hl_price = min(current['low'], previous['low'])
        self.state.confirmed_hl = SwingPoint(
            price=hl_price,
            bar_index=bar_index,
            cycle=self.state.up_swing_cycle
        )
        
        # Reset all other variables to start fresh
        self.state.higher_high = None
        self.state.up_patient = None
        self.state.candidate_hl = None
        self.state.lowest_since_hh = None
        self.state.lowest_since_hh_bar_index = None
        
        # Always set trendState when HOD is broken
        self.state.trend_state = TrendState.UPTREND
        # Always update dominantTrend when HOD is broken
        self.state.dominant_trend = TrendState.UPTREND
        
        # Clear trend break label
        if f"up_trend_break_{self.state.up_trend_break_bar}" in self.state.labels:
            del self.state.labels[f"up_trend_break_{self.state.up_trend_break_bar}"]
        
        if self.config.debug_mode:
            debug_text = f"HOD: {self.state.high_at_trend_break}\nCurrent: {current['high']}"
            logger.debug(debug_text)
    
    def _start_new_uptrend(self, bar_index: int, current: pd.Series, previous: pd.Series):
        """Start a new uptrend."""
        self.state.in_uptrend = True
        self.state.up_trend_broken = False
        self.state.up_swing_cycle = 1
        
        # HL1 is defined as the lower of the breaker's low and previous candle's low
        hl_price = min(current['low'], previous['low'])
        self.state.confirmed_hl = SwingPoint(
            price=hl_price,
            bar_index=bar_index,
            cycle=self.state.up_swing_cycle
        )
        
        # Reset patient and candidate variables
        self.state.up_patient = None
        self.state.candidate_hl = None
        self.state.higher_high = None
        self.state.lowest_since_hh = None
        self.state.lowest_since_hh_bar_index = None
    
    def _handle_uptrend_patient_formation(self, bar_index: int, current: pd.Series, previous: pd.Series):
        """Handle patient candle formation in uptrend."""
        # If no previous patient candle, this is the first one
        if self.state.up_patient is None:
            # Initialize higher_high as the previous candle's high (but only if not already defined)
            if self.state.higher_high is None:
                hh_price = previous['high']
                self.state.higher_high = SwingPoint(
                    price=hh_price,
                    bar_index=bar_index - 1,
                    cycle=self.state.up_swing_cycle
                )
                
                # Add HH label
                hh_label_id = f"hh_{bar_index-1}"
                self.state.labels[hh_label_id] = {
                    'x': bar_index - 1,
                    'y': hh_price,
                    'text': f"HH{self.state.up_swing_cycle} {hh_price:.2f}",
                    'style': 'label_down',
                    'color': 'red'
                }
                self.state.higher_high.label_id = hh_label_id
            
            # Initialize lowest point tracking (if not already tracking)
            if self.state.lowest_since_hh is None:
                self.state.lowest_since_hh = current['low']
                self.state.lowest_since_hh_bar_index = bar_index
            
            # Set up this patient candle
            self.state.up_patient = PatientCandle(
                high=current['high'],
                low=current['low'],
                bar_index=bar_index
            )
            
            # Add patient candle label
            pc_label_id = f"up_patient_{bar_index}"
            pc_text = "UT Patient" if self.state.is_dual_trend_active else "Patient"
            self.state.labels[pc_label_id] = {
                'x': bar_index,
                'y': current['high'],
                'text': pc_text,
                'style': 'label_down',
                'color': 'orange' if not self.state.is_dual_trend_active else 'green'
            }
            self.state.up_patient.label_id = pc_label_id
            
            # Add high point label
            high_label_id = f"ut_patient_high_{bar_index}"
            self.state.labels[high_label_id] = {
                'x': bar_index,
                'y': current['high'],
                'text': f"{current['high']:.2f}",
                'style': 'label_down',
                'color': 'green'
            }
            self.state.up_patient.high_label_id = high_label_id
        
        # If there's already a patient candle, update/roll it
        else:
            # Update the patient candle (roll it)
            self.state.up_patient.high = current['high']
            self.state.up_patient.low = current['low']
            self.state.up_patient.bar_index = bar_index
            
            # Update label positions
            if self.state.up_patient.label_id in self.state.labels:
                self.state.labels[self.state.up_patient.label_id]['x'] = bar_index
                self.state.labels[self.state.up_patient.label_id]['y'] = current['high']
            
            if self.state.up_patient.high_label_id in self.state.labels:
                self.state.labels[self.state.up_patient.high_label_id]['x'] = bar_index
                self.state.labels[self.state.up_patient.high_label_id]['y'] = current['high']
                self.state.labels[self.state.up_patient.high_label_id]['text'] = f"{current['high']:.2f}"
            
            # Track the lowest point since HH formed
            if (self.state.lowest_since_hh is not None and 
                current['low'] < self.state.lowest_since_hh):
                self.state.lowest_since_hh = current['low']
                self.state.lowest_since_hh_bar_index = bar_index
            
            # If a candidateHL already exists and current candle's low is lower, update candidateHL
            if (self.state.candidate_hl is not None and 
                current['low'] < self.state.candidate_hl.price and 
                not self.state.candidate_hl.confirmed):
                self.state.candidate_hl.price = current['low']
                self.state.candidate_hl.creation_bar = bar_index
                self.state.candidate_hl.bar_index = bar_index
                
                # Update label
                if self.state.candidate_hl.label_id in self.state.labels:
                    self.state.labels[self.state.candidate_hl.label_id]['x'] = bar_index
                    self.state.labels[self.state.candidate_hl.label_id]['y'] = current['low']
                    next_cycle = self.state.up_swing_cycle + 1
                    self.state.labels[self.state.candidate_hl.label_id]['text'] = f"Cand HL{next_cycle}"
    
    def _handle_uptrend_patient_break(self, bar_index: int, current: pd.Series):
        """Handle patient candle break in uptrend."""
        # Clear patient candle state regardless of anything else
        if self.state.up_patient is not None:
            # The patient candle is broken
            # Form candidateHL as the lower of the patient candle's low and current candle's low
            new_break_low = min(self.state.up_patient.low, current['low'])
            
            # Update lowest since HH if needed
            if (self.state.lowest_since_hh is not None and 
                new_break_low < self.state.lowest_since_hh):
                self.state.lowest_since_hh = new_break_low
                self.state.lowest_since_hh_bar_index = bar_index
            
            # Only update candidateHL if not confirmed yet
            if self.state.candidate_hl is None or not self.state.candidate_hl.confirmed:
                # If no candidate exists, create one
                if self.state.candidate_hl is None:
                    self.state.candidate_hl = CandidateSwing(
                        price=new_break_low,
                        bar_index=bar_index,
                        creation_bar=bar_index,
                        confirmed=False
                    )
                    
                    # Add candidate label
                    cand_hl_id = f"cand_hl_{bar_index}"
                    next_cycle = self.state.up_swing_cycle + 1
                    self.state.labels[cand_hl_id] = {
                        'x': bar_index,
                        'y': new_break_low,
                        'text': f"Cand HL{next_cycle}",
                        'style': 'label_up',
                        'color': 'purple'
                    }
                    self.state.candidate_hl.label_id = cand_hl_id
                # Otherwise update existing one
                else:
                    self.state.candidate_hl.price = min(self.state.candidate_hl.price, new_break_low)
                    self.state.candidate_hl.bar_index = bar_index
                    self.state.candidate_hl.creation_bar = bar_index
                    
                    # Update label
                    if self.state.candidate_hl.label_id in self.state.labels:
                        self.state.labels[self.state.candidate_hl.label_id]['x'] = bar_index
                        self.state.labels[self.state.candidate_hl.label_id]['y'] = self.state.candidate_hl.price
                        next_cycle = self.state.up_swing_cycle + 1
                        self.state.labels[self.state.candidate_hl.label_id]['text'] = f"Cand HL{next_cycle}"
            
            # Clear patient candle state
            # Remove patient candle labels
            if self.state.up_patient.label_id in self.state.labels:
                del self.state.labels[self.state.up_patient.label_id]
            
            if self.state.up_patient.high_label_id in self.state.labels:
                del self.state.labels[self.state.up_patient.high_label_id]
            
            self.state.up_patient = None
        
        # If we have higher_high but no patient candle, track lowest points
        elif self.state.higher_high is not None:
            # Track the lowest point since HH formed (even if not in patient candle mode)
            if (self.state.lowest_since_hh is not None and 
                current['low'] < self.state.lowest_since_hh):
                self.state.lowest_since_hh = current['low']
                self.state.lowest_since_hh_bar_index = bar_index
                
                # Update candidate HL label if it exists and not confirmed
                if self.state.candidate_hl is not None and not self.state.candidate_hl.confirmed:
                    # Update existing label
                    if self.state.candidate_hl.label_id in self.state.labels:
                        self.state.labels[self.state.candidate_hl.label_id]['x'] = bar_index
                        self.state.labels[self.state.candidate_hl.label_id]['y'] = self.state.lowest_since_hh
                        next_cycle = self.state.up_swing_cycle + 1
                        self.state.labels[self.state.candidate_hl.label_id]['text'] = f"Cand HL{next_cycle}"
                    # Create new label
                    else:
                        cand_hl_id = f"cand_hl_{bar_index}"
                        next_cycle = self.state.up_swing_cycle + 1
                        self.state.labels[cand_hl_id] = {
                            'x': bar_index,
                            'y': self.state.lowest_since_hh,
                            'text': f"Cand HL{next_cycle}",
                            'style': 'label_up',
                            'color': 'purple'
                        }
                        self.state.candidate_hl = CandidateSwing(
                            price=self.state.lowest_since_hh,
                            bar_index=bar_index,
                            creation_bar=bar_index,
                            confirmed=False,
                            label_id=cand_hl_id
                        )
    
    def _confirm_uptrend_swing(self, bar_index: int):
        """Confirm candidate HL as new swing low."""
        self.state.up_swing_cycle += 1
        
        # Use the lowest point since HH formed instead of just candidateHL
        if self.state.lowest_since_hh is not None:
            confirmed_hl_price = self.state.lowest_since_hh
            confirmed_hl_bar = self.state.lowest_since_hh_bar_index
            
            # Add HL label at the bar where the lowest point occurred
            hl_label_id = f"hl_{confirmed_hl_bar}"
            self.state.labels[hl_label_id] = {
                'x': confirmed_hl_bar,
                'y': confirmed_hl_price,
                'text': f"HL{self.state.up_swing_cycle}",
                'style': 'label_up',
                'color': 'green'
            }
            
            # Update confirmed HL
            self.state.confirmed_hl = SwingPoint(
                price=confirmed_hl_price,
                bar_index=confirmed_hl_bar,
                cycle=self.state.up_swing_cycle,
                label_id=hl_label_id
            )
            
        elif self.state.candidate_hl is not None:
            confirmed_hl_price = self.state.candidate_hl.price
            confirmed_hl_bar = self.state.candidate_hl.bar_index
            
            # Add HL label at the current bar
            hl_label_id = f"hl_{bar_index}"
            self.state.labels[hl_label_id] = {
                'x': confirmed_hl_bar,
                'y': confirmed_hl_price,
                'text': f"HL{self.state.up_swing_cycle}",
                'style': 'label_up',
                'color': 'green'
            }
            
            # Update confirmed HL
            self.state.confirmed_hl = SwingPoint(
                price=confirmed_hl_price,
                bar_index=confirmed_hl_bar,
                cycle=self.state.up_swing_cycle,
                label_id=hl_label_id
            )
        
        # Mark candidateHL as confirmed but don't delete it yet
        if self.state.candidate_hl is not None:
            self.state.candidate_hl.confirmed = True
        
        # Clear the higher_high variable to allow a new one to form
        self.state.higher_high = None
        
        # Reset lowest tracking for next cycle
        self.state.lowest_since_hh = None
        self.state.lowest_since_hh_bar_index = None
    
    def _update_trend_state(self, current: pd.Series):
        """Update trend state based on HOD/LOD breaks and swings."""
        # Determine trend state based on HOD/LOD breaks and confirmed swings
        if self.config.use_hod_lod_breaks:
            # Prioritize HOD/LOD breaks
            if self.state.hod_broken:
                self.state.trend_state = TrendState.UPTREND
                self.state.dominant_trend = TrendState.UPTREND
            elif self.state.lod_broken:
                self.state.trend_state = TrendState.DOWNTREND
                self.state.dominant_trend = TrendState.DOWNTREND
        
        # Use confirmed or candidate swings for early trend detection
        if self.config.use_confirmed_swings and not (self.state.hod_broken or self.state.lod_broken):
            # UPTREND LOGIC
            if self.state.in_uptrend:
                # From existing downtrend (reversal scenario)
                if self.state.trend_state == TrendState.DOWNTREND:
                    # Need CHL3 break for reversal (upSwingCycle 2 means we're working on forming HL3/HH3)
                    if (self.state.up_swing_cycle >= self.config.cycles_required and 
                        self.state.candidate_hl is not None and
                        current['high'] > self.state.candidate_hl.price):
                        self.state.trend_state = TrendState.UPTREND
                        # Always update dominantTrend to match trendState on trend reversal
                        self.state.dominant_trend = TrendState.UPTREND
                # From neutral or same trend continuation
                else:
                    # Need CHL2 break
                    cycles_needed = max(1, self.config.cycles_required - 2)
                    if (self.state.up_swing_cycle >= cycles_needed and 
                        self.state.candidate_hl is not None and
                        current['high'] > self.state.candidate_hl.price):
                        self.state.trend_state = TrendState.UPTREND
                        # Always update dominantTrend to match trendState
                        self.state.dominant_trend = TrendState.UPTREND
                    # Fallback to cycle check
                    elif (self.state.up_swing_cycle >= self.config.cycles_required and 
                          self.state.candidate_hl is not None and
                          current['high'] > self.state.candidate_hl.price):
                        self.state.trend_state = TrendState.UPTREND
                        # Always update dominantTrend to match trendState
                        self.state.dominant_trend = TrendState.UPTREND
            
            # DOWNTREND LOGIC
            if self.state.in_downtrend:
                # From existing uptrend (reversal scenario)
                if self.state.trend_state == TrendState.UPTREND:
                    # Need CLH3 break for reversal (downSwingCycle 2 means we're working on forming LH3/LL3)
                    if (self.state.down_swing_cycle >= self.config.cycles_required and 
                        self.state.candidate_lh is not None and
                        current['low'] < self.state.candidate_lh.price):
                        self.state.trend_state = TrendState.DOWNTREND
                        # Always update dominantTrend to match trendState on trend reversal
                        self.state.dominant_trend = TrendState.DOWNTREND
                # From neutral or same trend continuation
                else:
                    # Need CLH2 break
                    cycles_needed = max(1, self.config.cycles_required - 2)
                    if (self.state.down_swing_cycle >= cycles_needed and 
                        self.state.candidate_lh is not None and
                        current['low'] < self.state.candidate_lh.price):
                        self.state.trend_state = TrendState.DOWNTREND
                        # Always update dominantTrend to match trendState
                        self.state.dominant_trend = TrendState.DOWNTREND
                    # Fallback to cycle check
                    elif (self.state.down_swing_cycle >= self.config.cycles_required and 
                          self.state.candidate_lh is not None and
                          current['low'] < self.state.candidate_lh.price):
                        self.state.trend_state = TrendState.DOWNTREND
                        # Always update dominantTrend to match trendState
                        self.state.dominant_trend = TrendState.DOWNTREND
        
        # Special handling for dual trend situations
        # Only reset to neutral if no trend is detected at all
        if (not self.state.in_uptrend and 
            not self.state.in_downtrend and 
            not self.state.up_trend_broken and 
            not self.state.down_trend_broken):
            self.state.trend_state = TrendState.NEUTRAL
            self.state.dominant_trend = TrendState.NEUTRAL
        
        # Properly sync dominantTrend with trendState
        # This ensures dominantTrend always reflects the current trend state
        if self.state.trend_state != TrendState.NEUTRAL:
            self.state.dominant_trend = self.state.trend_state
        
        # Handle dual trend special case - when a non-dominant trend is broken
        if self.state.is_dual_trend_active and self.config.preserve_dominant_trend:
            # If one trend is broken but the other is still active, maintain the trend state
            # of the dominant trend
            if (self.state.up_trend_broken and 
                not self.state.down_trend_broken and 
                self.state.dominant_trend == TrendState.DOWNTREND):
                self.state.trend_state = TrendState.DOWNTREND
            elif (self.state.down_trend_broken and 
                  not self.state.up_trend_broken and 
                  self.state.dominant_trend == TrendState.UPTREND):
                self.state.trend_state = TrendState.UPTREND
    
    def _collect_labels(self) -> List[Dict]:
        """Collect all labels for visualization."""
        result = []
        for label_id, label_data in self.state.labels.items():
            result.append({
                'id': label_id,
                'x': label_data['x'],
                'y': label_data['y'],
                'text': label_data['text'],
                'style': label_data.get('style', 'label_none'),
                'color': label_data.get('color', 'white')
            })
        return result

    def plot_on_chart(self, ax, data_df, labels=None):
        """
        Plot the indicator on a matplotlib chart.
        
        Args:
            ax: Matplotlib axes object
            data_df: DataFrame with indicator data
            labels: Optional list of labels to display
        """
        # Plot OHLC data
        # ...
        
        # Plot trend state indicators
        trend_changes_up = []
        trend_changes_down = []
        
        for i in range(1, len(data_df)):
            if data_df['trend_state'].iloc[i] == 1 and data_df['trend_state'].iloc[i-1] != 1:
                trend_changes_up.append(i)
            elif data_df['trend_state'].iloc[i] == -1 and data_df['trend_state'].iloc[i-1] != -1:
                trend_changes_down.append(i)
        
        # Plot uptrend start indicators
        for idx in trend_changes_up:
            ax.scatter(idx, data_df['low'].iloc[idx] * 0.997, marker='^', color='green', s=100)
            ax.text(idx, data_df['low'].iloc[idx] * 0.994, "UPTREND START", 
                    color='green', fontsize=8, ha='center')
        
        # Plot downtrend start indicators
        for idx in trend_changes_down:
            ax.scatter(idx, data_df['high'].iloc[idx] * 1.003, marker='v', color='red', s=100)
            ax.text(idx, data_df['high'].iloc[idx] * 1.006, "DOWNTREND START", 
                    color='red', fontsize=8, ha='center')
        
        # Plot labels if provided
        if labels:
            for label in labels:
                x = label['x']
                y = label['y']
                text = label['text']
                color = label['color']
                style = label['style']
                
                # Adjust text position based on style
                if style == 'label_up':
                    y_offset = 0.997
                    va = 'top'
                else:  # label_down
                    y_offset = 1.003
                    va = 'bottom'
                
                ax.text(x, y * y_offset, text, color=color, fontsize=8, ha='center', va=va)
        
        return ax


class PatientLabelsVisualizer:
    """Class for visualizing the Patient Labels indicator."""
    
    @staticmethod
    def create_visualization(data_df, indicator_result):
        """
        Create a visualization of the Patient Labels indicator.
        
        Args:
            data_df: Original DataFrame with OHLCV data
            indicator_result: DataFrame with indicator results
            
        Returns:
            matplotlib figure
        """
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from matplotlib.patches import Rectangle
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=(14, 7))
        
        # Plot candlestick chart
        for i in range(len(data_df)):
            # Get current candle data
            date = data_df.index[i]
            open_price = data_df['open'].iloc[i]
            high_price = data_df['high'].iloc[i]
            low_price = data_df['low'].iloc[i]
            close_price = data_df['close'].iloc[i]
            
            # Determine candle color
            color = 'green' if close_price >= open_price else 'red'
            
            # Plot candle body
            body_height = abs(close_price - open_price)
            body_bottom = min(open_price, close_price)
            rect = Rectangle((i - 0.4, body_bottom), 0.8, body_height, color=color, alpha=0.5)
            ax.add_patch(rect)
            
            # Plot wicks
            ax.plot([i, i], [low_price, body_bottom], color='black', linewidth=1)
            ax.plot([i, i], [body_bottom + body_height, high_price], color='black', linewidth=1)
        
        # Plot labels
        if 'labels' in indicator_result.attrs:
            for label in indicator_result.attrs['labels']:
                x = label['x']
                y = label['y']
                text = label['text']
                color = label.get('color', 'white')
                style = label.get('style', 'label_none')
                
                # Determine text position and alignment based on style
                if style == 'label_up':
                    y_offset = -0.003
                    va = 'top'
                elif style == 'label_down':
                    y_offset = 0.003
                    va = 'bottom'
                else:
                    y_offset = 0
                    va = 'center'
                
                ax.text(x, y * (1 + y_offset), text, color=color, fontsize=8, 
                        ha='center', va=va, bbox=dict(facecolor='black', alpha=0.4))
        
        # Plot trend state changes
        trend_states = indicator_result['trend_state']
        for i in range(1, len(trend_states)):
            if trend_states.iloc[i] == 1 and trend_states.iloc[i-1] != 1:
                ax.scatter(i, data_df['low'].iloc[i] * 0.99, marker='^', color='green', s=100)
                ax.text(i, data_df['low'].iloc[i] * 0.98, "UPTREND", color='green', fontsize=10, ha='center')
            elif trend_states.iloc[i] == -1 and trend_states.iloc[i-1] != -1:
                ax.scatter(i, data_df['high'].iloc[i] * 1.01, marker='v', color='red', s=100)
                ax.text(i, data_df['high'].iloc[i] * 1.02, "DOWNTREND", color='red', fontsize=10, ha='center')
        
        # Format x-axis
        ax.set_xlim(-1, len(data_df))
        ax.set_xticks(range(0, len(data_df), max(1, len(data_df) // 10)))
        
        if isinstance(data_df.index[0], (pd.Timestamp, pd.DatetimeIndex)):
            ax.set_xticklabels([date.strftime('%Y-%m-%d %H:%M') for date in data_df.index[::max(1, len(data_df) // 10)]], rotation=45)
        
        # Format y-axis
        ax.set_ylabel('Price')
        
        # Add title
        ax.set_title('Patient Labels Indicator')
        
        # Add grid
        ax.grid(alpha=0.3)
        
        # Adjust layout
        plt.tight_layout()
        
        return fig


def run_patient_labels_indicator(data, config=None):
    """
    Run the Patient Labels indicator on the provided data.
    
    Args:
        data: DataFrame with OHLCV data 
             Required columns: ['open', 'high', 'low', 'close', 'datetime']
        config: Optional configuration parameters
        
    Returns:
        DataFrame with indicator results
    """
    # Initialize the indicator
    config = config or PatientLabelsConfig()
    indicator = PatientLabelsIndicator(config)
    
    # Process the data
    result = indicator.process_data(data)
    
    return result


def visualize_patient_labels(data, indicator_result):
    """
    Create a visualization of the Patient Labels indicator.
    
    Args:
        data: Original DataFrame with OHLCV data
        indicator_result: DataFrame with indicator results
        
    Returns:
        matplotlib figure
    """
    viz = PatientLabelsVisualizer()
    fig = viz.create_visualization(data, indicator_result)
    return fig


def main():
    """Main function to demonstrate the Patient Labels indicator."""
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    from datetime import datetime, timedelta
    
    # Generate sample data
    np.random.seed(42)
    num_bars = 100
    
    # Create datetime index
    dates = [datetime(2024, 1, 1) + timedelta(hours=i) for i in range(num_bars)]
    
    # Generate random price data with trend
    base_price = 100
    volatility = 1.0
    
    # Create price series with some trending behavior
    random_walk = np.random.normal(0, volatility, num_bars)
    
    # Add some trending sections
    trend_sections = [
        (20, 40, 0.2),  # Uptrend
        (50, 70, -0.3),  # Downtrend
        (80, 100, 0.25)  # Uptrend
    ]
    
    for start, end, trend in trend_sections:
        for i in range(start, min(end, num_bars)):
            random_walk[i] += trend
    
    # Calculate cumulative price
    price_series = base_price + np.cumsum(random_walk)
    
    # Create OHLCV data
    data = pd.DataFrame(index=dates)
    data['open'] = price_series
    
    # Generate high, low, close
    for i in range(num_bars):
        if i > 0:
            data.iloc[i, data.columns.get_loc('open')] = data.iloc[i-1, data.columns.get_loc('close')]
            
        day_volatility = volatility * np.random.uniform(0.5, 1.5)
        day_change = np.random.normal(0, day_volatility)
        
        close = data.iloc[i, data.columns.get_loc('open')] + day_change
        
        # Random high and low with some constraints
        high_low_range = abs(day_change) * np.random.uniform(1.5, 3.0)
        
        if day_change > 0:
            # Upday
            high = close + high_low_range * np.random.uniform(0.3, 0.7)
            low = data.iloc[i, data.columns.get_loc('open')] - high_low_range * np.random.uniform(0.1, 0.5)
        else:
            # Downday
            high = data.iloc[i, data.columns.get_loc('open')] + high_low_range * np.random.uniform(0.1, 0.5)
            low = close - high_low_range * np.random.uniform(0.3, 0.7)
        
        data.loc[data.index[i], 'high'] = high
        data.loc[data.index[i], 'low'] = low
        data.loc[data.index[i], 'close'] = close
    
    data['volume'] = np.random.randint(1000, 10000, num_bars)
    data['datetime'] = data.index
    
    # Run the indicator
    config = PatientLabelsConfig(
        show_downtrend=True,
        show_uptrend=True,
        show_trend_state=True,
        use_hod_lod_breaks=True,
        use_confirmed_swings=True,
        cycles_required=2,
        debug_mode=False,
        display_duration=5,
        preserve_dominant_trend=True
    )
    
    result = run_patient_labels_indicator(data, config)
    
    # Visualize the results
    fig = visualize_patient_labels(data, result)
    plt.show()
    
    return result


if __name__ == "__main__":
    main()


# Example usage with real market data from CSV
def example_with_real_data(csv_file_path):
    """Example usage with real market data from CSV file."""
    try:
        # Load data from CSV
        data = pd.read_csv(csv_file_path)
        
        # Ensure required columns exist
        required_columns = ['datetime', 'open', 'high', 'low', 'close']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            # Try to map common alternative column names
            column_mapping = {
                'date': 'datetime',
                'time': 'datetime',
                'timestamp': 'datetime',
                'Date': 'datetime',
                'Time': 'datetime',
                'Timestamp': 'datetime',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close'
            }
            
            # Rename columns if possible
            for missing_col in missing_columns[:]:
                for alt_col, std_col in column_mapping.items():
                    if alt_col in data.columns and std_col == missing_col:
                        data = data.rename(columns={alt_col: std_col})
                        missing_columns.remove(missing_col)
                        break
        
        # Check if there are still missing columns
        if missing_columns:
            print(f"Error: Missing required columns: {missing_columns}")
            print(f"Available columns: {data.columns.tolist()}")
            return None
        
        # Convert datetime to pandas datetime
        if 'datetime' in data.columns:
            data['datetime'] = pd.to_datetime(data['datetime'])
        
        # Set datetime as index
        if not data.index.name == 'datetime':
            data = data.set_index('datetime')
        
        # Run the indicator
        config = PatientLabelsConfig()
        result = run_patient_labels_indicator(data.reset_index(), config)
        
        # Visualize the results
        fig = visualize_patient_labels(data.reset_index(), result)
        plt.savefig('patient_labels_result.png')
        plt.show()
        
        return result
    
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        import traceback
        traceback.print_exc()
        return None


# Command-line interface
def cli():
    """Command-line interface for the Patient Labels indicator."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Patient Labels Indicator')
    parser.add_argument('--csv', type=str, help='Path to CSV file with OHLCV data')
    parser.add_argument('--show-uptrend', action='store_true', default=True, 
                        help='Show uptrend labels')
    parser.add_argument('--show-downtrend', action='store_true', default=True, 
                        help='Show downtrend labels')
    parser.add_argument('--show-trend-state', action='store_true', default=True, 
                        help='Show trend state labels')
    parser.add_argument('--use-hod-lod-breaks', action='store_true', default=True, 
                        help='Use HOD/LOD breaks for trend detection')
    parser.add_argument('--use-confirmed-swings', action='store_true', default=True, 
                        help='Use confirmed swings for trend detection')
    parser.add_argument('--cycles-required', type=int, default=2, 
                        help='Cycles required for trend change')
    parser.add_argument('--debug-mode', action='store_true', default=False, 
                        help='Enable debug mode')
    parser.add_argument('--display-duration', type=int, default=5, 
                        help='Candidate level display duration (bars)')
    parser.add_argument('--preserve-dominant-trend', action='store_true', default=True, 
                        help='Preserve dominant trend in dual mode')
    parser.add_argument('--output', type=str, default='patient_labels_output.csv', 
                        help='Output CSV file path')
    parser.add_argument('--plot', action='store_true', default=True, 
                        help='Plot results')
    
    args = parser.parse_args()
    
    if args.csv:
        # Create config from arguments
        config = PatientLabelsConfig(
            show_uptrend=args.show_uptrend,
            show_downtrend=args.show_downtrend,
            show_trend_state=args.show_trend_state,
            use_hod_lod_breaks=args.use_hod_lod_breaks,
            use_confirmed_swings=args.use_confirmed_swings,
            cycles_required=args.cycles_required,
            debug_mode=args.debug_mode,
            display_duration=args.display_duration,
            preserve_dominant_trend=args.preserve_dominant_trend
        )
        
        # Load data
        data = pd.read_csv(args.csv)
        
        # Check for required columns
        required_columns = ['datetime', 'open', 'high', 'low', 'close']
        for col in required_columns:
            if col not in data.columns:
                print(f"Error: Required column '{col}' missing from data")
                return
        
        # Convert datetime and set as index
        data['datetime'] = pd.to_datetime(data['datetime'])
        
        # Run the indicator
        result = run_patient_labels_indicator(data, config)
        
        # Save results to CSV
        result.to_csv(args.output)
        print(f"Results saved to {args.output}")
        
        # Plot if requested
        if args.plot:
            fig = visualize_patient_labels(data, result)
            plt.savefig('patient_labels_plot.png')
            print("Plot saved to patient_labels_plot.png")
            plt.show()
    else:
        # Run with sample data
        main()


if __name__ == "__main__":
    # Uncomment one of these lines to run:
    main()  # Run with synthetic data
    # cli()  # Run command-line interface
    # example_with_real_data('your_market_data.csv')  # Run with real data

        