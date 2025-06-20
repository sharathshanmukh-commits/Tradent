# strategies/patient_labels_strategy/signal_formatter.py

import pandas as pd
from typing import Dict, Any, Optional
from datetime import time

def format_signal_data(
    market_data: pd.DataFrame, 
    patient_labels_data: pd.DataFrame, 
    signal: Dict[str, Any],
    lookback_bars: Optional[int] = None,
    market_open_time: time = time(9, 30)
) -> str:
    """
    Format market data, patient labels data, and signal into a readable text format
    for analysis by an LLM or human review.
    
    Works with standardized data from DataLoader class.
    
    Args:
        market_data: DataFrame with OHLCV data from DataLoader
        patient_labels_data: DataFrame with Patient Labels indicator results
        signal: Dictionary containing signal details from PatientLabelsStrategy
        lookback_bars: Number of bars before the signal to include
                      If None, includes all bars from market open
        market_open_time: Market open time (default: 9:30 AM)
        
    Returns:
        Formatted text with all relevant context for signal analysis
    """
    # Get the signal bar index
    signal_idx = signal['bar_index']
    
    # Determine the start index based on lookback parameter
    if lookback_bars is None:
        # Get all data from the market open time
        signal_date = pd.to_datetime(market_data.iloc[signal_idx]['datetime']).date()
        market_open_datetime = pd.Timestamp.combine(signal_date, market_open_time)
        
        # Find the first bar after market open
        start_idx = market_data[market_data['datetime'] >= market_open_datetime].index[0]
    else:
        # Use specified lookback bars
        start_idx = max(0, signal_idx - lookback_bars)
    
    # Get data window up to and including signal bar
    end_idx = signal_idx + 1
    recent_market = market_data.iloc[start_idx:end_idx].copy()
    recent_pl_data = patient_labels_data.iloc[start_idx:end_idx].copy()
    
    # Add relative bar numbers for easier reference
    bar_range = range(-len(recent_market)+1, 1)
    recent_market['relative_bar'] = bar_range
    recent_pl_data['relative_bar'] = bar_range
    
    # === 1. Market Data Section ===
    market_section = "=== MARKET DATA ===\n"
    if lookback_bars is None:
        market_section += f"All bars from market open ({market_open_time.strftime('%H:%M')}) to signal\n\n"
    else:
        market_section += f"Last {lookback_bars} bars before signal\n\n"
    
    market_section += "Bars relative to signal (0 = signal bar, -1 = 1 bar before signal, etc.)\n\n"
    
    # Format OHLC data with timestamps
    pd.set_option('display.precision', 2)
    
    # Create header
    market_section += f"  {'Bar':3s}  |  {'Time':8s}  |  {'Open':7s}  |  {'High':7s}  |  {'Low':7s}  |  {'Close':7s}  |  {'Volume':8s}  |  {'Change%':8s}\n"
    market_section += "-" * 85 + "\n"
    
    # Add each row of data
    for i, row in recent_market.iterrows():
        # Calculate price change percentage
        close_change = 0
        if i > recent_market.index[0]:
            prev_close = recent_market.loc[recent_market.index[recent_market.index.get_loc(i)-1], 'close']
            close_change = ((row['close'] - prev_close) / prev_close) * 100
        
        # Format the time
        time_str = row['datetime'].strftime('%H:%M:%S')
        
        # Format the row
        market_section += f"  {row['relative_bar']:3d}  |  {time_str:8s}  |  {row['open']:7.2f}  |  {row['high']:7.2f}  |  "
        market_section += f"{row['low']:7.2f}  |  {row['close']:7.2f}  |  {row['volume']:8.0f}  |  {close_change:+7.2f}%\n"
    
    # === 2. Patient Labels Section ===
    pl_section = "\n=== PATIENT LABELS DATA ===\n\n"
    
    # Create a summary table of key indicator values
    pl_section += "Bar  |  Trend  |  Up/Down  |  Swing Cycles  |  Patient Candles\n"
    pl_section += "-----+---------+-----------+----------------+----------------\n"
    
    for i, row in recent_pl_data.iterrows():
        rel_bar = row['relative_bar']
        
        # Translate trend state to text
        trend_state = row['trend_state']
        trend_text = "NEUTRAL"
        if trend_state == 1:
            trend_text = "UPTREND"
        elif trend_state == -1:
            trend_text = "DOWNTREND"
        
        # Get uptrend/downtrend flags
        in_uptrend = str(row.get('in_uptrend', "N/A"))
        in_downtrend = str(row.get('in_downtrend', "N/A"))
        up_down = f"{in_uptrend}/{in_downtrend}"
        
        # Get swing cycles
        up_cycle = row.get('up_swing_cycle', 0)
        down_cycle = row.get('down_swing_cycle', 0)
        cycles = f"{up_cycle}/{down_cycle}"
        
        # Check for patient candles
        patient_text = ""
        if 'up_patient_high' in row and not pd.isna(row['up_patient_high']):
            patient_text += "UP "
        if 'down_patient_low' in row and not pd.isna(row['down_patient_low']):
            patient_text += "DOWN"
        if not patient_text:
            patient_text = "-"
        
        # Add the row
        pl_section += f"{rel_bar:3d}  |  {trend_text:7s} |  {up_down:9s} |  {cycles:14s} |  {patient_text}\n"
    
    # Add trend change information
    pl_section += "\nTrend Changes:\n"
    trend_changes = []
    for i in range(1, len(recent_pl_data)):
        if recent_pl_data.iloc[i]['trend_state'] != recent_pl_data.iloc[i-1]['trend_state']:
            from_state = recent_pl_data.iloc[i-1]['trend_state']
            to_state = recent_pl_data.iloc[i]['trend_state']
            rel_bar = recent_pl_data.iloc[i]['relative_bar']
            
            # Convert numeric states to text
            from_text = "NEUTRAL"
            if from_state == 1:
                from_text = "UPTREND"
            elif from_state == -1:
                from_text = "DOWNTREND"
                
            to_text = "NEUTRAL"
            if to_state == 1:
                to_text = "UPTREND"
            elif to_state == -1:
                to_text = "DOWNTREND"
                
            trend_changes.append(f"  Bar {rel_bar}: {from_text} → {to_text}")
    
    if trend_changes:
        pl_section += "\n".join(trend_changes) + "\n"
    else:
        pl_section += "  No trend changes in the analyzed period\n"
    
    # Add specific Patient Labels metrics from final bar
    last_bar = recent_pl_data.iloc[-1]
    pl_section += "\nFinal Patient Labels State:\n"
    
    # Add all relevant indicator data from the signal bar
    for col in last_bar.index:
        # Skip non-indicator columns
        if col in ['relative_bar', 'datetime', 'date', 'time', 'open', 'high', 'low', 'close', 'volume']:
            continue
            
        # Format the value
        value = last_bar[col]
        if pd.isna(value):
            value_str = "N/A"
        elif isinstance(value, (int, float)):
            value_str = f"{value:.4f}" if isinstance(value, float) else str(value)
        else:
            value_str = str(value)
            
        pl_section += f"  {col}: {value_str}\n"
    
    # === 3. Signal Section ===
    signal_section = "\n=== SIGNAL DETAILS ===\n\n"
    signal_section += f"Signal type: {signal['type'].upper()}\n"
    
    # Add timestamp if available
    if 'datetime' in signal:
        signal_time = pd.to_datetime(signal['datetime']).strftime('%Y-%m-%d %H:%M:%S')
        signal_section += f"Signal time: {signal_time}\n"
    else:
        # Use the datetime from market_data if signal doesn't have it
        signal_time = recent_market.iloc[-1]['datetime'].strftime('%Y-%m-%d %H:%M:%S')
        signal_section += f"Signal time: {signal_time}\n"
    
    signal_section += f"Reason: {signal['reason']}\n"
    signal_section += f"Entry price: {signal['entry_price']:.2f}\n"
    signal_section += f"Stop loss: {signal['stop_loss']:.2f}\n" 
    signal_section += f"Target price: {signal['target_price']:.2f}\n"
    signal_section += f"Risk/reward ratio: {signal['risk_reward_ratio']:.2f}\n"
    
    # Calculate additional metrics
    entry = signal['entry_price']
    stop = signal['stop_loss']
    target = signal['target_price']
    
    # Risk/reward in points and percentage
    risk_points = abs(entry - stop)
    risk_pct = (risk_points / entry) * 100
    reward_points = abs(target - entry)
    reward_pct = (reward_points / entry) * 100
    
    signal_section += f"Risk: {risk_points:.2f} points ({risk_pct:.2f}%)\n"
    signal_section += f"Reward: {reward_points:.2f} points ({reward_pct:.2f}%)\n"
    
    # Combine all sections
    formatted_data = market_section + pl_section + signal_section
    
    return formatted_data