#!/bin/bash
# Database monitoring script for live trading system

echo "🗄️  DATABASE MONITORING DASHBOARD"
echo "================================="
echo

# Check current sessions
echo "📊 ACTIVE SESSIONS:"
psql -d tradent_live -c "
SELECT 
    session_id,
    session_start,
    bars_processed,
    signals_generated,
    status
FROM trading_sessions 
WHERE status = 'RUNNING'
ORDER BY session_start DESC;
"

echo
echo "🚨 RECENT SIGNALS (Last 10):"
psql -d tradent_live -c "
SELECT 
    signal_id,
    timestamp,
    signal_type,
    entry_price,
    stop_loss,
    target_price
FROM trading_signals 
ORDER BY timestamp DESC 
LIMIT 10;
"

echo
echo "📈 LATEST MARKET DATA (Last 5 bars):"
psql -d tradent_live -c "
SELECT 
    timestamp,
    symbol,
    close,
    volume,
    session_id
FROM market_data 
ORDER BY timestamp DESC 
LIMIT 5;
"

echo
echo "📋 SESSION SUMMARY:"
psql -d tradent_live -c "
SELECT 
    COUNT(DISTINCT session_id) as total_sessions,
    SUM(bars_processed) as total_bars,
    SUM(signals_generated) as total_signals
FROM trading_sessions;
"