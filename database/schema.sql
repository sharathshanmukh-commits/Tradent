-- Patient Labels Trading System Database Schema
-- PostgreSQL + TimescaleDB for real-time trading data persistence
-- Run this script after creating the database and user

-- Connect to your database first:
-- psql -h localhost -U tradent_user -d tradent_live

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 1. Market Data Table (replaces processed_data.csv)
CREATE TABLE market_data (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    open DECIMAL(10,4) NOT NULL,
    high DECIMAL(10,4) NOT NULL,
    low DECIMAL(10,4) NOT NULL,
    close DECIMAL(10,4) NOT NULL,
    volume BIGINT NOT NULL,
    source TEXT NOT NULL,
    timestamp_received TIMESTAMPTZ NOT NULL,
    session_id TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Strategy Results Table (replaces strategy_results.csv)
CREATE TABLE strategy_results (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    open DECIMAL(10,4),
    high DECIMAL(10,4),
    low DECIMAL(10,4),
    close DECIMAL(10,4),
    volume BIGINT,
    
    -- Patient Labels Indicator outputs
    trend_state INTEGER,
    up_patient_high DECIMAL(10,4),
    down_patient_low DECIMAL(10,4),
    day_high DECIMAL(10,4),
    day_low DECIMAL(10,4),
    is_dual_trend_active BOOLEAN DEFAULT FALSE,
    in_uptrend BOOLEAN DEFAULT FALSE,
    up_swing_cycle INTEGER DEFAULT 0,
    in_downtrend BOOLEAN DEFAULT FALSE,
    down_swing_cycle INTEGER DEFAULT 0,
    dominant_trend INTEGER DEFAULT 0,
    
    -- Signal columns
    signal INTEGER DEFAULT 0,
    entry_price DECIMAL(10,4),
    stop_loss DECIMAL(10,4),
    target_price DECIMAL(10,4),
    
    session_id TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Trading Signals Table (replaces signals.csv)
CREATE TABLE trading_signals (
    id BIGSERIAL PRIMARY KEY,
    signal_id TEXT UNIQUE NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    datetime_signal TIMESTAMPTZ NOT NULL,
    bar_index INTEGER NOT NULL,
    signal_type TEXT NOT NULL,
    entry_price DECIMAL(10,4) NOT NULL,
    stop_loss DECIMAL(10,4) NOT NULL,
    target_price DECIMAL(10,4) NOT NULL,
    risk DECIMAL(10,4),
    risk_reward_ratio DECIMAL(5,2),
    reason TEXT,
    
    -- Market context snapshot
    market_snapshot JSONB,
    
    session_id TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. System Events Table (replaces events.log)
CREATE TABLE system_events (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    event_data JSONB NOT NULL,
    session_id TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 5. Trading Sessions Table (replaces session_info.json)
CREATE TABLE trading_sessions (
    session_id TEXT PRIMARY KEY,
    session_start TIMESTAMPTZ NOT NULL,
    session_end TIMESTAMPTZ,
    duration_minutes DECIMAL(10,2),
    data_source TEXT NOT NULL,
    configuration JSONB NOT NULL,
    bars_processed INTEGER DEFAULT 0,
    signals_generated INTEGER DEFAULT 0,
    output_directory TEXT,
    status TEXT DEFAULT 'RUNNING',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create TimescaleDB hypertables for time-series optimization
SELECT create_hypertable('market_data', 'timestamp');
SELECT create_hypertable('strategy_results', 'timestamp');
SELECT create_hypertable('trading_signals', 'timestamp');
SELECT create_hypertable('system_events', 'timestamp');

-- Create indexes for better query performance
CREATE INDEX idx_market_data_symbol_time ON market_data (symbol, timestamp DESC);
CREATE INDEX idx_strategy_results_session ON strategy_results (session_id, timestamp DESC);
CREATE INDEX idx_trading_signals_session ON trading_signals (session_id, timestamp DESC);
CREATE INDEX idx_trading_signals_type ON trading_signals (signal_type, timestamp DESC);
CREATE INDEX idx_system_events_type ON system_events (event_type, timestamp DESC);

-- Add constraints
ALTER TABLE trading_signals ADD CONSTRAINT chk_signal_type CHECK (signal_type IN ('buy', 'sell'));
ALTER TABLE trading_sessions ADD CONSTRAINT chk_status CHECK (status IN ('RUNNING', 'COMPLETED', 'ERROR'));

-- Create views for easy querying
CREATE VIEW v_latest_signals AS
SELECT 
    signal_id,
    timestamp,
    signal_type,
    entry_price,
    stop_loss,
    target_price,
    reason,
    session_id
FROM trading_signals
ORDER BY timestamp DESC
LIMIT 50;

CREATE VIEW v_session_summary AS
SELECT 
    session_id,
    session_start,
    session_end,
    duration_minutes,
    data_source,
    bars_processed,
    signals_generated,
    status
FROM trading_sessions
ORDER BY session_start DESC;

-- Sample queries for monitoring (comments for reference)
/*
-- Get recent signals
SELECT * FROM v_latest_signals;

-- Get current session stats
SELECT * FROM v_session_summary WHERE status = 'RUNNING';

-- Get signal performance by session
SELECT 
    session_id,
    COUNT(*) as total_signals,
    COUNT(CASE WHEN signal_type = 'buy' THEN 1 END) as buy_signals,
    COUNT(CASE WHEN signal_type = 'sell' THEN 1 END) as sell_signals,
    AVG(entry_price) as avg_entry_price,
    MIN(timestamp) as first_signal,
    MAX(timestamp) as last_signal
FROM trading_signals
GROUP BY session_id
ORDER BY first_signal DESC;

-- Get market data for a specific time range
SELECT * FROM market_data 
WHERE timestamp >= NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC;

-- Monitor system events
SELECT event_type, COUNT(*) as count
FROM system_events
WHERE timestamp >= NOW() - INTERVAL '1 hour'
GROUP BY event_type
ORDER BY count DESC;
*/