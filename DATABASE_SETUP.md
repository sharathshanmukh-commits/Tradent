# Database Integration Setup Guide

## Overview

This guide will help you set up PostgreSQL + TimescaleDB for real-time persistence of your trading data, replacing the CSV-only approach with a robust database solution.

## What You Get

✅ **Real-time persistence** - All market data saved immediately  
✅ **Signal notifications** - Immediate alerts when signals are found  
✅ **Live monitoring** - Query data while system runs  
✅ **Data safety** - No data loss if system crashes  
✅ **Complex analysis** - SQL queries for pattern analysis  

## Installation Steps

### 1. Install PostgreSQL and TimescaleDB

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install postgresql postgresql-contrib

# Add TimescaleDB repository
sudo add-apt-repository ppa:timescale/timescaledb-ppa
sudo apt update
sudo apt install timescaledb-2-postgresql-14

# Configure TimescaleDB
sudo timescaledb-tune --quiet --yes
sudo systemctl restart postgresql
```

```bash
# macOS with Homebrew
brew install postgresql timescaledb

# Start PostgreSQL
brew services start postgresql

# Configure TimescaleDB
timescaledb-tune --quiet --yes
brew services restart postgresql
```

### 2. Create Database and User

```bash
# Connect as postgres user
sudo -u postgres psql

# Or on macOS
psql postgres
```

```sql
-- Create database
CREATE DATABASE tradent_live;

-- Create user with password
CREATE USER tradent_user WITH PASSWORD 'TradentSecure2024!';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE tradent_live TO tradent_user;

-- Exit
\q
```

### 3. Create Database Schema

```bash
# Connect to your new database
psql -h localhost -U tradent_user -d tradent_live

# Run the schema creation script
\i database/schema.sql

# Verify tables were created
\dt

# Exit
\q
```

### 4. Install Python Dependencies

```bash
pip install asyncpg psycopg2-binary
```

### 5. Configure Database Connection

Edit `database/db_config.json` if needed:

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "database": "tradent_live",
    "user": "tradent_user",
    "password": "TradentSecure2024!",
    "pool_min_size": 5,
    "pool_max_size": 20,
    "command_timeout": 60
  }
}
```

## Usage

### Run System with Database

```bash
# Run with database integration
python main_with_database.py

# Run for 30 minutes
python main_with_database.py --duration 30

# Use custom config
python main_with_database.py --config my_config.json
```

### Monitor Live Data

While the system is running, you can monitor it in real-time:

```bash
# Connect to database
psql -h localhost -U tradent_user -d tradent_live
```

```sql
-- View recent signals
SELECT * FROM v_latest_signals;

-- View current session stats
SELECT * FROM v_session_summary WHERE status = 'RUNNING';

-- Monitor real-time market data
SELECT symbol, timestamp, close, volume 
FROM market_data 
WHERE timestamp >= NOW() - INTERVAL '10 minutes'
ORDER BY timestamp DESC
LIMIT 20;

-- Watch for new signals (refresh this query)
SELECT signal_id, timestamp, signal_type, entry_price, stop_loss, target_price
FROM trading_signals
WHERE timestamp >= NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC;
```

## Database Tables

| Table | Purpose | Replaces |
|-------|---------|----------|
| `market_data` | Raw market data bars | `processed_data.csv` |
| `strategy_results` | Strategy output with indicators | `strategy_results.csv` |
| `trading_signals` | Trading signals with context | `signals.csv` |
| `system_events` | System events and logs | `events.log` |
| `trading_sessions` | Session metadata | `session_info.json` |

## Key Features

### Real-time Signal Notifications
- Signals saved to database immediately when found
- Market context snapshot included with each signal
- No data loss if system crashes

### Live Monitoring
- Query data while system is running
- View recent signals and session stats
- Monitor system performance in real-time

### Data Safety
- ACID transactions prevent data corruption
- Connection pooling for reliability
- Graceful error handling

### Backup Files
- Optional CSV backups still created
- Database is primary storage
- Best of both worlds

## Troubleshooting

### Connection Issues
```bash
# Check PostgreSQL status
sudo systemctl status postgresql

# Check if TimescaleDB is loaded
psql -h localhost -U tradent_user -d tradent_live -c "SELECT * FROM pg_extension WHERE extname = 'timescaledb';"
```

### Permission Issues
```sql
-- Connect as postgres user and grant additional permissions
GRANT ALL ON ALL TABLES IN SCHEMA public TO tradent_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO tradent_user;
```

### Performance Tuning
```sql
-- Check hypertable status
SELECT * FROM timescaledb_information.hypertables;

-- Optimize for your use case
SELECT set_chunk_time_interval('market_data', INTERVAL '1 day');
```

## Advanced Queries

```sql
-- Signal performance analysis
SELECT 
    DATE(timestamp) as date,
    signal_type,
    COUNT(*) as signal_count,
    AVG(entry_price) as avg_entry,
    MIN(entry_price) as min_entry,
    MAX(entry_price) as max_entry
FROM trading_signals
WHERE timestamp >= NOW() - INTERVAL '7 days'
GROUP BY DATE(timestamp), signal_type
ORDER BY date DESC, signal_type;

-- Session comparison
SELECT 
    session_id,
    bars_processed,
    signals_generated,
    ROUND(signals_generated::DECIMAL / bars_processed * 100, 2) as signal_rate_pct
FROM trading_sessions
WHERE status = 'COMPLETED'
ORDER BY session_start DESC;

-- Market data gaps detection
SELECT 
    symbol,
    timestamp,
    LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp,
    timestamp - LAG(timestamp) OVER (ORDER BY timestamp) as gap
FROM market_data
WHERE timestamp >= NOW() - INTERVAL '1 hour'
ORDER BY timestamp;
```

The database integration provides enterprise-grade reliability while maintaining the simplicity of your existing workflow!