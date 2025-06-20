# LLM Integration for Tradent Trading System

This module provides LLM enhancement capabilities for trading signals in the Tradent live trading system.

## Architecture

The LLM integration follows a memory-first, non-blocking architecture:

1. **In-Memory Processing**: All signal operations happen in memory first for speed
2. **Non-Blocking Design**: Main trading system never waits for LLM processing
3. **Hybrid Database Strategy**: 
   - IMMEDIATE writes for critical data (signals, market data)
   - ASYNC writes for LLM results
4. **Continuous CSV Export**: Real-time CSV generation for analysis

## Components

### 1. SignalMemoryStore (`memory_store.py`)
- Thread-safe in-memory storage for signals
- O(1) operations for adding, retrieving, and updating signals
- Automatic cleanup of old signals
- Statistics tracking

### 2. SignalFormatter (`signal_formatter.py`)
- Formats trading signals for LLM processing
- Includes market context and technical indicators
- Supports batch formatting

### 3. LLMProcessor (`llm_processor.py`)
- Handles OpenRouter API calls
- Built-in rate limiting (14 requests/min, 950k tokens/min)
- Exponential backoff retry logic
- Connection pooling for efficiency

### 4. LLMResponseParser (`response_parser.py`)
- Robust parsing of LLM responses
- Multiple fallback strategies
- Handles various response formats

### 5. LLMCSVExporter (`csv_exporter.py`)
- Generates two CSV outputs:
  - `llm_signals.csv`: Signal bars with LLM enhancements
  - `llm_strategy_results.csv`: All bars with LLM columns
- Thread-safe file operations
- Continuous append mode

### 6. LLMWorker (`worker.py`)
- Background async worker
- Continuously processes signals from memory store
- Non-blocking database updates
- Periodic cleanup and statistics

### 7. LLMConfig (`config.py`)
- Centralized configuration management
- Environment variable support
- Validation and defaults

## Configuration

Add to your `config.json`:

```json
{
  "llm": {
    "enabled": true,
    "memory_retention_minutes": 60,
    "max_memory_signals": 1000,
    "csv_output_directory": "output/llm_enhanced/",
    "batch_export_interval_seconds": 60,
    "model": "openrouter/google/gemini-2.5-flash-preview-05-20",
    "batch_size": 1,
    "poll_interval": 1.0,
    "cleanup_interval_minutes": 30,
    "max_retries": 3,
    "lookback_periods": 20,
    "indicators_to_include": [
      "rsi", "atr", "sma_20", "sma_50", "volume_ratio",
      "bb_upper", "bb_lower", "bb_middle", "stochastic_k", "stochastic_d"
    ]
  }
}
```

## Environment Variables

Set your OpenRouter API key:
```bash
export OPENROUTER_API_KEY=your_api_key_here
```

## Database Schema

The integration adds two tables:

1. **llm_enhancement_tasks**: Tracks LLM processing tasks
2. **llm_analysis_results**: Stores LLM analysis results

Run the schema update:
```bash
psql -h localhost -U tradent_user -d tradent_live -f database/llm_schema_updates.sql
```

## Usage

The LLM integration is automatically initialized when you run:
```bash
python main_with_database.py
```

To disable LLM processing, set `"enabled": false` in the config.

## Output Files

The system generates:
- `output/llm_enhanced/llm_signals.csv`: Enhanced signal data
- `output/llm_enhanced/llm_strategy_results.csv`: Full market data with LLM columns
- `output/llm_enhanced/llm_processing_summary.json`: Processing statistics

## Performance Considerations

- Memory store can handle 1000+ signals efficiently
- LLM processing happens in background (non-blocking)
- Rate limiting prevents API throttling
- Automatic cleanup prevents memory bloat

## Monitoring

The system provides real-time statistics:
- Signals processed/pending/failed
- Average processing time
- Rating distribution
- Memory usage

Access stats via:
```python
stats = memory_store.get_stats()
```

## Error Handling

- Failed signals are automatically retried (up to 3 times)
- Non-critical errors don't stop the main trading system
- All errors are logged with context

## Testing

Test individual components:
```python
# Test memory store
from llm_integration.memory_store import SignalMemoryStore
store = SignalMemoryStore()

# Test signal formatter
from llm_integration.signal_formatter import SignalFormatter
formatter = SignalFormatter()

# Test response parser
from llm_integration.response_parser import LLMResponseParser
parser = LLMResponseParser()
```