-- LLM Enhancement Tables for Tradent Live Trading System
-- This schema adds LLM processing capabilities to the existing trading system

-- Table to track LLM enhancement tasks for each signal
CREATE TABLE IF NOT EXISTS llm_enhancement_tasks (
    task_id BIGSERIAL PRIMARY KEY,
    signal_id TEXT NOT NULL REFERENCES trading_signals(signal_id),
    session_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0
);

-- Table to store LLM analysis results
CREATE TABLE IF NOT EXISTS llm_analysis_results (
    analysis_id BIGSERIAL PRIMARY KEY,
    signal_id TEXT NOT NULL REFERENCES trading_signals(signal_id),
    task_id BIGINT NOT NULL REFERENCES llm_enhancement_tasks(task_id),
    llm_rating INTEGER CHECK (llm_rating BETWEEN 0 AND 4),
    llm_choppiness TEXT CHECK (llm_choppiness IN ('yes', 'no', 'unknown')),
    llm_analysis_text TEXT,
    llm_market_context JSONB,
    llm_model_used TEXT,
    processing_time_ms INTEGER,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    llm_raw_response TEXT
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_llm_tasks_status ON llm_enhancement_tasks (status, created_at);
CREATE INDEX IF NOT EXISTS idx_llm_tasks_signal ON llm_enhancement_tasks (signal_id);
CREATE INDEX IF NOT EXISTS idx_llm_results_signal ON llm_analysis_results (signal_id);
CREATE INDEX IF NOT EXISTS idx_llm_results_task ON llm_analysis_results (task_id);

-- Add comments for documentation
COMMENT ON TABLE llm_enhancement_tasks IS 'Tracks LLM processing tasks for trading signals';
COMMENT ON TABLE llm_analysis_results IS 'Stores LLM-enhanced analysis results for trading signals';
COMMENT ON COLUMN llm_enhancement_tasks.status IS 'Task status: pending, processing, completed, or failed';
COMMENT ON COLUMN llm_analysis_results.llm_rating IS 'LLM rating of signal quality (0-4)';
COMMENT ON COLUMN llm_analysis_results.llm_choppiness IS 'LLM assessment of market choppiness';
COMMENT ON COLUMN llm_analysis_results.llm_market_context IS 'Additional market context from LLM in JSON format';