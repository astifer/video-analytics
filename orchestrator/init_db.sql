-- Create outbox table
CREATE TABLE IF NOT EXISTS outbox_messages (
    id SERIAL PRIMARY KEY,
    message_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    error TEXT
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_messages(status);
CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON outbox_messages(created_at);

-- Create outbox table for runner commands
CREATE TABLE IF NOT EXISTS runner_outbox (
    id SERIAL PRIMARY KEY,
    scenario_id VARCHAR(255) NOT NULL,
    command_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    error TEXT,
    result JSONB
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_runner_outbox_status ON runner_outbox(status);
CREATE INDEX IF NOT EXISTS idx_runner_outbox_scenario ON runner_outbox(scenario_id);