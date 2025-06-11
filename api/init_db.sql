CREATE TABLE IF NOT EXISTS outbox_messages (
    id SERIAL PRIMARY KEY,
    message_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    error TEXT DEFAULT 'None',
    from_service TEXT DEFAULT 'None',
    target_service TEXT DEFAULT 'None'
);