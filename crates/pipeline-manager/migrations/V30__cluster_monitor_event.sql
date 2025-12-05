-- Regularly the status of the cluster is observed and stored in this table.
-- The rows in this table are regularly cleaned up to prevent it growing unbound.
CREATE TABLE IF NOT EXISTS cluster_monitor_event (
    id UUID PRIMARY KEY NOT NULL,                -- Unique event identifier.
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,  -- Timestamp when status recording started.
    api_status VARCHAR NOT NULL,                 -- Status enumeration variant.
    api_self_info VARCHAR NOT NULL,              -- Status information reported by itself.
    api_resources_info VARCHAR NOT NULL,         -- Status information about the resources backing the API server(s).
    compiler_status VARCHAR NOT NULL,            -- Status enumeration variant.
    compiler_self_info VARCHAR NOT NULL,         -- Status information reported by itself.
    compiler_resources_info VARCHAR NOT NULL,    -- Status information about the resources backing the compiler server(s).
    runner_status VARCHAR NOT NULL,              -- Status enumeration variant.
    runner_self_info VARCHAR NOT NULL,           -- Status information reported by itself.
    runner_resources_info VARCHAR NOT NULL       -- Status information about the resources backing the runner.
);
