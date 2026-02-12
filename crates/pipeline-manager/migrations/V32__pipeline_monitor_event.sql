-- Regularly the status of the pipeline is observed and stored in this table.
-- The rows in this table are regularly cleaned up to prevent it growing unbound.
CREATE TABLE IF NOT EXISTS pipeline_monitor_event (
    id UUID PRIMARY KEY NOT NULL,                -- Unique event identifier.
    pipeline_id UUID NOT NULL,                   -- Identifier of the pipeline the event corresponds to.
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,  -- Timestamp when status recording started.
    resources_status VARCHAR NOT NULL,           -- Resources status.
    resources_status_details VARCHAR NOT NULL,   -- Resources status details.
    resources_desired_status VARCHAR NOT NULL,   -- Resources desired status.
    runtime_status VARCHAR NULL,                 -- Runtime status (only set during deployment).
    runtime_status_details VARCHAR NULL,         -- Runtime status details (only set during deployment).
    runtime_desired_status VARCHAR NULL,         -- Runtime desired status (only set during deployment).
    program_status VARCHAR NOT NULL,             -- Program status.
    storage_status VARCHAR NOT NULL,             -- Storage status.
    FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE
);
