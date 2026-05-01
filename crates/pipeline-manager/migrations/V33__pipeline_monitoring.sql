-- Regularly the status of the pipeline is observed and stored in this table.
-- The rows in this table are regularly cleaned up to prevent the table from
-- growing unbounded.
CREATE TABLE IF NOT EXISTS pipeline_monitor_event (
    event_id UUID PRIMARY KEY NOT NULL,                           -- Unique event identifier.
    pipeline_id UUID NOT NULL,                                    -- Identifier of the pipeline the event corresponds to.
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,  -- Timestamp when status recording started.
    deployment_resources_status VARCHAR NOT NULL,           -- Resources status.
    deployment_resources_status_details VARCHAR NULL,       -- Resources status details.
    deployment_resources_desired_status VARCHAR NOT NULL,   -- Resources desired status.
    deployment_runtime_status VARCHAR NULL,                 -- Runtime status (only set during deployment).
    deployment_runtime_status_details VARCHAR NULL,         -- Runtime status details (only set during deployment).
    deployment_runtime_desired_status VARCHAR NULL,         -- Runtime desired status (only set during deployment).
    deployment_error VARCHAR NULL,                          -- Set if a deployment stopped by itself due to a fatal error.
    program_status VARCHAR NOT NULL,             -- Program status.
    storage_status VARCHAR NOT NULL,             -- Storage status.
    storage_status_details VARCHAR NULL,         -- Storage status details.
    FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE
);

-- Create an index on (pipeline_id, recorded_at DESC, event_id DESC), such that looking up
-- which events need to be deleted is faster.
CREATE INDEX ON pipeline_monitor_event(pipeline_id, recorded_at DESC, event_id DESC);

-- Create an index on (pipeline_id, event_id), such that point queries for specific
-- events are faster.
CREATE INDEX ON pipeline_monitor_event(pipeline_id, event_id);
