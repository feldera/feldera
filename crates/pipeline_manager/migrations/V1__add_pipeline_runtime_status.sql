-- Store runtime state of pipelines in a separate table.
-- DB schema support for desired-state-based pipeline
-- management.

CREATE TABLE IF NOT EXISTS pipeline_runtime_state (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    location varchar,
    desired_status varchar NOT NULL,
    current_status varchar NOT NULL,
    -- Time when the current status was set.
    status_since bigint NOT NULL,
    error varchar,
    -- Time when the pipeline process was started.
    created bigint NOT NULL,
    FOREIGN KEY (id) REFERENCES pipeline(id) ON DELETE CASCADE
);

INSERT INTO pipeline_runtime_state (id, tenant_id, location, desired_status, current_status, status_since, created)
SELECT id, tenant_id, CAST(port as varchar), 'shutdown', 'shutdown', extract(epoch from now()), extract(epoch from now())
FROM pipeline;

ALTER TABLE pipeline
DROP COLUMN port,
DROP COLUMN status,
DROP COLUMN created;

ALTER TABLE pipeline_history
DROP COLUMN port,
DROP COLUMN status,
DROP COLUMN created;
