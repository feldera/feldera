CREATE TABLE pipeline_deployment(
    id uuid PRIMARY KEY,
    pipeline_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    config VARCHAR NOT NULL,
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE,
    CONSTRAINT unique_deployment_id UNIQUE (id, tenant_id),
    CONSTRAINT unique_pipeline_id UNIQUE (pipeline_id, tenant_id)
);

-- Avoid NULL as a value for program status and use the string 'none', as per
-- `ProgramStatus` in `db/program.rs`
UPDATE program SET status = 'none' WHERE status IS NULL;

-- Remove history tables. We'd ideally create an entry in pipeline_deployment
-- for all entries in the below tables, but that feels overkill.
DROP TABLE pipeline_history CASCADE;
DROP TABLE program_history CASCADE;
DROP TABLE connector_history CASCADE;
DROP TABLE attached_connector_history CASCADE;

-- Remove `last_revision` column from the pipeline table because this is
-- subsumed by the pipeline_deployment table above
ALTER TABLE pipeline
DROP column last_revision;
