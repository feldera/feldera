-- These columns are only used internally to track pipelines.
ALTER TABLE pipeline
    ADD COLUMN track_compilation_version BIGINT DEFAULT 1;
ALTER TABLE pipeline
    ADD COLUMN track_deployment_base_version BIGINT DEFAULT 1;
ALTER TABLE pipeline
    ADD COLUMN track_deployment_operational_version BIGINT DEFAULT 1;
