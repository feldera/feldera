CREATE TABLE deployed_pipeline(
    id uuid PRIMARY KEY,
    pipeline_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    config VARCHAR NOT NULL,
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE,
    CONSTRAINT unique_deployment_id UNIQUE (id, tenant_id),
    CONSTRAINT unique_pipeline_id UNIQUE (pipeline_id, tenant_id)
);

-- TODO: migrate NULL pipeline-statuses and remove history tables
