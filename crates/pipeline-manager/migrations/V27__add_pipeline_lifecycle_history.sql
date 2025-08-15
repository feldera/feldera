CREATE TABLE IF NOT EXISTS pipeline_lifecycle_history (
    pipeline_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    name varchar NOT NULL,
    state varchar NOT NULL,
    info text,
    timestamp timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, tenant_id, timestamp),
    FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);
