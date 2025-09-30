CREATE TABLE IF NOT EXISTS pipeline_lifecycle_events (
    event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    deployment_resources_status varchar NOT NULL,
    deployment_runtime_status varchar,
    deployment_runtime_desired_status varchar,
    info text,
    recorded_at TIMESTAMP NOT NULL DEFAULT now(),
    FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);
