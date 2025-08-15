CREATE TABLE IF NOT EXISTS pipeline_lifecycle_events (
    event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    deployment_status varchar NOT NULL,
    info text,
    recorded_at TIMESTAMP NOT NULL DEFAULT now(),
    FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);
