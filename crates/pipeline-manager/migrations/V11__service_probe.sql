-- Unique constraint for service table such that we can add
-- the foreign key constraint in service_probe that it must
-- be created for the same tenant.
ALTER TABLE service
ADD CONSTRAINT unique_tenant_id_service_id UNIQUE(tenant_id, id);

CREATE TABLE IF NOT EXISTS service_probe (
    id uuid PRIMARY KEY,            -- Unique identifier
    tenant_id uuid NOT NULL,        -- Tenant the service and thus probe belongs to
    service_id uuid NOT NULL,       -- Service the probe belongs to
    status varchar NOT NULL,        -- ServiceProbeStatus: pending, running, success, failure
    probe_type varchar NOT NULL,    -- ServiceProbeType enumeration variant type
    request varchar NOT NULL,       -- ServiceProbeRequest (YAML serialized)
    response varchar,               -- Response instance. Value is dependent on status:
                                    --   * pending or running: NULL
                                    --   * success or failure: ServiceProbeResponse (YAML serialized)
    created_at bigint NOT NULL,     -- Timestamp when the probe was created
    started_at bigint,              -- Timestamp when the probe started (NULL iff pending)
    finished_at bigint,             -- Timestamp when the probe finished (NULL iff pending or running)
    FOREIGN KEY (tenant_id, service_id) REFERENCES service(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (service_id) REFERENCES service(id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);
