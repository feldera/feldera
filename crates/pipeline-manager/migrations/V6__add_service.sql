-- Service
-- A connector can refer to zero, one or multiple services in its configuration.
CREATE TABLE IF NOT EXISTS service (
    id uuid PRIMARY KEY,                  -- Unique identifier (used primarily for foreign key relations)
    tenant_id uuid NOT NULL,              -- Tenant the service belongs to
    name varchar NOT NULL,                -- Immutable unique name given by the tenant (e.g., mysql-example) used to refer to it in the API
    description varchar NOT NULL,         -- Description (e.g., "MySQL server example")
    config varchar NOT NULL,              -- YAML-serialized ServiceConfig enumeration (e.g., contains hostname, port, etc.)
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE,
    CONSTRAINT unique_service_name UNIQUE(tenant_id, name)
);
