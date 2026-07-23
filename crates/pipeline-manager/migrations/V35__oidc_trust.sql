-- Trust relationships for OIDC workload identity federation.
--
-- A trust registers an issuer plus subject/audience match patterns; an incoming
-- JWT from that issuer whose claims satisfy the patterns is authorized with the
-- recorded role, like a signature-verified API key. `role` is the RBAC role
-- granted (see V36), capped at the creator's role at write time.
--
-- Scope follows the role. read/write/admin trusts are tenant-scoped: `tenant_id`
-- names the tenant they authorize into. An `owner` trust is platform-wide and
-- belongs to no tenant, so `tenant_id` is NULL; the acting tenant then comes
-- from the Feldera-Tenant header at request time. The CHECK makes the two
-- inseparable: tenant_id is NULL if and only if the role is owner.
CREATE TABLE IF NOT EXISTS oidc_trust_relationship (
    id uuid PRIMARY KEY,
    tenant_id uuid,
    name varchar NOT NULL,
    description varchar,
    issuer varchar NOT NULL,
    subject varchar NOT NULL,
    audience varchar,
    role text NOT NULL DEFAULT 'read' CHECK (role IN ('read', 'write', 'admin', 'owner')),
    CONSTRAINT oidc_trust_owner_is_platform CHECK ((tenant_id IS NULL) = (role = 'owner')),
    CONSTRAINT unique_oidc_trust_name UNIQUE (tenant_id, name),
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);

-- unique_oidc_trust_name bounds tenant-scoped names per tenant, but a UNIQUE
-- constraint treats NULL tenant_ids as distinct and so does not bound owner
-- trusts. Name owner trusts uniquely across the platform with a partial index.
CREATE UNIQUE INDEX IF NOT EXISTS idx_oidc_owner_trust_name
    ON oidc_trust_relationship (name) WHERE tenant_id IS NULL;

-- The auth hot path resolves a federated token by its issuer, so index it.
CREATE INDEX IF NOT EXISTS idx_oidc_trust_issuer ON oidc_trust_relationship (issuer);
