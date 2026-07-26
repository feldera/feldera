-- Role-based access control and OIDC workload-identity trust.
--
-- Adds the user concept and the per-(user, tenant) role link that the platform
-- previously lacked (the only principal was the tenant). The released `api_key`
-- table carried a `scopes text[]`; it is replaced with a single `role`.
-- Roles: 'read' < 'write' < 'admin' (and 'owner', which is platform-wide and
-- never stored in a membership; it is sourced from configuration or an owner
-- OIDC trust relationship).

CREATE TABLE IF NOT EXISTS app_user (
    id       uuid PRIMARY KEY,
    provider varchar NOT NULL,   -- OIDC issuer the subject was seen under
    subject  varchar NOT NULL,   -- OIDC `sub`
    email    varchar,            -- display only, may be null
    CONSTRAINT unique_user_identity UNIQUE (provider, subject)
);

CREATE TABLE IF NOT EXISTS tenant_membership (
    tenant_id uuid NOT NULL,
    user_id   uuid NOT NULL,
    role      varchar NOT NULL CHECK (role IN ('read', 'write', 'admin')),
    PRIMARY KEY (tenant_id, user_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant(id)   ON DELETE CASCADE,
    FOREIGN KEY (user_id)   REFERENCES app_user(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tenant_membership_user ON tenant_membership (user_id);

-- Replace the released per-key `scopes text[]` with a single `role`.
-- Existing keys were uniformly {read, write}; the backfill preserves their access.
ALTER TABLE api_key ADD COLUMN role varchar NOT NULL DEFAULT 'read'
    CHECK (role IN ('read', 'write'));
UPDATE api_key SET role = CASE WHEN 'write' = ANY (scopes) THEN 'write' ELSE 'read' END;
ALTER TABLE api_key DROP COLUMN scopes;

-- Trust relationships for OIDC workload identity federation.
--
-- A trust registers an issuer plus subject/audience match patterns; an incoming
-- JWT from that issuer whose claims satisfy the patterns is authorized with the
-- recorded role, like a signature-verified API key. `role` is capped at the
-- creator's role at write time.
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
    role varchar NOT NULL DEFAULT 'read' CHECK (role IN ('read', 'write', 'admin', 'owner')),
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
