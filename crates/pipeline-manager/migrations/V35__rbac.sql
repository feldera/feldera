-- Role-based access control.
--
-- Adds the user concept and the per-(user, tenant) role link that the platform
-- previously lacked (the only principal was the tenant). API keys and OIDC
-- trust relationships gain a single `role` replacing the old `scopes text[]`.
-- Roles: 'read' < 'write' < 'admin' (and 'owner', which is platform-wide and
-- never stored here; it is sourced from configuration or an owner OIDC trust).

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
    role      text NOT NULL CHECK (role IN ('read', 'write', 'admin')),
    PRIMARY KEY (tenant_id, user_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant(id)   ON DELETE CASCADE,
    FOREIGN KEY (user_id)   REFERENCES app_user(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tenant_membership_user ON tenant_membership (user_id);

-- Replace the per-credential `scopes text[]` with a single `role`.
-- Existing rows were uniformly {read, write}; backfill preserves their access.
ALTER TABLE api_key                 ADD COLUMN role text NOT NULL DEFAULT 'read';
ALTER TABLE oidc_trust_relationship ADD COLUMN role text NOT NULL DEFAULT 'read';

UPDATE api_key
   SET role = CASE WHEN 'write' = ANY (scopes) THEN 'write' ELSE 'read' END;
UPDATE oidc_trust_relationship
   SET role = CASE WHEN 'write' = ANY (scopes) THEN 'write' ELSE 'read' END;

ALTER TABLE api_key                 DROP COLUMN scopes;
ALTER TABLE oidc_trust_relationship DROP COLUMN scopes;
