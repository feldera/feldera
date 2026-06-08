-- Trust relationships for OIDC workload identity federation.
--
-- A tenant registers an issuer plus subject/audience match patterns; an
-- incoming JWT from that issuer whose claims satisfy the patterns is authorized
-- to act as the tenant with the recorded role, like a signature-verified API
-- key. `role` is the RBAC role granted (see V36); it is capped at the creator's
-- role at write time and defaults to the least privilege.
CREATE TABLE IF NOT EXISTS oidc_trust_relationship (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    name varchar NOT NULL,
    description varchar,
    issuer varchar NOT NULL,
    subject varchar NOT NULL,
    audience varchar,
    role text NOT NULL DEFAULT 'read' CHECK (role IN ('read', 'write', 'admin', 'owner')),
    CONSTRAINT unique_oidc_trust_name UNIQUE (tenant_id, name),
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);

-- The auth hot path resolves a federated token by its issuer, so index it.
CREATE INDEX IF NOT EXISTS idx_oidc_trust_issuer ON oidc_trust_relationship (issuer);
