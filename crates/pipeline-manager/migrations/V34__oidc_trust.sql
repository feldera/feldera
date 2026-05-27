-- Trust relationships for OIDC workload identity federation.
-- A tenant registers an issuer + subject/audience match pattern; an incoming
-- JWT whose claims satisfy the pattern is authorized as that tenant with the
-- recorded scopes, exactly like an API key.
CREATE TABLE IF NOT EXISTS oidc_trust_relationship (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    name varchar NOT NULL,
    description varchar,
    issuer varchar NOT NULL,
    subject varchar NOT NULL,
    audience varchar,
    scopes text[] NOT NULL,
    CONSTRAINT unique_oidc_trust_name UNIQUE (tenant_id, name),
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_oidc_trust_issuer ON oidc_trust_relationship (issuer);
