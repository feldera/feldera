-- Role-based access control, OIDC workload-identity trust, and tenant identity.
--
-- Adds a user, and a role per (user, tenant). Replaces `api_key.scopes text[]`
-- with a single `role`. Roles are ordered 'read' < 'write' < 'admin'; 'owner'
-- is platform-wide, never stored in a membership, and comes from configuration
-- or from an owner OIDC trust relationship.
--
-- The `tenant identity` section at the end of this file makes a tenant's name,
-- on its own, its identity.

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
-- creator's role at write time, and at 'admin' by the CHECK: 'owner' is
-- platform-wide and comes from configuration (`--owner-trusts`), never from a
-- row here, so no request can mint an owner.
CREATE TABLE IF NOT EXISTS oidc_trust_relationship (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    name varchar NOT NULL,
    description varchar,
    issuer varchar NOT NULL,
    subject varchar NOT NULL,
    audience varchar,
    role varchar NOT NULL DEFAULT 'read' CHECK (role IN ('read', 'write', 'admin')),
    CONSTRAINT unique_oidc_trust_name UNIQUE (tenant_id, name),
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);

-- The auth hot path resolves a federated token by its issuer, so index it.
CREATE INDEX IF NOT EXISTS idx_oidc_trust_issuer ON oidc_trust_relationship (issuer);

-- BEGIN tenant identity
--
-- db/test.rs runs the section between these markers on its own, against temp
-- tables, so keep both markers and keep the section free of the tables above.
--
-- Make the tenant name, on its own, the tenant's identity.
--
-- V0 keyed a tenant by (tenant, provider), where `provider` is the OIDC issuer.
-- Only one issuer is ever configured, so the pair never distinguished two
-- tenants; all it did was tie a tenant's identity to the issuer string. Change
-- FELDERA_AUTH_ISSUER and the next login forked a second tenant of the same
-- name, leaving the pipelines on the first one, which nothing could reach. The
-- issuer stays on as provenance, renamed `initial_provider` at the end.

-- Two tenants may already share a name here, from an issuer change made before
-- this migration, and nothing in the database says which deployments those are.
-- Rename rather than refuse: a migration that fails stops the manager from
-- starting, with no way out from inside the product.
--
-- The name stays with the tenant its users reach today, which is the one
-- registered under the issuer configured now, so the upgrade changes nothing
-- anyone sees. `run_migrations` puts that issuer in `feldera.auth_issuer`; with
-- authentication off it is absent and the name follows the pipelines instead.
-- Ties break on the id. Every other tenant keeps its own pipelines under
-- `<name> (<id>)`, which an owner can list and rename.
DO $$
DECLARE renamed int;
BEGIN
    WITH ranked AS (
        SELECT t.id,
               row_number() OVER (
                   PARTITION BY t.tenant
                   ORDER BY (t.provider IS NOT DISTINCT FROM
                             current_setting('feldera.auth_issuer', true)) DESC,
                            (SELECT count(*) FROM pipeline p WHERE p.tenant_id = t.id) DESC,
                            t.id
               ) AS rank_in_name
        FROM tenant t
    )
    UPDATE tenant
    SET tenant = tenant.tenant || ' (' || tenant.id || ')'
    FROM ranked
    WHERE tenant.id = ranked.id AND ranked.rank_in_name > 1;

    GET DIAGNOSTICS renamed = ROW_COUNT;
    IF renamed > 0 THEN
        RAISE NOTICE
            'Renamed % tenant(s) whose name was shared with another tenant, '
            'which happens when the configured OIDC issuer changed. Their '
            'pipelines are untouched; list them with GET /v0/tenants.',
            renamed;
    END IF;
END $$;

-- The old constraint was declared inline, so its name is whatever PostgreSQL
-- generated; look it up rather than assume.
DO $$
DECLARE old_constraint text;
BEGIN
    SELECT conname INTO old_constraint
    FROM pg_constraint
    WHERE conrelid = 'tenant'::regclass
      AND contype = 'u'
      AND pg_get_constraintdef(oid) LIKE 'UNIQUE (tenant, provider)%';

    IF old_constraint IS NOT NULL THEN
        EXECUTE format('ALTER TABLE tenant DROP CONSTRAINT %I', old_constraint);
    END IF;
END $$;

ALTER TABLE tenant ADD CONSTRAINT unique_tenant_name UNIQUE (tenant);

-- Now that the column keys nothing, name it for what it holds: the OIDC issuer
-- the tenant was first provisioned under, kept for provenance. `provider` read
-- like a live property of the tenant, which invited exactly the coupling this
-- section removes.
--
-- `app_user.provider` keeps its name: there the issuer is part of the identity,
-- because OIDC only guarantees `sub` to be unique within one issuer.
ALTER TABLE tenant RENAME COLUMN provider TO initial_provider;
-- END tenant identity
