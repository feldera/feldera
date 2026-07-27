-- Role-based access control, OIDC workload-identity trust, and tenant identity.
--
-- Adds the user concept and the per-(user, tenant) role link that the platform
-- previously lacked (the only principal was the tenant). The released `api_key`
-- table carried a `scopes text[]`; it is replaced with a single `role`.
-- Roles: 'read' < 'write' < 'admin' (and 'owner', which is platform-wide and
-- never stored in a membership; it is sourced from configuration or an owner
-- OIDC trust relationship).
--
-- The second half makes a tenant's name, on its own, its identity.

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

-- BEGIN tenant identity
--
-- db/test.rs runs the section between these markers on its own, against temp
-- tables, so keep both markers and keep the section free of the tables above.
--
-- Make the tenant name, on its own, the tenant's identity.
--
-- V0 keyed a tenant by (tenant, provider), where `provider` is the OIDC issuer
-- its users authenticate through. Only one issuer is ever configured, so the
-- pair never distinguished two tenants. What it did do was tie a tenant's
-- identity to the issuer string: changing FELDERA_AUTH_ISSUER (an IdP migration,
-- a custom domain, a recreated Cognito pool) made the next login miss the
-- conflict target and create a second tenant of the same name, leaving every
-- pipeline on the unreachable first one.
--
-- Keying on the name alone makes that case reuse the existing tenant. The issuer
-- stays as provenance, under a name that says so: `initial_provider`.

-- A deployment whose issuer changed before this migration already has two
-- tenants sharing a name, and we cannot tell from here which deployments those
-- are. Rename rather than refuse: a failed migration would stop the manager from
-- starting and leave no in-product way out, whereas renaming keeps every tenant
-- present, reachable and unchanged.
--
-- Nothing is merged, moved or deleted. The name stays with the tenant users
-- already reach today, which is the one registered under the issuer that is
-- configured now: only that one was reachable before this migration, since a
-- login resolved (name, issuer). Keeping the name there means the upgrade does
-- not change what anyone sees. `feldera.auth_issuer` is set on this connection
-- by `run_migrations`; when it is absent, as with authentication disabled, fall
-- back to the tenant holding the most pipelines so the name still follows the
-- work. Ties break on the id for determinism.
--
-- The others keep all of their own pipelines under a name qualified by their id,
-- which is unique. An owner finds them through `GET /v0/tenants`, acts in them
-- with the `Feldera-Tenant` header, and can rename them.
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
-- END tenant identity
