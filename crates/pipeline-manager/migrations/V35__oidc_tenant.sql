-- Allowed OIDC provider.
CREATE TABLE oidc_provider (
    issuer VARCHAR PRIMARY KEY NOT NULL,     -- Issuer URL for the provider (uniquely identifies it)
    subject_filter VARCHAR NOT NULL,         -- Filter which subjects of the provider are allowed to authenticate
    client_id VARCHAR NOT NULL,              -- Client identifier Feldera uses when contacting the provider
    CONSTRAINT unique_issuer UNIQUE (issuer)
);

-- A user is created when they are first authenticated through the OIDC provider.
-- An OIDC provider cannot be deleted until all users are deleted.
CREATE TABLE oidc_user (
    id UUID PRIMARY KEY NOT NULL,  -- Unique identifier for the user
    issuer VARCHAR NOT NULL,       -- OIDC issuer
    subject VARCHAR NOT NULL,      -- OIDC subject
    CONSTRAINT unique_issuer_subject UNIQUE (issuer, subject),
    FOREIGN KEY (issuer) REFERENCES oidc_provider(issuer) ON DELETE CASCADE
);

-- A tenant is the conceptual entity which owns zero or more pipelines.
--
-- If the tenant is the user-dedicated one, it will have the format: `user-dedicated-<user UUID>`.
-- It is not possible to create a tenant with this name unless it's during user creation.
--
-- The table already exists, as such we need to do alterations to get it to the following:
-- CREATE TABLE tenant (
--     id UUID PRIMARY KEY NOT NULL,   -- Unique identifier for the tenant
--     name VARCHAR NOT NULL,
--     CONSTRAINT unique_name UNIQUE (name)
-- );
ALTER TABLE tenant ADD COLUMN name VARCHAR NULL;
UPDATE tenant SET name = CONCAT(tenant, '-', provider);
ALTER TABLE tenant DROP CONSTRAINT tenant_tenant_provider_key;
ALTER TABLE tenant DROP COLUMN provider;
ALTER TABLE tenant DROP COLUMN tenant;
ALTER TABLE tenant ALTER COLUMN name SET NOT NULL;
ALTER TABLE tenant ADD CONSTRAINT unique_name UNIQUE (name);
ALTER TABLE tenant ALTER COLUMN id SET NOT NULL;

-- A user belongs to zero or more tenants.
-- A tenant has zero or more users.
CREATE TABLE user_tenant (
    user_id UUID NOT NULL,                                          -- User identifier
    tenant_id UUID NOT NULL,                                        -- Tenant identifier
    role VARCHAR NOT NULL,                                          -- "read", "write", "owner"
    PRIMARY KEY (user_id, tenant_id),
    FOREIGN KEY (user_id) REFERENCES oidc_user(id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);

-- The api_key table only needs to have its `scopes` removed, and replaced with `role`.
ALTER TABLE api_key DROP COLUMN scopes;
ALTER TABLE api_key ADD COLUMN role VARCHAR NULL;
UPDATE api_key SET role = 'write';
ALTER TABLE api_key ALTER COLUMN id SET NOT NULL;

-- All pipelines belonging to a tenant need to be removed before a tenant can be deleted.
ALTER TABLE pipeline DROP CONSTRAINT pipeline_tenant_id_fkey;
ALTER TABLE pipeline ADD CONSTRAINT pipeline_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE RESTRICT;
