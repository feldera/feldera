-- Safe to delete this table here because it is not
-- used in any internal or external APIs
DROP TABLE api_key;

-- Hash has to be globally unique as the API key is
-- the only value required to authenticate, so it must
-- uniquely correspond to a (tenant_id, name) pair
CREATE TABLE IF NOT EXISTS api_key (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    name varchar NOT NULL,
    hash varchar NOT NULL,
    scopes text[] NOT NULL,
    CONSTRAINT unique_api_key_name UNIQUE(tenant_id, name),
    CONSTRAINT unique_hash UNIQUE(hash),
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);
