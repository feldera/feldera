-- Per-tenant connector dependency configuration.
--
-- One row per tenant. `content` is the verbatim text of that tenant's
-- `connectors.toml` (one Cargo dep per line, no section header) and is
-- spliced into the describer and per-pipeline `Cargo.toml` `[dependencies]`
-- block. An empty `content` means "bundled connectors only".
--
-- `content_hash` is the sha256 of `content` and serves both as the describer
-- cache key and as the ETag for optimistic concurrency on updates.
-- `version` is monotonic per tenant and is the value clients echo back via
-- `If-Match` headers when editing.
CREATE TABLE IF NOT EXISTS tenant_connector_config (
    tenant_id    uuid PRIMARY KEY,
    content      varchar NOT NULL,
    content_hash varchar NOT NULL,
    version      bigint NOT NULL,
    edited_at    timestamp with time zone NOT NULL,
    edited_by    varchar NOT NULL,
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);
