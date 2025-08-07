-- Create support data collections table
CREATE TABLE IF NOT EXISTS support_data_collections (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id UUID NOT NULL,
    tenant_id UUID NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    -- Serialized support bundle data using MessagePack
    support_bundle_data BYTEA NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Foreign key constraints
    FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE,

    -- Indexes for efficient querying
    CONSTRAINT unique_support_data_collection UNIQUE(pipeline_id, collected_at)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_support_data_collections_pipeline_id ON support_data_collections(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_support_data_collections_collected_at ON support_data_collections(collected_at);
CREATE INDEX IF NOT EXISTS idx_support_data_collections_tenant_id ON support_data_collections(tenant_id);
