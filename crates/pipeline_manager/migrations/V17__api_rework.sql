-- Drop all previous pipeline-related tables and triggers
DROP TRIGGER IF EXISTS pipeline_notify ON pipeline;
DROP TRIGGER IF EXISTS pipeline_runtime_state_notify ON pipeline_runtime_state;
DROP TRIGGER IF EXISTS program_notify ON program;
DROP TABLE IF EXISTS attached_connector;
DROP TABLE IF EXISTS connector;
DROP TABLE IF EXISTS service;
DROP TABLE IF EXISTS pipeline_runtime_state;
DROP TABLE IF EXISTS pipeline_deployment;
DROP TABLE IF EXISTS pipeline;
DROP TABLE IF EXISTS compiled_binary;
DROP TABLE IF EXISTS program;

-- Pipelines
--
-- Columns set explicitly by the end-user: name, description, runtime_config, program_code, program_config.
-- All other columns are set/updated as a side-effect or due to program and deployment state transitions.
CREATE TABLE IF NOT EXISTS pipeline (
    -- General
    id UUID PRIMARY KEY,                    -- Globally unique pipeline identifier
    tenant_id UUID NOT NULL,                -- Tenant identifier
    name VARCHAR NOT NULL,                  -- Name
    description VARCHAR NOT NULL,           -- Description
    created_at BIGINT NOT NULL,             -- Epoch timestamp when the pipeline was created.
    version BIGINT NOT NULL,                -- Pipeline version code, incremented each time if any of the columns
                                            -- name, description, runtime_config, program_code or program_config
                                            -- is changed.
                                            -- Initial value is 1.

    -- Runtime
    runtime_config VARCHAR NOT NULL,        -- Pipeline runtime configuration
                                            -- Serialized type: RuntimeConfiguration
    
    -- Program
    program_code VARCHAR NOT NULL,          -- Program SQL code
    program_config VARCHAR NOT NULL,        -- Program compilation configuration
                                            -- Serialized type: ProgramConfiguration
    program_version BIGINT NOT NULL,        -- Program version code, incremented each time the program_code or program_config is edited.
                                            -- Initial value is 1.
    program_status VARCHAR NOT NULL,        -- Program status, which evolves as the stages of compilation take place.
                                            -- When program code is edited, the status is changed back to pending.
                                            -- Value is one of: pending, compiling_sql, compiling_rust, success,
                                            --                  sql_error, rust_error, system_error
                                            -- Serialized type: together with program_error into ProgramStatus
    program_status_since BIGINT NOT NULL,   -- Epoch timestamp when the current program status was set.
    program_error VARCHAR NULL,             -- Error (if any) that occurred during compilation.
                                            -- * Non-NULL: sql_error, rust_error, system_error
                                            -- * NULL: pending, success, compiling_sql, compiling_rust
    program_info VARCHAR NULL,              -- After SQL compilation, this is populated and contains all information
                                            -- the program such as schema (tables, views, properties) and the
                                            -- connectors derived from the schema.
                                            -- Serialized type: optional ProgramInfo
                                            -- * Non-NULL: success, compiling_rust, rust_error
                                            -- * NULL: pending, sql_error
                                            -- * Non-NULL or NULL: system_error
    program_binary_url VARCHAR NULL,        -- URL where the program binary can be downloaded from.
                                            -- * Non-NULL: success
                                            -- * NULL: any other program status

    -- Deployment
    deployment_status VARCHAR NOT NULL,          -- Deployment status, which evolves as the stages take place.
                                                 -- Value is one of: shutdown, provisioning, initializing,
                                                 --                  running, paused, failed, shutting_down
    deployment_status_since BIGINT NOT NULL,     -- Epoch timestamp when the deployment status was set.
    deployment_desired_status VARCHAR NOT NULL,  -- Desired deployment status, which affects the upcoming transitions.
                                                 -- Value is one of: running, paused, shutdown
    deployment_error VARCHAR NULL,               -- Critical error (if any) that occurred during deployment.
                                                 -- Serialized type: optional PipelineRuntimeError
                                                 -- * Non-NULL: failed
                                                 -- * NULL: any other deployment status
    deployment_config VARCHAR NULL,              -- Comprehensive configuration passed to the pipeline executable.
                                                 -- Serialized type: optional PipelineConfig
                                                 -- * Non-NULL: any other deployment status
                                                 -- * NULL: shutdown
                                                 -- * Non-NULL or NULL: failed
    deployment_location VARCHAR NULL,            -- Location (<hostname>:<port>) to reach the running pipeline.
                                                 -- * Non-NULL: any other deployment status
                                                 -- * NULL: shutdown, provisioning

    -- Constraints
    CONSTRAINT unique_pipeline_name UNIQUE(tenant_id, name),
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);

-- Invoke trigger function for changes to the pipeline table
CREATE TRIGGER pipeline_notify
AFTER INSERT OR UPDATE OR DELETE ON pipeline
FOR EACH ROW EXECUTE PROCEDURE notification();
