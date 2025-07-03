-- PIPELINE SCHEMA CHANGES

-- Add `storage_status` field to the `pipeline` table
ALTER TABLE pipeline
ADD COLUMN storage_status VARCHAR NOT NULL DEFAULT 'cleared';

-- PIPELINE DESIRED STATE CHANGES

-- Unchanged states: rename desired state Shutdown to Stopped
UPDATE pipeline SET deployment_desired_status = 'stopped'
                WHERE (deployment_status = 'provisioning'
                       OR deployment_status = 'initializing'
                       OR deployment_status = 'paused'
                       OR deployment_status = 'running'
                       OR deployment_status = 'unavailable')
                      AND deployment_desired_status = 'shutdown';

-- Shutdown (will become Stopped): rename desired state Shutdown to Stopped
UPDATE pipeline SET deployment_desired_status = 'stopped'
                WHERE deployment_status = 'shutdown'
                  AND deployment_desired_status = 'shutdown';

-- ShuttingDown (will become Stopped): because clearing, the only desired state is Stopped in this migration
UPDATE pipeline SET deployment_desired_status = 'stopped'
                WHERE deployment_status = 'shutting_down';

-- Failed (will become Stopping): only allowed desired state is Stopped
UPDATE pipeline SET deployment_desired_status = 'stopped'
                WHERE deployment_status = 'failed';

-- Suspended (will become Stopped): rename desired state Shutdown to Stopped
UPDATE pipeline SET deployment_desired_status = 'stopped'
                WHERE deployment_status = 'suspended'
                  AND deployment_desired_status = 'shutdown';

-- SuspendingCircuit (will become Suspending): only allowed desired state is Suspended
UPDATE pipeline SET deployment_desired_status = 'suspended'
                WHERE deployment_status = 'suspending_circuit';

-- SuspendingCompute (will become Stopping): only allowed desired state is Stopped
UPDATE pipeline SET deployment_desired_status = 'stopped'
                WHERE deployment_status = 'suspending_compute';

-- PIPELINE STATE CHANGES

-- The states that do not change have in use storage
UPDATE pipeline SET storage_status = 'in_use',
                    deployment_error = NULL,
                    suspend_info = NULL
                WHERE deployment_status = 'provisioning'
                   OR deployment_status = 'initializing'
                   OR deployment_status = 'paused'
                   OR deployment_status = 'running'
                   OR deployment_status = 'unavailable';

-- Shutdown -> Stopped
UPDATE pipeline SET deployment_status = 'stopped',
                    storage_status = 'cleared',
                    deployment_error = NULL,
                    suspend_info = NULL
                WHERE deployment_status = 'shutdown';

-- ShuttingDown -> Stopped
UPDATE pipeline SET deployment_status = 'stopped',
                    storage_status = 'clearing',
                    deployment_error = NULL,
                    suspend_info = NULL
                WHERE deployment_status = 'shutting_down';

-- Failed -> Stopping
UPDATE pipeline SET deployment_status = 'stopping',
                    storage_status = 'in_use',
                    deployment_error = deployment_error, -- Remains
                    suspend_info = NULL
                WHERE deployment_status = 'failed';

-- Suspended -> Stopped
UPDATE pipeline SET deployment_status = 'stopped',
                    storage_status = 'in_use',
                    deployment_error = NULL,
                    suspend_info = suspend_info -- Remains
                WHERE deployment_status = 'suspended';

-- SuspendingCircuit -> Suspending
UPDATE pipeline SET deployment_status = 'suspending',
                    storage_status = 'in_use',
                    deployment_error = NULL,
                    suspend_info = NULL
                WHERE deployment_status = 'suspending_circuit';

-- SuspendingCompute -> Stopping
UPDATE pipeline SET deployment_status = 'stopping',
                    storage_status = 'in_use',
                    deployment_error = NULL,
                    suspend_info = suspend_info -- Remains
                WHERE deployment_status = 'suspending_compute';
