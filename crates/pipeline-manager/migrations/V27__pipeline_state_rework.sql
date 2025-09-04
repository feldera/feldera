-- Add the `deployment_id` field to the `pipeline` table.
ALTER TABLE pipeline ADD COLUMN deployment_id UUID NULL;
UPDATE pipeline SET deployment_id = '00000000-0000-0000-0000-000000000000'
                WHERE deployment_status != 'stopping' AND deployment_status != 'stopped';

-- Remove the suspending status as it no longer exists.
UPDATE pipeline SET deployment_status = 'paused'
                WHERE deployment_status = 'suspending';

-- Create new `deployment_resources_*` and `deployment_runtime_*` columns.
ALTER TABLE pipeline ADD COLUMN deployment_resources_status VARCHAR NOT NULL DEFAULT '';
ALTER TABLE pipeline ADD COLUMN deployment_resources_status_since  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
ALTER TABLE pipeline ADD COLUMN deployment_resources_desired_status VARCHAR NOT NULL DEFAULT '';
ALTER TABLE pipeline ADD COLUMN deployment_resources_desired_status_since TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
ALTER TABLE pipeline ADD COLUMN deployment_runtime_status VARCHAR NULL;
ALTER TABLE pipeline ADD COLUMN deployment_runtime_status_since TIMESTAMP WITH TIME ZONE NULL;
ALTER TABLE pipeline ADD COLUMN deployment_runtime_desired_status VARCHAR NULL;
ALTER TABLE pipeline ADD COLUMN deployment_runtime_desired_status_since TIMESTAMP WITH TIME ZONE NULL;

-- deployment_resources_status
UPDATE pipeline SET deployment_resources_status = deployment_status
                WHERE deployment_status = 'provisioning'
                   OR deployment_status = 'stopping'
                   OR deployment_status = 'stopped';
UPDATE pipeline SET deployment_resources_status = 'provisioned'
                WHERE NOT (
                    deployment_status = 'provisioning'
                    OR deployment_status = 'stopping'
                    OR deployment_status = 'stopped'
                );

-- deployment_resources_status_since
-- Already set via DEFAULT NOW()

-- deployment_resources_desired_status
UPDATE pipeline SET deployment_resources_desired_status = 'stopped'
                WHERE deployment_desired_status = 'stopped';
UPDATE pipeline SET deployment_resources_desired_status = 'provisioned'
                WHERE deployment_desired_status != 'stopped';

-- deployment_resources_desired_status_since
-- Already set via DEFAULT NOW()

-- deployment_runtime_status, deployment_runtime_status_since
UPDATE pipeline SET deployment_runtime_status = deployment_status, deployment_runtime_status_since = NOW()
                WHERE NOT (
                    deployment_status = 'provisioning'
                    OR deployment_status = 'stopping'
                    OR deployment_status = 'stopped'
                );

-- deployment_runtime_desired_status, deployment_runtime_desired_status_since
UPDATE pipeline SET deployment_runtime_desired_status = deployment_desired_status, deployment_runtime_desired_status_since = NOW()
                WHERE NOT (
                    deployment_status = 'provisioning'
                    OR deployment_status = 'stopping'
                    OR deployment_status = 'stopped'
                );

-- Column to inform the pipeline of what it should initially become upon startup.
-- It is set when the pipeline is desired to be started, and unset when it is stopping.
ALTER TABLE pipeline ADD COLUMN deployment_initial VARCHAR NULL;
UPDATE pipeline SET deployment_initial = 'paused'
                WHERE deployment_desired_status != 'stopped' OR (
                    deployment_status != 'stopping'
                    AND deployment_status != 'stopped'
                );

-- Finally, we can drop the old columns as they can be inferred from the new ones
ALTER TABLE pipeline DROP COLUMN deployment_status;
ALTER TABLE pipeline DROP COLUMN deployment_status_since;
ALTER TABLE pipeline DROP COLUMN deployment_desired_status;

-- Column to increment to trigger a notification for runtime status to be checked.
ALTER TABLE pipeline ADD COLUMN notify_counter BIGINT DEFAULT 0;
