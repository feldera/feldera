-- Add the `deployment_id` field to the `pipeline` table.
ALTER TABLE pipeline
ADD COLUMN deployment_id UUID NULL;
UPDATE pipeline SET deployment_id = '00000000-0000-0000-0000-000000000000'
                WHERE deployment_status != 'stopping' AND deployment_status != 'stopped';

-- Remove the suspending state as it no longer exists.
UPDATE pipeline SET deployment_status = 'paused'
                WHERE deployment_status = 'suspending';
