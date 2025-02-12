-- Adds the `deployment_check` and `deployment_check_timestamp` fields to the `pipeline` table.

ALTER TABLE pipeline
ADD COLUMN deployment_check VARCHAR NULL,
ADD COLUMN deployment_check_timestamp TIMESTAMP WITH TIME ZONE NULL;

UPDATE pipeline SET deployment_check = ''
                WHERE deployment_status != 'shutting_down'
                  AND deployment_status != 'shutdown';

UPDATE pipeline SET deployment_check_timestamp = NOW()
                WHERE deployment_status != 'shutting_down'
                  AND deployment_status != 'shutdown';
