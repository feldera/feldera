ALTER TABLE pipeline
ADD COLUMN deployment_resources_status_details VARCHAR NULL;

UPDATE pipeline SET deployment_resources_status_details = '{}'
                WHERE deployment_resources_status = 'provisioned';
