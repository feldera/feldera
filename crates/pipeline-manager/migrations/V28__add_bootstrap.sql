ALTER TABLE pipeline
ADD COLUMN bootstrap_policy VARCHAR NULL;

ALTER TABLE pipeline
ADD COLUMN deployment_runtime_status_details VARCHAR NULL;