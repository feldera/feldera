-- `storage_status_details` contains a JSON value encoded as a string.
ALTER TABLE pipeline
    ADD COLUMN storage_status_details VARCHAR NULL;

-- `suspend_info` has been superseded by `storage_status_details`.
ALTER TABLE pipeline
    DROP COLUMN suspend_info;
