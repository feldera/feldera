-- Adds the `suspend_info` field to the `pipeline` table.

ALTER TABLE pipeline
ADD COLUMN suspend_info VARCHAR NULL;
