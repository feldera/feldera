-- Adds the `refresh_version` field to the `pipeline` table.

ALTER TABLE pipeline
ADD COLUMN refresh_version BIGINT NOT NULL DEFAULT 1;
UPDATE pipeline SET refresh_version = version;
