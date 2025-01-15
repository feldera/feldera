-- Adds the `uses_json` field to the `pipeline` table.

ALTER TABLE pipeline
ADD COLUMN uses_json BOOLEAN NOT NULL DEFAULT FALSE;
