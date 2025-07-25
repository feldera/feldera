-- Remove `program_binary_url` field from the `pipeline` table
ALTER TABLE pipeline
DROP COLUMN program_binary_url;
