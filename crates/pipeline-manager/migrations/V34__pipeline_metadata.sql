-- Add client_metadata column to the pipeline table.
-- Holds client-side annotations as a JSON object (e.g. description, tags).
-- Schema-free extensibility: new annotation fields only require extending the Rust
-- ClientMetadata struct, not a DB migration.
ALTER TABLE pipeline ADD COLUMN client_metadata TEXT NOT NULL DEFAULT '';

-- Copy existing description values into client_metadata.description.
-- Descriptions are subject to validation, but existing (migrated) values are exempt until changed
UPDATE pipeline SET client_metadata = json_build_object('description', description)::text WHERE description != '';

-- The standalone description column is superseded by client_metadata.description.
ALTER TABLE pipeline DROP COLUMN description;
