-- Add metadata field to the pipeline table.
-- This is arbitrary, optional text provided by the user.
ALTER TABLE pipeline ADD COLUMN metadata TEXT NOT NULL DEFAULT '';

-- Copy existing description values into the metadata JSON "description" field.
UPDATE pipeline SET metadata = json_build_object('description', description)::text WHERE description != '';
