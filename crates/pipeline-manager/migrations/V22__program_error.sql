-- Resets the `program_error` field in the `pipeline` table such
-- that it can be used to hold the new type.

UPDATE pipeline SET program_error = '{}';
ALTER TABLE pipeline ALTER COLUMN program_error SET NOT NULL;
