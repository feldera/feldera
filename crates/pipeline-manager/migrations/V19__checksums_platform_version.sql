-- Adds three fields to the `pipeline` table:
--
-- `platform_version`
-- The Feldera platform version of the pipeline.
--
-- `program_binary_source_checksum`
-- Combined checksum of all the inputs that influenced Rust compilation to a binary.
-- It is non-NULL if the program_status is 'success', and NULL otherwise.
--
-- `program_binary_integrity_checksum`
-- Checksum of the binary file itself.
-- It is non-NULL if the program_status is 'success', and NULL otherwise.

ALTER TABLE pipeline
ADD COLUMN platform_version VARCHAR NOT NULL DEFAULT '',
ADD COLUMN program_binary_source_checksum VARCHAR NULL,
ADD COLUMN program_binary_integrity_checksum VARCHAR NULL;

-- We set the initial source checksum value for legacy pipelines to the pipeline identifier, because it is unique.
UPDATE pipeline SET program_binary_source_checksum = id WHERE program_status = 'success';

-- There is no integrity checksum known at migration, as such it is set to empty string.
UPDATE pipeline SET program_binary_integrity_checksum = '' WHERE program_status = 'success';
