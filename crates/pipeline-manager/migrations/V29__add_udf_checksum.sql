-- Add column for UDF checksum to determine when tests need to be re-run
ALTER TABLE pipeline ADD COLUMN program_binary_udf_checksum TEXT;
