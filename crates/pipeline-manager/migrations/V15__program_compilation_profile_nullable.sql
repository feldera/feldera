-- The program compilation profile can now also not be specified,
-- in which case the compiler default compilation profile is used.
ALTER TABLE program ALTER COLUMN compilation_profile DROP NOT NULL;
