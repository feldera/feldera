-- This default compilation profile is only required for pre-existing programs
ALTER TABLE program
ADD COLUMN compilation_profile varchar NOT NULL DEFAULT 'unoptimized';
