ALTER TABLE program
ADD COLUMN compilation_profile varchar NOT NULL DEFAULT 'unoptimized';
