ALTER TABLE program
ADD COLUMN jit_mode bool NOT NULL DEFAULT FALSE;

ALTER TABLE program_history
ADD COLUMN jit_mode bool NOT NULL DEFAULT FALSE;