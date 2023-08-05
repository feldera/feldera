-- Used by the compiler service to record
-- the set of compiled binaries that are
-- currently available and the URLs they
-- can be fetched from
CREATE TABLE compiled_binary (
    program_id uuid NOT NULL,
    version bigint NOT NULL,
    url varchar NOT NULL
);
