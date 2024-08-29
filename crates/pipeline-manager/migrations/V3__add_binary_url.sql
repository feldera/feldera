-- Used by the compiler service to record
-- the set of compiled binaries that are
-- currently available and the URLs they
-- can be fetched from
CREATE TABLE compiled_binary (
    program_id uuid NOT NULL,
    version bigint NOT NULL,
    url varchar NOT NULL,
    PRIMARY KEY (program_id, version),
    FOREIGN KEY (program_id) REFERENCES program(id) ON DELETE CASCADE
);

-- A trigger function used to issue notify commands
-- to listening clients. It is intended to be used by
-- reconciliation loops among the pipeline-manager services.
-- This one function works across `programs`, `pipelines` and `pipeline_runtime_state`
-- tables because the fields we access have the same name in both.
--
-- The format of each notification is <operation> <tenant_id> <object_id>,
-- where operation can be A=Add, U=Update, D=Delete. 
CREATE OR REPLACE FUNCTION notification() RETURNS trigger AS $$
    BEGIN
        IF (TG_OP = 'DELETE') THEN
            PERFORM pg_notify(TG_TABLE_NAME, 'D' || ' ' || OLD.tenant_id::text || ' ' || OLD.id::text );
        ELSIF (TG_OP = 'UPDATE') THEN
            PERFORM pg_notify(TG_TABLE_NAME, 'U' || ' ' || NEW.tenant_id::text || ' ' || NEW.id::text );
        ELSIF (TG_OP = 'INSERT') THEN
            PERFORM pg_notify(TG_TABLE_NAME, 'A' || ' ' || NEW.tenant_id::text || ' ' || NEW.id::text );
        END IF;
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

-- Invoke trigger function for changes to the pipeline table
CREATE TRIGGER pipeline_notify 
AFTER INSERT OR UPDATE OR DELETE ON pipeline 
FOR EACH ROW EXECUTE PROCEDURE notification();

-- Invoke trigger function for changes to the pipeline runtime state.
-- INSERT and DELETE notifications always happen alongside pipeline
-- INSERTs AND DELETEs, so we only need to set up an additional trigger
-- for UPDATE events.
CREATE TRIGGER pipeline_runtime_state_notify
AFTER UPDATE ON pipeline_runtime_state
FOR EACH ROW EXECUTE PROCEDURE notification();

-- Invoke trigger function for changes to the program table
CREATE TRIGGER program_notify 
AFTER INSERT OR UPDATE OR DELETE ON program
FOR EACH ROW EXECUTE PROCEDURE notification();
