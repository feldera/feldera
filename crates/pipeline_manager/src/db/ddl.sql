CREATE TABLE IF NOT EXISTS program (
            id uuid PRIMARY KEY,
            version bigint NOT NULL,
            name varchar UNIQUE NOT NULL,
            description varchar NOT NULL,
            code varchar NOT NULL,
            schema varchar,
            status varchar,
            error varchar,
            status_since timestamp NOT NULL);
CREATE TABLE IF NOT EXISTS pipeline (
            id uuid PRIMARY KEY,
            program_id uuid,
            version bigint NOT NULL,
            name varchar UNIQUE NOT NULL,
            description varchar NOT NULL,
            config varchar NOT NULL,
            -- TODO: add 'host' field when we support remote pipelines.
            port smallint,
            status varchar NOT NULL,
            created bigint,
            FOREIGN KEY (program_id) REFERENCES program(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS connector (
            id uuid PRIMARY KEY,
            name varchar UNIQUE NOT NULL,
            description varchar NOT NULL,
            config varchar NOT NULL);
CREATE TABLE IF NOT EXISTS attached_connector (
            pipeline_id uuid NOT NULL,
            connector_id uuid NOT NULL,
            name varchar,
            config varchar,
            is_input bool NOT NULL,
            PRIMARY KEY (pipeline_id, name),
            FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE,
            FOREIGN KEY (connector_id) REFERENCES connector(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS api_key (
                hash varchar PRIMARY KEY,
                scopes text[] NOT NULL);