CREATE TABLE IF NOT EXISTS tenant (
            id uuid PRIMARY KEY,
            tenant varchar NOT NULL,
            provider varchar NOT NULL,
            UNIQUE (tenant, provider)
);
CREATE TABLE IF NOT EXISTS program (
            id uuid PRIMARY KEY,
            version bigint NOT NULL,
            tenant_id uuid NOT NULL,
            name varchar NOT NULL,
            description varchar NOT NULL,
            code varchar NOT NULL,
            schema varchar,
            status varchar,
            error varchar,
            status_since timestamp NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE,
            -- Used for a foreign key in the pipeline table. This makes it easy for the DB to check that pipelines can only be made of programs that belong to the same tenant.
            UNIQUE(id, tenant_id),
            UNIQUE(tenant_id, name));
CREATE TABLE IF NOT EXISTS pipeline (
            id uuid PRIMARY KEY,
            program_id uuid,
            version bigint NOT NULL,
            tenant_id uuid NOT NULL,
            name varchar NOT NULL,
            description varchar NOT NULL,
            config varchar NOT NULL,
            -- TODO: add 'host' field when we support remote pipelines.
            port smallint,
            status varchar NOT NULL,
            created bigint,
            FOREIGN KEY (program_id, tenant_id) REFERENCES program(id, tenant_id) ON DELETE CASCADE,
            UNIQUE(tenant_id, name));
CREATE TABLE IF NOT EXISTS connector (
            id uuid NOT NULL PRIMARY KEY,
            tenant_id uuid NOT NULL,
            name varchar NOT NULL,
            description varchar NOT NULL,
            config varchar NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE,
            UNIQUE(tenant_id, name));
CREATE TABLE IF NOT EXISTS attached_connector (
            pipeline_id uuid NOT NULL,
            connector_id uuid NOT NULL,
            tenant_id uuid NOT NULL,
            name varchar,
            config varchar,
            is_input bool NOT NULL,
            PRIMARY KEY (pipeline_id, name),
            FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE,
            FOREIGN KEY (connector_id) REFERENCES connector(id) ON DELETE CASCADE,
            FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS api_key (
            hash varchar PRIMARY KEY,
            tenant_id uuid NOT NULL,
            scopes text[] NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE);