CREATE DATABASE dbsp WITH OWNER dbsp;
\c dbsp dbsp;

CREATE TABLE project (
    id bigint,
    version bigint,
    name varchar,
    code varchar,
    status varchar,
    error varchar,
    status_since timestamp,
    PRIMARY KEY (id)
);

CREATE SEQUENCE project_id_seq AS bigint;

CREATE TABLE pipeline (
    id bigint,
    project_id bigint,
    project_version bigint,
    -- TODO: add 'host' field when we support remote pipelines.
    port int NOT NULL,
    killed bool NOT NULL,
    created timestamp,
    PRIMARY KEY (id),
    FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE
);

CREATE SEQUENCE pipeline_id_seq AS bigint;

CREATE TABLE project_config (
    id bigint,
    project_id bigint,
    version bigint,
    name varchar,
    config varchar,
    PRIMARY KEY (id),
    FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE
);

CREATE SEQUENCE project_config_id_seq AS bigint;
