-- Create tables and tasks in Snowflake to run the demo.
--
-- Run this script using the following command line, where <random_schema_name>
-- can be generated, e.g., by `uuidgen` for each run of the demo:
--
-- snowsql -D schema_name=<random_schema_name> -f setup.sql

!set variable_substitution=true

USE ROLE CI_ROLE_1;
USE DATABASE CI;

-- Create target schema.  In real-world use, the schema will exist in the
CREATE SCHEMA &{schema_name};
USE SCHEMA &{schema_name};

create table PRICE (
    part bigint not null,
    vendor bigint not null,
    created timestamp,
    effective_since date,
    price decimal
);

create table PREFERRED_VENDOR (
    part_id bigint not null,
    part_name varchar not null,
    vendor_id bigint not null,
    vendor_name varchar not null,
    price decimal
);

-- Create schema for landing tables by copying target table declarations.
CREATE SCHEMA &{schema_name}_landing CLONE &{schema_name};
USE SCHEMA &{schema_name}_landing;

-- Remove all data from cloned tables.  This isn't necessary in this case, because original
-- tables are still empty, but this is how it would normally be done in scenarious where
-- we want to stream udpates to existing tables.  Note that Snowflake uses copy-on-write,
-- so cloning a table and then instantly truncating it should be free.
TRUNCATE PRICE;
TRUNCATE PREFERRED_VENDOR;

-- TODO: drop all constraints on the landing tables.

-- Add metadata columns to store 'insert' or 'delete' action and unique row id.
ALTER TABLE PRICE add
    __action STRING NOT NULL,
    __stream_id NUMBER NOT NULL,
    __seq_number NUMBER NOT NULL;
ALTER TABLE PRICE add
    unique (__stream_id, __seq_number);

ALTER TABLE PREFERRED_VENDOR add
    __action STRING NOT NULL,
    __stream_id NUMBER NOT NULL,
    __seq_number NUMBER NOT NULL;
ALTER TABLE PREFERRED_VENDOR add
    unique (__stream_id, __seq_number);

-- If input data contains unknown columns, schema evolution will make sure that those columns
-- get stored in the landing table, so the user can later act on them.
ALTER TABLE PRICE SET ENABLE_SCHEMA_EVOLUTION = TRUE;
ALTER TABLE PREFERRED_VENDOR SET ENABLE_SCHEMA_EVOLUTION = TRUE;


-- Create append-only streams over landing tables.
CREATE STREAM PRICE_STREAM ON TABLE PRICE APPEND_ONLY = TRUE;
CREATE STREAM PREFERRED_VENDOR_STREAM ON TABLE PREFERRED_VENDOR APPEND_ONLY = TRUE;

!set sql_delimiter=/

-- Create tasks to ingest updates from streams into target tables.
CREATE TASK INGEST_DATA
  SCHEDULE = '1 minute'
  -- By not specifying a warehouse to run the task, we opt for the serverless
  -- model, which is likely more cost-effective for use in CI.  Real-world
  -- deployments should probably use a user-managed warehouse.
  --
  -- NOTE: for the CI user to be able to create serverless tasks, we had to
  -- grant it the `EXECUTE MANAGED TASK` privileged on the Snowflake account.
  --WAREHOUSE = COMPUTE_WH
  WHEN
    ((SYSTEM$STREAM_HAS_DATA('PRICE_STREAM')) or (SYSTEM$STREAM_HAS_DATA('PREFERRED_VENDOR_STREAM')))
  AS
    BEGIN
        START TRANSACTION;

        -- Move data from the stream to the target table.
        MERGE INTO &{schema_name}.PRICE AS T
        USING (
            -- We may have ingested multiple updates for the same key, which
            -- must be applied in the logical order in which they arrived;
            -- however the `MERGE` construct ignores ordering.  We use
            -- `max(__seq_number) .. GROUP BY` to identify the last update
            -- for the given key, which, assuming that the input collection
            -- is a set (all weights are 1), is the only one that needs to take
            -- effect, since all previous updates get canceled out).
            SELECT * FROM PRICE_STREAM where (__stream_id, __seq_number)
                in (SELECT __stream_id, max(__seq_number) as __seq_number FROM PRICE_STREAM GROUP BY (part, vendor, created, effective_since, price, __stream_id))
        ) AS S ON (T.part = S.part and T.vendor = S.vendor and T.created = S.created and T.effective_since = S.effective_since and T.price = S.price)
        WHEN MATCHED AND S.__action = 'delete' THEN
            DELETE
        WHEN NOT MATCHED AND S.__action = 'insert' THEN
            INSERT (part, vendor, created, effective_since, price)
            VALUES (S.part, S.vendor, S.created, S.effective_since, S.price);
        -- Delete all ingested records from the landing table.
        -- We do this in the same transaction, before the stream
        -- offset advances.
        DELETE from PRICE WHERE (__stream_id, __seq_number) in (SELECT __stream_id, __seq_number FROM PRICE_STREAM);

        MERGE INTO &{schema_name}.PREFERRED_VENDOR AS T
        USING (
            SELECT * from PREFERRED_VENDOR_STREAM where (__stream_id, __seq_number)
                in (SELECT __stream_id, max(__seq_number) as __seq_number FROM PREFERRED_VENDOR_STREAM GROUP BY (part_id, part_name, __stream_id))
        ) AS S ON (T.part_id = S.part_id and T.part_name = S.part_name)
        WHEN MATCHED AND S.__action = 'delete' THEN
            DELETE
        -- This clause is only needed for tables with a unique key when updating a values
        WHEN MATCHED AND S.__action = 'insert' THEN
            UPDATE SET T.vendor_id = S.vendor_id, T.vendor_name = S.vendor_name, T.price = S.price
        WHEN NOT MATCHED AND S.__action = 'insert' THEN
            INSERT (part_id, part_name, vendor_id, vendor_name, price)
            VALUES (S.part_id, S.part_name, S.vendor_id, S.vendor_name, S.price);
        DELETE from PREFERRED_VENDOR WHERE (__stream_id, __seq_number) in (SELECT __stream_id, __seq_number FROM PREFERRED_VENDOR_STREAM);

        COMMIT;
    END;/

!set sql_delimiter=";"

alter task ingest_data resume;