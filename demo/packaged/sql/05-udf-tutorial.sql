-- Tutorial: User Defined Function (UDF) (udf-tutorial)
--
-- Learn how to define and implement a UDF in Feldera
-- using a simple example in which to encode incoming
-- binary data as base64.
--
-- WORKFLOW
-- 1. Within the SQL, declare UDFs using CREATE FUNCTION and use them in views
-- 2. Compile the SQL: the SQL compiler generates for each FUNCTION a stub Rust function,
--    which are shown in `stubs.rs`
-- 3. Copy the function signatures from `stubs.rs` into `udf.rs` and implement them there
-- 4. Declare any external crate dependencies in `udf.toml`
--
-- For more detailed documentation: https://docs.feldera.com/sql/udf

-- Binary data entries come in at a rate of 1 per second.
CREATE TABLE binary_data (
    entry VARBINARY
) WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "limit": 1000000,
                    "rate": 1,
                    "fields": {
                        "entry": {
                            "range": [1, 20],
                            "strategy": "uniform",
                            "value": { "strategy": "uniform", "range": [0, 256] }
                        }
                    }
                }]
            }
        }
    }]'
);

-- UDF which encodes binary data as base64.
CREATE FUNCTION base64(s VARBINARY) RETURNS VARCHAR;

-- View of base64-encoded binary data entries.
CREATE MATERIALIZED VIEW base64_data AS (
    SELECT  base64(b.entry)
    FROM    binary_data AS b
);
