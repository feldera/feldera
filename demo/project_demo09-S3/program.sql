CREATE TABLE test_table(
    id INTEGER NOT NULL PRIMARY KEY
) WITH (
    'connectors' = '[{
        "format": { "name": "csv", },
        "transport": {
            "name": "s3_input",
            "config": {
                "credentials": {
                    "type": "AccessKey",
                    "aws_access_key_id": <S3_ACCESS_KEY>,
                    "aws_secret_access_key": <S3_SECRET_KEY>,
                },
                "bucket_name": "feldera-connector-test-bucket",
                "region": "us-west-1",
                "read_strategy": {
                    "type": "Prefix",
                    "prefix": "foo",
                }
            }
    }}]'
);

CREATE VIEW test_view AS SELECT * FROM test_table;
