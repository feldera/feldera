package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Assert;
import org.junit.Test;

import java.util.stream.Collectors;

/** Tests for validation of connector format and transport configs. */
public class ConnectorTests extends BaseSQLTests {
    /** Compiles SQL, asserts exit code 0, and verifies each message appears in compiler output. */
    private void runConnectorTest(String sql, String... expectedMessages) {
        DBSPCompiler compiler = this.chattyCompiler();
        compiler.submitStatementsForCompilation(sql);
        compiler.getFinalCircuit(true);
        Assert.assertEquals(0, compiler.messages.exitCode);
        for (String msg : expectedMessages)
            TestUtil.assertMessagesContain(compiler.messages, msg);
    }

    /** Adds four spaces of indentation to every line of {@code body}. */
    private static String indent4(String body) {
        return body.lines()
                   .map(line -> "    " + line)
                   .collect(Collectors.joining("\n"));
    }

    /**
     * Compiles a table input connector with the given connector body (the JSON object
     * content after {@code "name": "c",}), asserts exit code 0, and verifies each
     * expected message appears in compiler output.
     *
     * <p>The generated SQL wraps the body as:
     * <pre>
     * CREATE TABLE T (x INT) WITH (    -- line 1
     *   'connectors' = '[{             -- line 2
     *     "name": "c",                 -- line 3
     *     {connectorBody}              -- lines 4+
     *   }]'
     * );
     * </pre>
     * For a typical {@code "format": {"name": "X", "config": {F}}} body, the first field
     * {@code F} is on line 7 with 8 spaces of indentation.
     */
    private void tableConnectorTest(String connectorBody, String... expectedMessages) {
        runConnectorTest(
                "CREATE TABLE T (x INT) WITH (\n" +
                "  'connectors' = '[{\n" +
                "    \"name\": \"c\",\n" +
                indent4(connectorBody) + "\n" +
                "  }]'\n" +
                ");",
                expectedMessages);
    }

    /**
     * Compiles a view output connector with the given connector body, asserts exit code 0,
     * and verifies each expected message appears in compiler output.
     *
     * <p>The generated SQL wraps the body as:
     * <pre>
     * CREATE TABLE T (x INT);          -- line 1
     * CREATE VIEW V WITH (             -- line 2
     *   'connectors' = '[{             -- line 3
     *     "name": "c",                 -- line 4
     *     {connectorBody}              -- lines 5+
     *   }]'
     * ) AS SELECT * FROM T;
     * </pre>
     * For a typical {@code "format": {"name": "X", "config": {F}}} body, the first field
     * {@code F} is on line 8 with 8 spaces of indentation.
     */
    private void viewConnectorTest(String connectorBody, String... expectedMessages) {
        runConnectorTest(
                "CREATE TABLE T (x INT);\n" +
                "CREATE VIEW V WITH (\n" +
                "  'connectors' = '[{\n" +
                "    \"name\": \"c\",\n" +
                indent4(connectorBody) + "\n" +
                "  }]'\n" +
                ") AS SELECT * FROM T;",
                expectedMessages);
    }

    // ---- Connector structure validation ----

    @Test
    public void formatNotObject() {
        runConnectorTest("""
                CREATE TABLE T (x INT) WITH (
                  'connectors' = '[{
                    "name": "c",
                    "format": "csv"
                  }]'
                );""",
                "\"format\" must be a JSON object");
    }

    @Test
    public void formatMissingName() {
        runConnectorTest("""
                CREATE TABLE T (x INT) WITH (
                  'connectors' = '[{
                    "name": "c",
                    "format": {}
                  }]'
                );""",
                "\"format\" must have a \"name\" field");
    }

    @Test
    public void formatNameNotString() {
        runConnectorTest("""
                CREATE TABLE T (x INT) WITH (
                  'connectors' = '[{
                    "name": "c",
                    "format": {"name": 42}
                  }]'
                );""",
                "\"format.name\" must be a string");
    }

    @Test
    public void formatConfigNotObject() {
        runConnectorTest("""
                CREATE TABLE T (x INT) WITH (
                  'connectors' = '[{
                    "name": "c",
                    "format": {"name": "csv", "config": "bad"}
                  }]'
                );""",
                "\"format.config\" must be a JSON object");
    }

    @Test
    public void transportNotObject() {
        runConnectorTest("""
                CREATE TABLE T (x INT) WITH (
                  'connectors' = '[{
                    "name": "c",
                    "transport": "kafka"
                  }]'
                );""",
                "\"transport\" must be a JSON object");
    }

    @Test
    public void transportMissingName() {
        runConnectorTest("""
                CREATE TABLE T (x INT) WITH (
                  'connectors' = '[{
                    "name": "c",
                    "transport": {}
                  }]'
                );""",
                "\"transport\" must have a \"name\" field");
    }

    @Test
    public void transportNameNotString() {
        runConnectorTest("""
                CREATE TABLE T (x INT) WITH (
                  'connectors' = '[{
                    "name": "c",
                    "transport": {"name": 42}
                  }]'
                );""",
                "\"transport.name\" must be a string");
    }

    @Test
    public void transportConfigNotObject() {
        runConnectorTest("""
                CREATE TABLE T (x INT) WITH (
                  'connectors' = '[{
                    "name": "c",
                    "transport": {"name": "file_input", "config": "bad"}
                  }]'
                );""",
                "\"transport.config\" must be a JSON object");
    }

    @Test
    public void issue4896() {
        // An unnamed connector should produce a warning.
        String sql = """
               CREATE TABLE T (COL1 INT) WITH (
                  'connectors' = '[{
                    "url": "localhost"
                  }]'
               );""";
        DBSPCompiler compiler = this.chattyCompiler();
        compiler.submitStatementsForCompilation(sql);
        compiler.getFinalCircuit(true);
        TestUtil.assertMessagesContain(compiler.messages,
                "warning: Unnamed connector: Connector nr. 1 for table 't' does not have a name.\n" +
                "It is recommended to name all connectors using the \"name\" property; " +
                "names will be required in the future.");

        // A non-string connector name should be a compilation error.
        sql = """
               CREATE TABLE T (COL1 INT) WITH (
                  'connectors' = '[{
                    "name": [],
                    "url": "localhost"
                  }]'
               );""";
        this.statementsFailingInCompilation(sql, """
               Compilation error: Expected a string value for the connector "name" property
                   3|     "name": [],
                                  ^
                   4|     "url": "localhost"
               """);

        // Duplicate connector names on the same table should be a compilation error.
        sql = """
               CREATE TABLE T (COL1 INT) WITH (
                  'connectors' = '[{
                    "name": "Bob",
                    "url": "localhost"
                  }, {
                    "name": "Bob",
                    "url": "localhost:8080"
                  }]'
               );""";
        this.statementsFailingInCompilation(sql, """
               error: Compilation error: Two connectors for the same table 't' cannot have the same name: 'Bob'
                   6|     "name": "Bob",
                                  ^
                   7|     "url": "localhost:8080\"""");
    }

    @Test
    public void testPreprocessorValidation() {
        // Missing "name" field in preprocessor.
        this.statementsFailingInCompilation("""
                CREATE TABLE T(x INT) WITH ('connectors' = '[{
                   "name": "0",
                   "transport": {
                     "name": "datagen",
                     "config": {}
                   },
                   "preprocessor": [{
                      "config": {}
                   }]
                }]');""", """
                Compilation error: Preprocessor must have a field "name"
                    7|   "preprocessor": [{
                                          ^
                    8|      "config": {}""");
        // Missing "message_oriented" field.
        this.statementsFailingInCompilation("""
                CREATE TABLE T(x INT) WITH ('connectors' = '[{
                   "name": "0",
                   "transport": {
                     "name": "datagen",
                     "config": {}
                   },
                   "preprocessor": [{
                      "name": "Bob",
                      "config": {}
                   }]
                }]');""", """
                Compilation error: Preprocessor must have a field "message_oriented"
                    7|   "preprocessor": [{
                                          ^
                    8|      "name": "Bob",""");
        // Wrong type for "message_oriented".
        this.statementsFailingInCompilation("""
                CREATE TABLE T(x INT) WITH ('connectors' = '[{
                   "name": "0",
                   "transport": {
                     "name": "datagen",
                     "config": {}
                   },
                   "preprocessor": [{
                      "message_oriented": "streaming",
                      "name": "Bob",
                      "config": {}
                   }]
                }]');""", """
                Compilation error: Preprocessor field "message_oriented" must be a Boolean value
                    8|      "message_oriented": "streaming",
                                                ^
                    9|      "name": "Bob",""");
    }

    @Test
    public void testPostprocessorValidation() {
        // Missing "name" field in postprocessor.
        this.statementsFailingInCompilation("""
                CREATE TABLE T(x INT);
                CREATE VIEW V WITH ('connectors' = '[{
                   "name": "0",
                   "postprocessor": [{
                      "config": {}
                   }]
                }]') AS SELECT * FROM T;""", """
                Compilation error: Postprocessor must have a field "name"
                    4|   "postprocessor": [{
                                           ^
                    5|      "config": {}""");
    }

    // ---- CSV format config ----

    @Test
    public void csvValidConfig() {
        tableConnectorTest("""
                "format": {
                  "name": "csv",
                  "config": {
                    "delimiter": ";",
                    "headers": true,
                    "double_quote": false,
                    "flexible": false,
                    "trim": "fields"
                  }
                }""");
    }

    @Test
    public void csvNonAsciiDelimiter() {
        tableConnectorTest("""
                "format": {
                  "name": "csv",
                  "config": {
                    "delimiter": "α"
                  }
                }""",
                "field \"delimiter\" must be an ASCII character",
                "    7|        \"delimiter\": \"α\"\n" +
                "              ^");
    }

    @Test
    public void csvSameDelimiterAndQuote() {
        tableConnectorTest("""
                "format": {
                  "name": "csv",
                  "config": {
                    "delimiter": ";",
                    "quote": ";"
                  }
                }""",
                "fields \"delimiter\" and \"quote\" must be different characters",
                "    7|        \"delimiter\": \";\",\n" +
                "              ^");
    }

    @Test
    public void csvUnknownField() {
        tableConnectorTest("""
                "format": {
                  "name": "csv",
                  "config": {
                    "delimter": ";"
                  }
                }""",
                "warning: Invalid configuration: unknown field \"delimter\"\n" +
                "    7|        \"delimter\": \";\"\n" +
                "              ^");
    }

    @Test
    public void csvWrongTypeForBoolean() {
        tableConnectorTest("""
                "format": {
                  "name": "csv",
                  "config": {
                    "headers": "yes"
                  }
                }""",
                "Cannot deserialize value of type `boolean` from String \"yes\"",
                "    7|        \"headers\": \"yes\"\n" +
                "              ^");
    }

    @Test
    public void csvInvalidTrimValue() {
        tableConnectorTest("""
                "format": {
                  "name": "csv",
                  "config": {
                    "trim": "both"
                  }
                }""",
                "field \"trim\": invalid value \"both\"; valid values are: \"none\", \"headers\", \"fields\", \"all\"",
                "    7|        \"trim\": \"both\"\n" +
                "              ^");
    }

    // ---- CSV encoder (output) config ----

    @Test
    public void csvEncoderValidConfig() {
        viewConnectorTest("""
                "format": {
                  "name": "csv",
                  "config": {
                    "delimiter": ";",
                    "buffer_size_records": 500
                  }
                }""");
    }

    @Test
    public void csvEncoderUnknownField() {
        viewConnectorTest("""
                "format": {
                  "name": "csv",
                  "config": {
                    "delimter": ";"
                  }
                }""",
                "warning: Invalid configuration: unknown field \"delimter\"\n" +
                "    8|        \"delimter\": \";\"\n" +
                "              ^");
    }

    @Test
    public void csvEncoderNonAsciiDelimiter() {
        viewConnectorTest("""
                "format": {
                  "name": "csv",
                  "config": {
                    "delimiter": "α"
                  }
                }""",
                "field \"delimiter\" must be an ASCII character",
                "    8|        \"delimiter\": \"α\"\n" +
                "              ^");
    }

    // ---- JSON format config ----

    @Test
    public void jsonParserUnknownField() {
        tableConnectorTest("""
                "format": {
                  "name": "json",
                  "config": {
                    "update_format": "raw",
                    "streem": true
                  }
                }""",
                "warning: Invalid configuration: unknown field \"streem\"\n" +
                "    8|        \"streem\": true\n" +
                "              ^");
    }

    @Test
    public void jsonParserInvalidUpdateFormat() {
        tableConnectorTest("""
                "format": {
                  "name": "json",
                  "config": {
                    "update_format": "upsert"
                  }
                }""",
                "field \"update_format\": invalid value \"upsert\"; valid values are:",
                "    7|        \"update_format\": \"upsert\"\n" +
                "              ^");
    }

    @Test
    public void jsonEncoderKeyFieldsWithoutDebezium() {
        viewConnectorTest("""
                "format": {
                  "name": "json",
                  "config": {
                    "update_format": "raw",
                    "key_fields": ["x"]
                  }
                }""",
                "\"key_fields\" is only valid with \"update_format\": \"debezium\"");
    }

    // ---- Raw format config ----

    @Test
    public void rawParserInvalidMode() {
        tableConnectorTest("""
                "format": {
                  "name": "raw",
                  "config": {
                    "mode": "chunks"
                  }
                }""",
                "field \"mode\": invalid value \"chunks\"; valid values are: \"blob\", \"lines\"",
                "    7|        \"mode\": \"chunks\"\n" +
                "              ^");
    }

    // ---- Parquet format config ----

    @Test
    public void parquetParserUnknownField() {
        tableConnectorTest("""
                "format": {
                  "name": "parquet",
                  "config": {
                    "compress": true
                  }
                }""",
                "warning: Invalid configuration: unknown field \"compress\"\n" +
                "    7|        \"compress\": true\n" +
                "              ^");
    }

    @Test
    public void parquetEncoderUnknownField() {
        viewConnectorTest("""
                "format": {
                  "name": "parquet",
                  "config": {
                    "compress": true
                  }
                }""",
                "warning: Invalid configuration: unknown field \"compress\"\n" +
                "    8|        \"compress\": true\n" +
                "              ^");
    }

    // ---- Avro format config ----

    @Test
    public void avroParserUnknownField() {
        tableConnectorTest("""
                "format": {
                  "name": "avro",
                  "config": {
                    "update_format": "debezium",
                    "compress": true
                  }
                }""",
                "warning: Invalid configuration: unknown field \"compress\"");
    }

    @Test
    public void avroParserSchemaAndRegistryMutuallyExclusive() {
        tableConnectorTest("""
                "format": {
                  "name": "avro",
                  "config": {
                    "schema": "{}",
                    "registry_urls": ["http://localhost:8081"]
                  }
                }""",
                "\"schema\" and \"registry_urls\" are mutually exclusive");
    }

    @Test
    public void avroEncoderCdcFieldRequiresRaw() {
        viewConnectorTest("""
                "format": {
                  "name": "avro",
                  "config": {
                    "update_format": "debezium",
                    "cdc_field": "op"
                  }
                }""",
                "\"cdc_field\" is only valid with \"update_format\": \"raw\"");
    }

    // ---- Delta Table transport config ----

    @Test
    public void deltaReaderMissingMode() {
        tableConnectorTest("""
                "transport": {
                  "name": "delta_table_input",
                  "config": {
                    "uri": "s3://bucket/table"
                  }
                }""",
                "required field \"mode\" is missing");
    }

    @Test
    public void deltaReaderVersionAndDatetimeExclusive() {
        tableConnectorTest("""
                "transport": {
                  "name": "delta_table_input",
                  "config": {
                    "uri": "s3://bucket/table",
                    "mode": "snapshot",
                    "version": 5,
                    "datetime": "2024-01-01T00:00:00Z"
                  }
                }""",
                "\"version\" and \"datetime\" are mutually exclusive");
    }

    @Test
    public void deltaReaderCdcFieldsOutsideCdcMode() {
        tableConnectorTest("""
                "transport": {
                  "name": "delta_table_input",
                  "config": {
                    "uri": "s3://bucket/table",
                    "mode": "snapshot",
                    "cdc_delete_filter": "col IS NULL"
                  }
                }""",
                "\"cdc_delete_filter\" is only valid with \"mode\": \"cdc\"");
    }

    @Test
    public void deltaWriterInvalidLogRetention() {
        viewConnectorTest("""
                "transport": {
                  "name": "delta_table_output",
                  "config": {
                    "uri": "s3://bucket/table",
                    "log_retention_duration": "30 days"
                  }
                }""",
                "invalid \"log_retention_duration\" value \"30 days\": "
                + "expected format \"interval <N> <unit>\"");
    }

    @Test
    public void deltaWriterZeroThreads() {
        viewConnectorTest("""
                "transport": {
                  "name": "delta_table_output",
                  "config": {
                    "uri": "s3://bucket/table",
                    "threads": 0
                  }
                }""",
                "\"threads\" must be greater than 0");
    }

    // ---- HTTP transport config ----

    @Test
    public void httpOutputUnknownField() {
        viewConnectorTest("""
                "transport": {
                  "name": "http_output",
                  "config": {
                    "backpressure": true,
                    "bufffer_size": 100
                  }
                }""",
                "warning: Invalid configuration: unknown field \"bufffer_size\"\n" +
                "    9|        \"bufffer_size\": 100\n" +
                "              ^");
    }

    // ---- S3 transport config ----

    @Test
    public void s3InputMissingRegion() {
        tableConnectorTest("""
                "transport": {
                  "name": "s3_input",
                  "config": {
                    "bucket_name": "my-bucket"
                  }
                }""",
                "required field \"region\" is missing or empty");
    }

    @Test
    public void s3InputMissingBucketName() {
        tableConnectorTest("""
                "transport": {
                  "name": "s3_input",
                  "config": {
                    "region": "us-east-1"
                  }
                }""",
                "required field \"bucket_name\" is missing or empty");
    }

    @Test
    public void s3InputUnknownField() {
        tableConnectorTest("""
                "transport": {
                  "name": "s3_input",
                  "config": {
                    "region": "us-east-1",
                    "bucket_name": "my-bucket",
                    "acl": "public-read"
                  }
                }""",
                "unknown field \"acl\"");
    }

    @Test
    public void s3InputValidConfig() {
        tableConnectorTest("""
                "transport": {
                  "name": "s3_input",
                  "config": {
                    "region": "us-east-1",
                    "bucket_name": "my-bucket",
                    "prefix": "data/",
                    "max_retries": 3
                  }
                }""");
    }

    // ---- Postgres transport config ----

    @Test
    public void postgresReaderUnknownField() {
        tableConnectorTest("""
                "transport": {
                  "name": "postgres_input",
                  "config": {
                    "uri": "postgres://localhost/db",
                    "query": "SELECT * FROM t",
                    "batch_size": 1000
                  }
                }""",
                "unknown field \"batch_size\"");
    }

    @Test
    public void postgresCdcReaderUnknownField() {
        tableConnectorTest("""
                "transport": {
                  "name": "postgres_cdc_input",
                  "config": {
                    "uri": "postgres://localhost/db",
                    "publication": "my_pub",
                    "source_table": "public.orders",
                    "batch_size": 1000
                  }
                }""",
                "unknown field \"batch_size\"");
    }

    @Test
    public void postgresWriterCdcMissingOpColumn() {
        viewConnectorTest("""
                "transport": {
                  "name": "postgres_output",
                  "config": {
                    "uri": "postgres://localhost/db",
                    "table": "t",
                    "mode": "cdc",
                    "cdc_op_column": ""
                  }
                }""",
                "\"cdc_op_column\" cannot be empty in CDC mode");
    }

    @Test
    public void postgresWriterCdcOnConflictDoNothing() {
        viewConnectorTest("""
                "transport": {
                  "name": "postgres_output",
                  "config": {
                    "uri": "postgres://localhost/db",
                    "table": "t",
                    "mode": "cdc",
                    "on_conflict_do_nothing": true
                  }
                }""",
                "\"on_conflict_do_nothing\" is not supported in CDC mode");
    }

    @Test
    public void postgresWriterMaterializedCdcTsColumn() {
        viewConnectorTest("""
                "transport": {
                  "name": "postgres_output",
                  "config": {
                    "uri": "postgres://localhost/db",
                    "table": "t",
                    "cdc_ts_column": "my_ts"
                  }
                }""",
                "\"cdc_ts_column\" must not be set in MATERIALIZED mode");
    }

    @Test
    public void postgresWriterZeroThreads() {
        viewConnectorTest("""
                "transport": {
                  "name": "postgres_output",
                  "config": {
                    "uri": "postgres://localhost/db",
                    "table": "t",
                    "threads": 0
                  }
                }""",
                "\"threads\" must be at least 1");
    }

    @Test
    public void postgresWriterUnknownField() {
        viewConnectorTest("""
                "transport": {
                  "name": "postgres_output",
                  "config": {
                    "uri": "postgres://localhost/db",
                    "table": "t",
                    "batch_size": 1000
                  }
                }""",
                "unknown field \"batch_size\"");
    }

    // ---- Kafka transport config ----

    @Test
    public void kafkaInputMissingTopic() {
        tableConnectorTest("""
                "transport": {
                  "name": "kafka_input",
                  "config": {
                    "bootstrap.servers": "localhost:9092"
                  }
                }""",
                "required field \"topic\" is missing or empty");
    }

    @Test
    public void kafkaInputInvalidLogLevel() {
        tableConnectorTest("""
                "transport": {
                  "name": "kafka_input",
                  "config": {
                    "topic": "my-topic",
                    "log_level": "verbose"
                  }
                }""",
                "field \"log_level\": invalid value \"verbose\"");
    }

    @Test
    public void kafkaOutputMissingTopic() {
        viewConnectorTest("""
                "transport": {
                  "name": "kafka_output",
                  "config": {
                    "bootstrap.servers": "localhost:9092"
                  }
                }""",
                "required field \"topic\" is missing or empty");
    }

    @Test
    public void kafkaOutputInvalidLogLevel() {
        viewConnectorTest("""
                "transport": {
                  "name": "kafka_output",
                  "config": {
                    "topic": "my-topic",
                    "log_level": "verbose"
                  }
                }""",
                "field \"log_level\": invalid value \"verbose\"");
    }

    // ---- Iceberg transport config ----

    @Test
    public void icebergReaderMissingMode() {
        tableConnectorTest("""
                "transport": {
                  "name": "iceberg_input",
                  "config": {
                    "metadata_location": "s3://bucket/table/metadata/v1.json"
                  }
                }""",
                "required field \"mode\" is missing");
    }

    @Test
    public void icebergReaderSnapshotIdAndDatetimeExclusive() {
        tableConnectorTest("""
                "transport": {
                  "name": "iceberg_input",
                  "config": {
                    "mode": "snapshot",
                    "metadata_location": "s3://bucket/table/metadata/v1.json",
                    "snapshot_id": 42,
                    "datetime": "2024-01-01T00:00:00Z"
                  }
                }""",
                "\"snapshot_id\" and \"datetime\" are mutually exclusive");
    }

    @Test
    public void icebergReaderMissingLocation() {
        tableConnectorTest("""
                "transport": {
                  "name": "iceberg_input",
                  "config": {
                    "mode": "snapshot"
                  }
                }""",
                "missing metadata location");
    }

    @Test
    public void icebergReaderMetadataLocationAndCatalogTypeExclusive() {
        tableConnectorTest("""
                "transport": {
                  "name": "iceberg_input",
                  "config": {
                    "mode": "snapshot",
                    "metadata_location": "s3://bucket/table/metadata/v1.json",
                    "catalog_type": "glue",
                    "table_name": "db.orders",
                    "glue.warehouse": "s3://warehouse/"
                  }
                }""",
                "\"metadata_location\" is not supported when \"catalog_type\" is set");
    }

    @Test
    public void icebergReaderMissingTableName() {
        tableConnectorTest("""
                "transport": {
                  "name": "iceberg_input",
                  "config": {
                    "mode": "snapshot",
                    "catalog_type": "glue",
                    "glue.warehouse": "s3://warehouse/"
                  }
                }""",
                "\"table_name\" must be specified when \"catalog_type\" is set");
    }

    @Test
    public void icebergReaderGlueMissingWarehouse() {
        tableConnectorTest("""
                "transport": {
                  "name": "iceberg_input",
                  "config": {
                    "mode": "snapshot",
                    "catalog_type": "glue",
                    "table_name": "db.orders"
                  }
                }""",
                "missing Iceberg warehouse location");
    }

    @Test
    public void icebergReaderGluePropWithoutGlueCatalog() {
        tableConnectorTest("""
                "transport": {
                  "name": "iceberg_input",
                  "config": {
                    "mode": "snapshot",
                    "metadata_location": "s3://bucket/table/metadata/v1.json",
                    "glue.warehouse": "s3://warehouse/"
                  }
                }""",
                "\"glue.warehouse\" is only valid when \"catalog_type\": \"glue\"");
    }

    @Test
    public void icebergReaderRestMissingUri() {
        tableConnectorTest("""
                "transport": {
                  "name": "iceberg_input",
                  "config": {
                    "mode": "snapshot",
                    "catalog_type": "rest",
                    "table_name": "db.orders"
                  }
                }""",
                "missing Iceberg REST catalog URI");
    }

    @Test
    public void icebergReaderInvalidMode() {
        tableConnectorTest("""
                "transport": {
                  "name": "iceberg_input",
                  "config": {
                    "mode": "batch",
                    "metadata_location": "s3://bucket/table/metadata/v1.json"
                  }
                }""",
                "field \"mode\": invalid value \"batch\"");
    }

    // ---- File transport config ----

    @Test
    public void fileInputUnknownField() {
        tableConnectorTest("""
                "transport": {
                  "name": "file_input",
                  "config": {
                    "path": "/data/input.csv",
                    "encoding": "utf-8"
                  }
                }""",
                "unknown field \"encoding\"");
    }

    @Test
    public void fileInputValidConfig() {
        tableConnectorTest("""
                "transport": {
                  "name": "file_input",
                  "config": {
                    "path": "/data/input.csv",
                    "follow": true
                  }
                }""");
    }

    @Test
    public void fileOutputUnknownField() {
        viewConnectorTest("""
                "transport": {
                  "name": "file_output",
                  "config": {
                    "path": "/data/output.csv",
                    "encoding": "utf-8"
                  }
                }""",
                "unknown field \"encoding\"");
    }

    // ---- URL transport config ----

    @Test
    public void urlInputUnknownField() {
        tableConnectorTest("""
                "transport": {
                  "name": "url_input",
                  "config": {
                    "path": "https://example.com/data.csv",
                    "retries": 3
                  }
                }""",
                "unknown field \"retries\"");
    }

    @Test
    public void urlInputValidConfig() {
        tableConnectorTest("""
                "transport": {
                  "name": "url_input",
                  "config": {
                    "path": "https://example.com/data.csv",
                    "pause_timeout": 30
                  }
                }""");
    }

    // ---- Pub/Sub transport config ----

    @Test
    public void pubSubInputSnapshotAndTimestampExclusive() {
        tableConnectorTest("""
                "transport": {
                  "name": "pub_sub_input",
                  "config": {
                    "subscription": "my-sub",
                    "snapshot": "my-snapshot",
                    "timestamp": "2024-08-17T16:39:57-08:00"
                  }
                }""",
                "\"snapshot\" and \"timestamp\" are mutually exclusive");
    }

    @Test
    public void pubSubInputUnknownField() {
        tableConnectorTest("""
                "transport": {
                  "name": "pub_sub_input",
                  "config": {
                    "subscription": "my-sub",
                    "max_messages": 100
                  }
                }""",
                "unknown field \"max_messages\"");
    }

    @Test
    public void pubSubInputValidConfig() {
        tableConnectorTest("""
                "transport": {
                  "name": "pub_sub_input",
                  "config": {
                    "subscription": "my-sub",
                    "project_id": "my-project"
                  }
                }""");
    }

    // ---- DynamoDB transport config ----

    @Test
    public void dynamodbOutputValidConfig() {
        viewConnectorTest("""
                "transport": {
                  "name": "dynamodb_output",
                  "config": {
                    "table": "my-table",
                    "region": "us-east-1"
                  }
                }""");
    }

    @Test
    public void dynamodbOutputTableTooShort() {
        viewConnectorTest("""
                "transport": {
                  "name": "dynamodb_output",
                  "config": {
                    "table": "ab",
                    "region": "us-east-1"
                  }
                }""",
                "DynamoDB requires 3–255 characters");
    }

    @Test
    public void dynamodbOutputTableInvalidChars() {
        viewConnectorTest("""
                "transport": {
                  "name": "dynamodb_output",
                  "config": {
                    "table": "my/table",
                    "region": "us-east-1"
                  }
                }""",
                "contains invalid characters");
    }

    @Test
    public void dynamodbOutputMissingRegion() {
        viewConnectorTest("""
                "transport": {
                  "name": "dynamodb_output",
                  "config": {
                    "table": "my-table"
                  }
                }""",
                "\"region\" cannot be empty");
    }

    @Test
    public void dynamodbOutputBatchSizeExceedsLimit() {
        viewConnectorTest("""
                "transport": {
                  "name": "dynamodb_output",
                  "config": {
                    "table": "my-table",
                    "region": "us-east-1",
                    "write_mode": "batch",
                    "batch_size": 26
                  }
                }""",
                "\"batch_size\" must be between 1 and 25 for \"batch\" write mode");
    }

    @Test
    public void dynamodbOutputCredentialsMustBePaired() {
        viewConnectorTest("""
                "transport": {
                  "name": "dynamodb_output",
                  "config": {
                    "table": "my-table",
                    "region": "us-east-1",
                    "aws_access_key_id": "AKIA..."
                  }
                }""",
                "\"aws_access_key_id\" and \"aws_secret_access_key\" must be specified together");
    }

    @Test
    public void dynamodbOutputZeroThreads() {
        viewConnectorTest("""
                "transport": {
                  "name": "dynamodb_output",
                  "config": {
                    "table": "my-table",
                    "region": "us-east-1",
                    "threads": 0
                  }
                }""",
                "\"threads\" must be at least 1");
    }

    @Test
    public void dynamodbOutputUnknownField() {
        viewConnectorTest("""
                "transport": {
                  "name": "dynamodb_output",
                  "config": {
                    "table": "my-table",
                    "region": "us-east-1",
                    "consistant_reads": true
                  }
                }""",
                "unknown field \"consistant_reads\"");
    }

    // ---- Redis transport config ----

    @Test
    public void redisOutputUnknownField() {
        viewConnectorTest("""
                "transport": {
                  "name": "redis_output",
                  "config": {
                    "connection_string": "redis://localhost:6379/0",
                    "ttl_seconds": 3600
                  }
                }""",
                "unknown field \"ttl_seconds\"");
    }

    @Test
    public void redisOutputValidConfig() {
        viewConnectorTest("""
                "transport": {
                  "name": "redis_output",
                  "config": {
                    "connection_string": "redis://localhost:6379/0",
                    "key_separator": "|"
                  }
                }""");
    }

    // ---- Clock transport config ----

    @Test
    public void clockInputZeroResolution() {
        tableConnectorTest("""
                "transport": {
                  "name": "clock",
                  "config": {
                    "clock_resolution_usecs": 0
                  }
                }""",
                "\"clock_resolution_usecs\" must be greater than 0");
    }

    @Test
    public void clockInputUnknownField() {
        tableConnectorTest("""
                "transport": {
                  "name": "clock",
                  "config": {
                    "clock_resolution_usecs": 1000,
                    "tick_rate": 60
                  }
                }""",
                "unknown field \"tick_rate\"");
    }

    @Test
    public void clockInputValidConfig() {
        tableConnectorTest("""
                "transport": {
                  "name": "clock",
                  "config": {
                    "clock_resolution_usecs": 1000,
                    "http_driven": true
                  }
                }""");
    }

    // ---- NATS transport config ----

    @Test
    public void natsInputValidConfig() {
        tableConnectorTest("""
                "transport": {
                  "name": "nats_input",
                  "config": {
                    "connection_config": {
                      "server_url": "nats://localhost:4222"
                    },
                    "stream_name": "my-stream",
                    "consumer_config": {
                      "deliver_policy": "All"
                    }
                  }
                }""");
    }

    @Test
    public void natsInputValidConfigByStartSequence() {
        tableConnectorTest("""
                "transport": {
                  "name": "nats_input",
                  "config": {
                    "connection_config": {
                      "server_url": "nats://localhost:4222",
                      "connection_timeout_secs": 30
                    },
                    "stream_name": "orders",
                    "inactivity_timeout_secs": 120,
                    "consumer_config": {
                      "deliver_policy": {"ByStartSequence": {"start_sequence": 42}},
                      "replay_policy": "Original",
                      "filter_subjects": ["orders.>"]
                    }
                  }
                }""");
    }

    @Test
    public void natsInputMissingStreamName() {
        tableConnectorTest("""
                "transport": {
                  "name": "nats_input",
                  "config": {
                    "connection_config": {
                      "server_url": "nats://localhost:4222"
                    },
                    "consumer_config": {
                      "deliver_policy": "All"
                    }
                  }
                }""",
                "required field \"stream_name\" is missing or empty");
    }

    @Test
    public void natsInputMissingConnectionConfig() {
        tableConnectorTest("""
                "transport": {
                  "name": "nats_input",
                  "config": {
                    "stream_name": "my-stream",
                    "consumer_config": {
                      "deliver_policy": "All"
                    }
                  }
                }""",
                "required field \"connection_config\" is missing");
    }

    @Test
    public void natsInputMissingServerUrl() {
        tableConnectorTest("""
                "transport": {
                  "name": "nats_input",
                  "config": {
                    "connection_config": {
                      "connection_timeout_secs": 10
                    },
                    "stream_name": "my-stream",
                    "consumer_config": {
                      "deliver_policy": "All"
                    }
                  }
                }""",
                "required field \"connection_config.server_url\" is missing or empty");
    }

    @Test
    public void natsInputMissingDeliverPolicy() {
        tableConnectorTest("""
                "transport": {
                  "name": "nats_input",
                  "config": {
                    "connection_config": {
                      "server_url": "nats://localhost:4222"
                    },
                    "stream_name": "my-stream",
                    "consumer_config": {
                      "replay_policy": "Instant"
                    }
                  }
                }""",
                "required field \"consumer_config.deliver_policy\" is missing");
    }

    @Test
    public void natsInputInvalidReplayPolicy() {
        tableConnectorTest("""
                "transport": {
                  "name": "nats_input",
                  "config": {
                    "connection_config": {
                      "server_url": "nats://localhost:4222"
                    },
                    "stream_name": "my-stream",
                    "consumer_config": {
                      "deliver_policy": "All",
                      "replay_policy": "fast"
                    }
                  }
                }""",
                "invalid value \"fast\"");
    }

    @Test
    public void natsInputUnknownFieldTopLevel() {
        tableConnectorTest("""
                "transport": {
                  "name": "nats_input",
                  "config": {
                    "connection_config": {
                      "server_url": "nats://localhost:4222"
                    },
                    "stream_name": "my-stream",
                    "consumer_config": {
                      "deliver_policy": "All"
                    },
                    "max_reconnects": 5
                  }
                }""",
                "unknown field \"max_reconnects\"");
    }

    @Test
    public void natsInputUnknownFieldInConnectionConfig() {
        tableConnectorTest("""
                "transport": {
                  "name": "nats_input",
                  "config": {
                    "connection_config": {
                      "server_url": "nats://localhost:4222",
                      "tls_enabled": true
                    },
                    "stream_name": "my-stream",
                    "consumer_config": {
                      "deliver_policy": "All"
                    }
                  }
                }""",
                "unknown field \"tls_enabled\"");
    }

    @Test
    public void natsInputZeroInactivityTimeout() {
        tableConnectorTest("""
                "transport": {
                  "name": "nats_input",
                  "config": {
                    "connection_config": {
                      "server_url": "nats://localhost:4222"
                    },
                    "stream_name": "my-stream",
                    "inactivity_timeout_secs": 0,
                    "consumer_config": {
                      "deliver_policy": "All"
                    }
                  }
                }""",
                "\"inactivity_timeout_secs\" must be at least 1");
    }
}
