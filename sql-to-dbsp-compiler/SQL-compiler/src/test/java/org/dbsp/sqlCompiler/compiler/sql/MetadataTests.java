package org.dbsp.sqlCompiler.compiler.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DeclareViewStatement;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.NameGen;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/** Tests about table and view metadata */
public class MetadataTests extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.ioOptions.trimInputs = true;
        return options;
    }

    File createTempJsonFile() throws IOException {
        File file = File.createTempFile("out", ".json", new File("."));
        file.deleteOnExit();
        return file;
    }

    @Test
    public void systemView() {
        // Create a view named like a system view
        this.statementsFailingInCompilation("CREATE VIEW ERROR_VIEW AS SELECT 2;",
                "'error_view' already defined");
    }

    @Test
    public void issue3743() throws IOException, InterruptedException, SQLException {
        File file = createInputScript("""
                CREATE TYPE X AS (x int);
                CREATE FUNCTION f(arg X) RETURNS X;
                CREATE FUNCTION g(x int NOT NULL) RETURNS ROW(a INT, b INT) NOT NULL;
                CREATE VIEW V AS SELECT f(X(1)), g(2).a;""");

        File udf = Paths.get(RUST_DIRECTORY, "udf.rs").toFile();
        PrintWriter script = new PrintWriter(udf, StandardCharsets.UTF_8);
        script.println("""
                use crate::{Tup1, Tup2};
                use feldera_sqllib::*;
                pub fn f(x: Option<Tup1<Option<i32>>>) -> Result<Option<Tup1<Option<i32>>>, Box<dyn std::error::Error>> {
                   match x {
                      None => Ok(None),
                      Some(x) => match x.0 {
                         None => Ok(Some(Tup1::new(None))),
                         Some(x) => Ok(Some(Tup1::new(Some(x + 1)))),
                      }
                   }
                }
                
                pub fn g(x: i32) -> Result<Tup2<i32, i32>, Box<dyn std::error::Error>> {
                   Ok(Tup2::new(x-1, x+1))
                }""");
        script.close();
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.TEST_FILE_PATH, file.getPath());
        if (messages.errorCount() > 0)
            throw new RuntimeException(messages.toString());
        Utilities.compileAndTestRust(BaseSQLTests.RUST_DIRECTORY, false);
    }

    @Test
    public void issue3637() throws IOException, SQLException {
        String sql = """
                CREATE TABLE t (id VARCHAR);
                
                DECLARE RECURSIVE VIEW v(
                    id VARCHAR,
                    parent_id VARCHAR
                );
                
                CREATE MATERIALIZED VIEW v
                AS SELECT id,
                    -- Delta lake output connector using field type Null instead of using the explicit VARCHAR
                    NULL AS parent_id
                FROM t""";
        File file = createInputScript(sql);
        File json = this.createTempJsonFile();
        CompilerMessages message = CompilerMain.execute(
                "-js", json.getPath(), "--noRust", file.getPath());
        Assert.assertEquals(0, message.exitCode);
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        for (JsonNode out: parsed.get("outputs")) {
            if (out.get("name").asText().equals("v'")) {
                ArrayNode fields = (ArrayNode)out.get("fields");
                String type = fields.get(1).get("columnType").get("type").asText();
                Assert.assertEquals("VARCHAR", type);
            }
        }
    }

    @Test
    public void issue4183() throws IOException, SQLException {
        String sql = "WRONG";
        File file = createInputScript(sql);
        File errorFile = File.createTempFile("err", ".txt", new File("."));
        errorFile.deleteOnExit();
        CompilerMain.runAndReportErrors("--noRust", file.getPath(), "--errors", errorFile.getPath());
        String str = Utilities.readFile(errorFile.getPath());
        Assert.assertTrue(str.contains("Error parsing SQL"));
    }

    @Test
    public void lineageTest() throws SQLException, IOException {
        // Check that the calcite property in the dataflow graph is never "null" for this program
        final String file = "../../demo/packaged/sql/08-fine-grained-authorization.sql";
        File json = this.createTempJsonFile();
        CompilerMain.execute("--dataflow", json.getPath(), "--noRust", file);
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        ObjectNode df = (ObjectNode)parsed.get("mir");
        for (var prop: df.properties()) {
            JsonNode calcite = prop.getValue().get("calcite");
            if (calcite != null)
                // Nested nodes do not have this property
                Assert.assertFalse(calcite.isNull());
        }
    }

    @Test
    public void issue3341() {
        String sql = """
                CREATE FUNCTION F(x INTEGER NOT NULL) RETURNS INTEGER;
                CREATE TABLE T(x INTEGER);
                CREATE VIEW V AS SELECT F(x) FROM T;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.ioOptions.quiet = false;
        compiler.submitStatementsForCompilation(sql);
        compiler.getFinalCircuit(true);
        TestUtil.assertMessagesContain(compiler,
                "Argument 0 is nullable, while 'f' expects a not nullable value");
    }

    @Test
    public void propertiesTest() {
        String ddl = """
               CREATE TABLE T (
                  COL1 INT
               ) WITH (
                  'connectors' = '[{
                    "name": "kafka",
                    "url": "localhost"
                  }]'
               );
               CREATE VIEW V WITH (
                  'connectors' = '[{
                     "name": "file_input",
                     "path": "/tmp/x"
                  }]'
               ) AS SELECT * FROM T;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(ddl);
        getCircuit(compiler);
        JsonNode meta = compiler.getIOMetadataAsJson();
        JsonNode inputs = meta.get("inputs");
        Assert.assertNotNull(inputs);
        Assert.assertTrue(inputs.isArray());
        JsonNode c = inputs.get(0).get("properties");
        Assert.assertNotNull(c);
        String str = c.toPrettyString();
        Assert.assertEquals("""
                {
                  "connectors" : {
                    "value" : "[{\\n     \\"name\\": \\"kafka\\",\\n     \\"url\\": \\"localhost\\"\\n   }]",
                    "key_position" : {
                      "start_line_number" : 4,
                      "start_column" : 4,
                      "end_line_number" : 4,
                      "end_column" : 15
                    },
                    "value_position" : {
                      "start_line_number" : 4,
                      "start_column" : 19,
                      "end_line_number" : 7,
                      "end_column" : 6
                    }
                  }
                }""", str);

        JsonNode outputs = meta.get("outputs");
        Assert.assertNotNull(inputs);
        Assert.assertTrue(outputs.isArray());
        // error_view and declared view
        Assert.assertEquals(2, outputs.size());
        c = outputs.get(1).get("properties");
        Assert.assertNotNull(c);
        str = c.toPrettyString();
        Assert.assertEquals("""
                {
                  "connectors" : {
                    "value" : "[{\\n      \\"name\\": \\"file_input\\",\\n      \\"path\\": \\"/tmp/x\\"\\n   }]",
                    "key_position" : {
                      "start_line_number" : 10,
                      "start_column" : 4,
                      "end_line_number" : 10,
                      "end_column" : 15
                    },
                    "value_position" : {
                      "start_line_number" : 10,
                      "start_column" : 19,
                      "end_line_number" : 13,
                      "end_column" : 6
                    }
                  }
                }""", str);
    }

    @Test
    public void stripProperties1() throws IOException, SQLException {
        NameGen.reset();
        String sql = """
                CREATE TABLE CUSTOMER (
                    cc_num BIGINT NOT NULL PRIMARY KEY, -- Credit card number
                    name varchar,                       -- Customer name
                    lat DOUBLE,                         -- Customer home address latitude
                    long DOUBLE                         -- Customer home address longitude
                ) WITH (
                    'materialized' = 'true',
                    -- Configure the random data generator to generate 100000 customer records.
                    -- (see https://www.feldera.com/docs/connectors/sources/datagen)
                    'connectors' = '[{
                      "transport": {
                        "name": "datagen",
                        "config": {
                          "plan": [{
                            "limit": 100000,
                            "fields": {
                              "name": { "strategy": "name" },
                              "cc_num": { "range": [ 100000000000000, 100000000100000 ] },
                              "lat": { "strategy": "uniform", "range": [ 25, 50 ] },
                              "long": { "strategy": "uniform", "range": [ -126, -67 ] }
                            }
                          }]
                        }
                      }
                    }]'
                );
                
                CREATE TABLE TRANSACTION (
                    ts TIMESTAMP LATENESS INTERVAL 10 MINUTES, -- Transaction time
                    amt DOUBLE,                                -- Transaction amount
                    cc_num BIGINT NOT NULL,                    -- Credit card number
                    shipping_lat DOUBLE,                       -- Shipping address latitude
                    shipping_long DOUBLE,                      -- Shipping address longitude
                    FOREIGN KEY (cc_num) REFERENCES CUSTOMER(cc_num)
                ) WITH (
                    'materialized' = 'true',
                    -- Configure the random data generator to generate 1M transactions at the rate of 1000 transactions/s.
                    'connectors' = '[{
                      "transport": {
                      "name": "datagen",
                        "config": {
                          "plan": [{
                            "limit": 1000000,
                            "rate": 1000,
                            "fields": {
                              "ts": { "strategy": "increment", "scale": 1000, "range": [1722063600000,2226985200000] },
                              "amt": { "strategy": "zipf", "range": [ 1, 10000 ] },
                              "cc_num": { "strategy": "uniform", "range": [ 100000000000000, 100000000100000 ] },
                              "shipping_lat": { "strategy": "uniform", "range": [ 25, 50 ] },
                              "shipping_long": { "strategy": "uniform", "range": [ -126, -67 ] }
                            }
                          }]
                        }
                      }
                    }]'
                );
                
                CREATE VIEW TRANSACTION_WITH_DISTANCE AS
                    SELECT
                        t.*,
                        ST_DISTANCE(ST_POINT(shipping_long, shipping_lat), ST_POINT(long,lat)) AS distance
                    FROM
                        TRANSACTION as t
                        LEFT JOIN CUSTOMER as c
                        ON t.cc_num = c.cc_num;
                
                -- Compute two rolling aggregates over a 1-day time window for each transaction:
                -- 1. Average spend per transaction.
                -- 2. The number of transactions whose shipping address is more than 50,000 meters away from
                --    the card holder's home address.
                CREATE VIEW TRANSACTION_WITH_AGGREGATES AS
                SELECT
                   *,
                   AVG(amt) OVER window_1_day as avg_1day,
                   SUM(case when distance > 50000 then 1 else 0 end) OVER window_1_day as count_1day
                FROM
                  TRANSACTION_WITH_DISTANCE
                WINDOW window_1_day AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 1 DAYS PRECEDING AND CURRENT ROW);
                """;
        File file = createInputScript(sql);
        CompilerMain.execute("-o", BaseSQLTests.TEST_FILE_PATH, file.getPath());
        String rust = Utilities.readFile(Paths.get(BaseSQLTests.TEST_FILE_PATH));
        Assert.assertFalse(rust.contains("connectors"));
    }

    @Test
    public void issue4896() {
        String sql = """
               CREATE TABLE T (COL1 INT) WITH (
                  'connectors' = '[{
                    "url": "localhost"
                  }]'
               );""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.ioOptions.quiet = false;
        compiler.submitStatementsForCompilation(sql);
        // Force compilation
        compiler.getFinalCircuit(true);
        Assert.assertTrue(compiler.messages.toString().contains(
                "warning: Unnamed connector: Connector nr. 1 for Table 't' does not have a name.\n" +
                "It is recommended to name all connectors using the \"name\" property; " +
                        "names will be required in the future."));

        sql = """
               CREATE TABLE T (COL1 INT) WITH (
                  'connectors' = '[{
                    "name": [],
                    "url": "localhost"
                  }]'
               );""";
        this.statementsFailingInCompilation(sql,
                "Expected a string value for the connector \"name\" property");

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
        this.statementsFailingInCompilation(sql,
                "Connector 'Bob' for Table 't' must have a unique name per table/view");
    }

    @Test
    public void stripProperties() throws IOException, SQLException {
        // Test that the properties are stripped from the generated Rust
        String sql = """
               CREATE TABLE T (
                  COL1 INT
               ) WITH (
                  'connectors' = '[{
                    "name": "kafka",
                    "url": "localhost"
                  }]'
               );
               CREATE VIEW V WITH (
                  'connectors' = '[{
                     "name": "file_input",
                     "path": "/tmp/x"
                  }]'
               ) AS SELECT * FROM T;""";
        File file = createInputScript(sql);
        CompilerMain.execute("-o", BaseSQLTests.TEST_FILE_PATH, file.getPath());
        String rust = Utilities.readFile(BaseSQLTests.TEST_FILE_PATH);
        Assert.assertFalse(rust.contains("connectors"));

        sql = """
               CREATE TABLE T (COL1 INT);
               CREATE VIEW V AS SELECT * FROM T;""";
        file = createInputScript(sql);
        CompilerMain.execute("-o", BaseSQLTests.TEST_FILE_PATH, file.getPath());
        String rust0 = Utilities.readFile(getTestFilePath());
        Assert.assertEquals(rust, rust0);
    }

    @Test
    public void illegalPropertiesTest() {
        String ddl = """
               CREATE TABLE T (
                  COL1 INT
               ) WITH (
                  'connector' = 'kafka',
                  'connector' = 'localhost'
               );
               CREATE VIEW V AS SELECT * FROM T;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.submitStatementsForCompilation(ddl);
        TestUtil.assertMessagesContain(compiler, "Duplicate key");
        TestUtil.assertMessagesContain(compiler, "Previous declaration");
    }

    @Test
    public void materializedProperty() {
        String ddl = "CREATE VIEW V WITH ('materialized' = 'true') AS SELECT 5;";
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.ioOptions.quiet = false;
        compiler.submitStatementsForCompilation(ddl);
        TestUtil.assertMessagesContain(compiler, "please use 'CREATE MATERIALIZED VIEW' instead");
    }

    @Test
    public void unusedInputColumns() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.ioOptions.quiet = false;
        compiler.submitStatementsForCompilation("""
                CREATE TABLE T(used INTEGER, unused INTEGER);
                CREATE TABLE T1(used INTEGER, unused INTEGER) with ('materialized' = 'true');
                CREATE VIEW V AS SELECT used FROM ((SELECT * FROM T) UNION ALL (SELECT * FROM T1));""");
        TestUtil.assertMessagesContain(compiler, "Column 'unused' of table 't' is unused");
    }

    @Test
    public void testNestedMap() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation("""
                CREATE TABLE fails (
                        j ROW(
                            s MAP<VARCHAR, ROW(
                                t VARCHAR
                            )>
                        )
                    );""");
        compiler.getFinalCircuit(false);
        ObjectNode node = compiler.getIOMetadataAsJson();
        String json = node.toPrettyString();
        Assert.assertTrue(json.contains("""
                {
                  "inputs" : [ {
                    "name" : "fails",
                    "case_sensitive" : false,
                    "fields" : [ {
                      "name" : "j",
                      "case_sensitive" : false,
                      "columntype" : {
                        "fields" : [ {
                          "key" : {
                            "nullable" : false,
                            "precision" : -1,
                            "type" : "VARCHAR"
                          },
                          "name" : "s",
                          "nullable" : false,
                          "type" : "MAP",
                          "value" : {
                            "fields" : [ {
                              "name" : "t",
                              "nullable" : false,
                              "precision" : -1,
                              "type" : "VARCHAR"
                            } ],
                            "nullable" : true
                          }
                        } ],
                        "nullable" : true
                      },
                      "unused" : false
                    } ],
                    "materialized" : false,
                    "foreign_keys" : [ ]
                  } ]"""));
    }

    @Test
    public void trimUnusedInputColumns() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation("""
                CREATE TABLE T(used INTEGER, unused INTEGER);
                CREATE TABLE T1(used INTEGER, unused INTEGER) with ('materialized' = 'true');
                CREATE VIEW V AS SELECT used FROM ((SELECT * FROM T) UNION ALL (SELECT * FROM T1));""");
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        IInputOperator input = circuit.getInput(new ProgramIdentifier("t", false));
        Assert.assertNotNull(input);
        DBSPTypeTuple tuple = input.getDataOutputType().to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class);
        // Field 'unused' has been dropped
        Assert.assertEquals(1, tuple.size());

        input = circuit.getInput(new ProgramIdentifier("t1", false));
        Assert.assertNotNull(input);
        tuple = input.getDataOutputType().to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class);
        // Field 'unused' is not dropped from materialized tables
        Assert.assertEquals(2, tuple.size());

        ObjectNode json = compiler.getIOMetadataAsJson();
        JsonNode t1 = json.get("inputs").get(1);
        Assert.assertEquals("t1", t1.get("name").asText());

        JsonNode unused = t1.get("fields").get(1);
        Assert.assertTrue(unused.get("unused").asBoolean());
        JsonNode used = t1.get("fields").get(0);
        Assert.assertFalse(used.get("unused").asBoolean());
    }

    @Test
    public void issue3427() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.ioOptions.quiet = false;
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t1(c1 INTEGER);
                CREATE VIEW v1 AS SELECT ELEMENT(ARRAY [2, 3]) FROM t1;""");
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        TestUtil.assertMessagesContain(compiler.messages,
                "Column 'c1' of table 't1' is unused");
    }

    @Test
    public void issue3706() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.ioOptions.quiet = false;
        compiler.submitStatementsForCompilation("""
                CREATE TABLE T(x INT, y INT);
                CREATE VIEW V as (SELECT * FROM T) UNION ALL (SELECT y, x FROM T);
                """);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        TestUtil.assertMessagesContain(compiler.messages, """
                While compiling:
                    2|CREATE VIEW V as (SELECT * FROM T) UNION ALL (SELECT y, x FROM T);
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                warning: Fields reordered: The input collections of a 'UNION' operation have columns with the same names, but in a different order.  This may be a mistake.
                Note that UNION never reorders fields.
                First type: (x INTEGER, y INTEGER)
                Mismatched type: (y INTEGER, x INTEGER)""");
    }

    @Test
    public void nullKey() {
        String ddl = """
               CREATE TABLE T (
                  COL1 INT PRIMARY KEY
               );
               CREATE VIEW V AS SELECT * FROM T;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.languageOptions.lenient = true;  // produces warning for primary key
        compiler.options.ioOptions.quiet = false; // show warnings
        compiler.submitStatementsForCompilation(ddl);
        DBSPCircuit circuit = getCircuit(compiler);
        TestUtil.assertMessagesContain(compiler, "PRIMARY KEY cannot be nullable");
        IInputOperator t = circuit.getInput(new ProgramIdentifier("t", false));
        Assert.assertNotNull(t);
        DBSPType ix = t.getDataOutputType().to(DBSPTypeIndexedZSet.class).elementType;
        Assert.assertTrue(ix.is(DBSPTypeTuple.class));
        DBSPTypeTuple tuple = ix.to(DBSPTypeTuple.class);
        // The type should not be nullable despite the declaration
        Assert.assertFalse(tuple.tupFields[0].mayBeNull);
    }

    @Test
    public void casing() throws IOException, InterruptedException, SQLException {
        String sql = """
                CREATE TABLE "T" (COL1 INT NOT NULL);
                CREATE TABLE "t" (COL1 INT NOT NULL, COL2 DOUBLE NOT NULL);
                CREATE VIEW V AS SELECT COL1, rlike(COL2, 'asf') FROM "t";""";
        File file = createInputScript(sql);
        CompilerMessages messages = CompilerMain.execute(
                "-q", "-o", BaseSQLTests.TEST_FILE_PATH, file.getPath());
        messages.print();
        Assert.assertEquals(0, messages.errorCount());
        Utilities.compileAndTestRust(BaseSQLTests.RUST_DIRECTORY, true);
    }

    // Test that schema for a table can be retrieved from a JDBC data source
    @Test @Ignore("Does not find system table")
    public void jdbcSchemaTest() throws ClassNotFoundException, SQLException {
        // Create a table in HSQLDB
        Class.forName("org.hsqldb.jdbcDriver");
        String jdbcUrl = "jdbc:hsqldb:mem:db";
        Connection connection = DriverManager.getConnection(jdbcUrl, "", "");
        try (Statement s = connection.createStatement()) {
            s.execute("""
                    create table mytable(
                    id integer not null primary key,
                    strcol varchar(25))
                    """);
        }

        // Create a schema that retrieves data from HSQLDB
        DataSource mockDataSource = JdbcSchema.dataSource(jdbcUrl, "org.hsqldb.jdbcDriver", "", "");
        Connection executorConnection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = executorConnection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        JdbcSchema hsql = JdbcSchema.create(rootSchema, "schema", mockDataSource, null, null);

        CompilerOptions options = new CompilerOptions();
        options.languageOptions.throwOnError = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.addSchemaSource("schema", hsql);
        compiler.submitStatementForCompilation("CREATE VIEW V AS SELECT * FROM mytable");
        this.getCCS(compiler);
        ObjectNode node = compiler.getIOMetadataAsJson();
        String json = node.toPrettyString();
        Assert.assertTrue(json.contains("MYTABLE"));
    }

    @Test
    public void testUDFTypeError() throws IOException, SQLException {
        File file = createInputScript("""
                CREATE FUNCTION myfunction(d DATE, i INTEGER) RETURNS VARCHAR NOT NULL;
                CREATE VIEW V AS SELECT myfunction(DATE '2023-10-20', '5');""");
        CompilerMessages messages = CompilerMain.execute("--noRust", file.getPath());
        Assert.assertEquals(1, messages.errorCount());
        Assert.assertTrue(messages.toString().contains(
                "Cannot apply 'myfunction' to arguments of type 'myfunction(<DATE>, <CHAR(1)>)'. " +
                        "Supported form(s): myfunction(<DATE>, <INTEGER>)"));
    }

    @Test
    public void testUDA() throws IOException, InterruptedException, SQLException {
        File file = createInputScript("""
                CREATE LINEAR AGGREGATE I8_AVG(s TINYINT) RETURNS TINYINT;
                CREATE TABLE T(x TINYINT);
                CREATE VIEW V AS SELECT I8_AVG(x) FROM T;""");

        File udf = Paths.get(RUST_DIRECTORY, "udf.rs").toFile();
        PrintWriter script = new PrintWriter(udf, StandardCharsets.UTF_8);
        script.println("""
                use feldera_sqllib::*;
                use crate::Tup2;
                
                pub type i8_avg_accumulator_type = Tup2<i32, i32>;
                
                pub fn i8_avg_map(val: i8) -> i8_avg_accumulator_type {
                    Tup2::new(val as i32, 1)
                }
                
                pub fn i8_avg_post(val: i8_avg_accumulator_type) -> i8 {
                    (val.0 / val.1).try_into().unwrap()
                }
                """);
        script.close();
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.TEST_FILE_PATH, file.getPath());
        if (messages.errorCount() > 0)
            throw new RuntimeException(messages.toString());
        Utilities.compileAndTestRust(BaseSQLTests.RUST_DIRECTORY, false);

        // Truncate file to 0 bytes
        FileWriter writer = new FileWriter(udf);
        writer.close();
    }

    @Test
    public void testUDA2() throws IOException, InterruptedException, SQLException {
        File file = createInputScript("""
                CREATE LINEAR AGGREGATE I128_SUM(s BINARY(16)) RETURNS BINARY(16);
                CREATE TABLE T(x BINARY(16), y BINARY(16) NOT NULL);
                CREATE MATERIALIZED VIEW V AS SELECT I128_SUM(x) AS S, I128_SUM(y) AS N FROM T;""");

        // Save a copy of cargo.toml
        Path cargo = Paths.get(RUST_DIRECTORY, "..", "Cargo.toml");
        Path cargoBackup = Paths.get(RUST_DIRECTORY, "..", "Cargo.toml.bak");
        try {
            Files.copy(cargo, cargoBackup, StandardCopyOption.REPLACE_EXISTING);
            String cargoContents = Utilities.readFile(cargo);
            cargoContents = cargoContents.replace("[dependencies]",
                    """
                            [dependencies]
                            i256 = { version = "0.2.2", features = ["num-traits"] }
                            num-traits = "0.2.19"
                            """);
            PrintWriter p = new PrintWriter(cargo.toFile(), StandardCharsets.UTF_8);
            p.write(cargoContents);
            p.close();

            File udf = Paths.get(RUST_DIRECTORY, "udf.rs").toFile();
            PrintWriter script = new PrintWriter(udf, StandardCharsets.UTF_8);
            script.println("""
                    use i256::I256;
                    use feldera_sqllib::*;
                    use crate::{AddAssignByRef, AddByRef, HasZero, MulByRef, SizeOf, Tup3};
                    use derive_more::Add;
                    use num_traits::Zero;
                    use rkyv::Fallible;
                    use std::ops::{Add, AddAssign};
                    
                    #[derive(Add, Clone, Debug, Default, PartialOrd, Ord, Eq, PartialEq, Hash)]
                    pub struct I256Wrapper {
                        pub data: I256,
                    }
                    
                    impl SizeOf for I256Wrapper {
                        fn size_of_children(&self, context: &mut size_of::Context) {}
                    }
                    
                    impl From<[u8; 32]> for I256Wrapper {
                        fn from(value: [u8; 32]) -> Self {
                            Self { data: I256::from_be_bytes(value) }
                        }
                    }
                    
                    impl From<&[u8]> for I256Wrapper {
                        fn from(value: &[u8]) -> Self {
                            let mut padded = [0u8; 32];
                            // If original value is negative, pad with sign
                            if value[0] & 0x80 != 0 {
                                padded.fill(0xff);
                            }
                            let len = value.len();
                            if len > 32 {
                                panic!("Slice larger than target");
                            }
                            padded[32-len..].copy_from_slice(&value[..len]);
                            Self { data: I256::from_be_bytes(padded) }
                        }
                    }
                    
                    impl MulByRef<Weight> for I256Wrapper {
                        type Output = Self;
                    
                        fn mul_by_ref(&self, other: &Weight) -> Self::Output {
                            println!("Mul {:?} by {}", self, other);
                            Self {
                                data: self.data.checked_mul_i64(*other)
                                    .expect("Overflow during multiplication"),
                            }
                        }
                    }
                    
                    impl HasZero for I256Wrapper {
                        fn zero() -> Self {
                            Self { data: I256::zero() }
                        }
                    
                        fn is_zero(&self) -> bool {
                            self.data.is_zero()
                        }
                    }
                    
                    impl AddByRef for I256Wrapper {
                        fn add_by_ref(&self, other: &Self) -> Self {
                            Self { data: self.data.add(other.data) }
                        }
                    }
                    
                    impl AddAssignByRef<Self> for I256Wrapper {
                        fn add_assign_by_ref(&mut self, other: &Self) {
                            self.data += other.data
                        }
                    }
                    
                    #[repr(C)]
                    #[derive(Debug, Copy, Clone, PartialOrd, Ord, Eq, PartialEq)]
                    pub struct ArchivedI256Wrapper {
                        pub bytes: [u8; 32],
                    }
                    
                    impl rkyv::Archive for I256Wrapper {
                        type Archived = ArchivedI256Wrapper;
                        type Resolver = ();
                    
                        #[inline]
                        unsafe fn resolve(&self, pos: usize, _: Self::Resolver, out: *mut Self::Archived) {
                            out.write(ArchivedI256Wrapper {
                                bytes: self.data.to_be_bytes(),
                            });
                        }
                    }
                    
                    impl<S: Fallible + ?Sized> rkyv::Serialize<S> for I256Wrapper {
                        #[inline]
                        fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
                            Ok(())
                        }
                    }
                    
                    impl<D: Fallible + ?Sized> rkyv::Deserialize<I256Wrapper, D> for ArchivedI256Wrapper {
                        #[inline]
                        fn deserialize(&self, _: &mut D) -> Result<I256Wrapper, D::Error> {
                            Ok(I256Wrapper::from(self.bytes))
                        }
                    }
                    
                    pub type i128_sum_accumulator_type = I256Wrapper;
                    
                    pub fn i128_sum_map(val: ByteArray) -> i128_sum_accumulator_type {
                        I256Wrapper::from(val.as_slice())
                    }
                    
                    pub fn i128_sum_post(val: i128_sum_accumulator_type) -> ByteArray {
                        // Check for overflow
                        if val.data < I256::from(i128::MIN) || val.data > I256::from(i128::MAX) {
                            panic!("Result of aggregation {} does not fit in 128 bits", val.data);
                        }
                        ByteArray::new(&val.data.to_be_bytes()[16..])
                    }
                    """);
            script.close();
            CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.TEST_FILE_PATH, file.getPath());
            if (messages.errorCount() > 0)
                throw new RuntimeException(messages.toString());
            Utilities.compileAndTestRust(BaseSQLTests.RUST_DIRECTORY, false);
            // Truncate udf file to 0 bytes
            FileWriter writer = new FileWriter(udf);
            writer.close();
        } finally {
            // Restore cargo.toml
            Files.copy(cargoBackup, cargo, StandardCopyOption.REPLACE_EXISTING);
            Utilities.deleteFile(cargoBackup.toFile(), true);
        }
    }

    @Test
    public void testUDF() throws IOException, InterruptedException, SQLException {
        File file = createInputScript("""
                CREATE FUNCTION contains_number(str VARCHAR NOT NULL, value DECIMAL(2, 0)) RETURNS BOOLEAN NOT NULL;
                CREATE VIEW V0 AS SELECT contains_number(CAST('YES: 10 NO:5.1 MAYBE: 2' AS VARCHAR), 5.1);
                CREATE FUNCTION "EMPTY"() RETURNS VARCHAR;
                CREATE VIEW V1 AS SELECT "EMPTY"();""");

        File udf = Paths.get(RUST_DIRECTORY, "udf.rs").toFile();
        PrintWriter script = new PrintWriter(udf, StandardCharsets.UTF_8);
        script.println("""
                use feldera_sqllib::*;
                pub fn contains_number(str: SqlString, value: Option<SqlDecimal<2, 0>>) -> Result<bool, Box<dyn std::error::Error>> {
                   match value {
                       None => Err("null value".into()),
                       Some(value) => Ok(str.str().contains(&format!("{}", value).to_string())),
                   }
                }
                pub fn EMPTY() -> Result<Option<SqlString>, Box<dyn std::error::Error>> {
                   Ok(Some(SqlString::new()))
                }""");
        script.close();
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.TEST_FILE_PATH, file.getPath());
        if (messages.errorCount() > 0)
            throw new RuntimeException(messages.toString());
        Utilities.compileAndTestRust(BaseSQLTests.RUST_DIRECTORY, false);

        Path protos = Paths.get(BaseSQLTests.RUST_DIRECTORY, DBSPCompiler.STUBS_FILE_NAME);
        Assert.assertTrue(protos.toFile().exists());
        List<String> str = Files.readAllLines(protos);
        Assert.assertEquals("""
                // Compiler-generated file.
                // This file contains stubs for user-defined functions declared in the SQL program.
                // Each stub defines a function prototype that must be implemented in `udf.rs`.
                // Copy these stubs to `udf.rs`, replacing their bodies with the actual UDF implementation.
                // See detailed documentation in https://docs.feldera.com/sql/udf.

                #![allow(non_snake_case)]

                use feldera_sqllib::*;
                use crate::*;
                pub fn contains_number(str: SqlString, value: Option<SqlDecimal<2, 0>>) -> Result<bool, Box<dyn std::error::Error>> {
                    udf::contains_number(
                        str,
                        value)
                }
                
                pub fn EMPTY() -> Result<Option<SqlString>, Box<dyn std::error::Error>> {
                    udf::EMPTY()
                }
                """, String.join(System.lineSeparator(), str));
        Utilities.deleteFile(protos.toFile(), true);

        // Truncate file to 0 bytes
        FileWriter writer = new FileWriter(udf);
        writer.close();
    }

    @Test
    public void testDefaultColumnValueCompiler() throws IOException, InterruptedException, SQLException {
        String sql = """
                CREATE TABLE T (COL1 INT NOT NULL DEFAULT 0, COL2 DOUBLE DEFAULT 0.0, COL3 VARCHAR DEFAULT NULL);
                CREATE VIEW V AS SELECT COL1 FROM T;""";
        File file = createInputScript(sql);
        CompilerMessages messages = CompilerMain.execute("-q", "-o", BaseSQLTests.TEST_FILE_PATH, file.getPath());
        messages.print();
        Assert.assertEquals(0, messages.errorCount());
        Utilities.compileAndTestRust(BaseSQLTests.RUST_DIRECTORY, false);
    }

    @Test
    public void testHelpMessage() throws SQLException {
        // If this test fails you should update sql-to-dbsp-compiler/using.md
        PrintStream save = System.out;
        ByteArrayOutputStream capture = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capture));
        CompilerMain.execute("-h");
        System.setOut(save);
        String captured = capture.toString();
        Assert.assertEquals("""
                Usage: sql-to-dbsp [options] Input file to compile
                  Options:
                    --alltables
                      Generate an input for each CREATE TABLE, even if the table is not used\s
                      by any view
                      Default: false
                    --crates
                      Followed by a program name. Generates code using multiple crates;\s
                      `outputFile` is interpreted as a directory.
                      Default: <empty string>
                    --dataflow
                      Emit the Dataflow graph of the program in the specified JSON file
                    --enterprise
                      Generate code supporting enterprise features
                      Default: false
                    --errors
                      Error output file; stderr if not specified
                      Default: <empty string>
                    --handles
                      Use handles (true) or Catalog (false) in the emitted Rust code
                      Default: false
                    -h, --help, -?
                      Show this message and exit
                    --ignoreOrder
                      Ignore ORDER BY clauses at the end
                      Default: false
                    --jdbcSource
                      Connection string to a database that contains table metadata
                      Default: <empty string>
                    --je, -je
                      Emit error messages as a JSON array to the error output
                      Default: false
                    --jpg, -jpg
                      Emit a jpg image of the circuit instead of Rust
                      Default: false
                    --js, -js
                      Emit a JSON file containing the schema of all views and tables in the\s
                      specified file.
                    --lenient
                      Lenient SQL validation.  If true it allows duplicate column names in a\s
                      view.\s
                      Default: false
                    --no-restrict-io
                      Do not restrict the types of columns allowed in tables and views
                      Default: false
                    --noRust
                      Do not generate Rust output files
                      Default: false
                    --outputsAreSets
                      Ensure that outputs never contain duplicates
                      Default: false
                    --plan
                      Emit the Calcite plan of the program in the specified JSON file
                    --png, -png
                      Emit a png image of the circuit instead of Rust
                      Default: false
                    --streaming
                      Compiling a streaming program, where only inserts are allowed
                      Default: false
                    --trimInputs
                      Do not ingest unused fields of input tables
                      Default: false
                    --unaryPlusNoop
                      Compile unary plus into a no-operation; similar to sqlite
                      Default: false
                    -O
                      Optimization level (0, 1, or 2)
                      Default: 2
                    -T
                      Specify logging level for a class (can be repeated)
                      Syntax: -Tkey=value
                      Default: {}
                    -i
                      Generate an incremental circuit
                      Default: false
                    -o
                      Output file; stdout if not specified
                      Default: <empty string>
                    -q
                      Quiet: do not print warnings
                      Default: false
                    -v
                      Output verbosity
                      Default: 0

                """, captured);
    }

    @Test
    public void generatePlanTest() throws IOException, SQLException {
        String sql = """
            CREATE TABLE T (COL1 INT NOT NULL, COL2 DOUBLE NOT NULL);
            CREATE VIEW V1 AS SELECT COL1 FROM T;
            CREATE VIEW V2 AS SELECT SUM(COL1) FROM T;""";
        File file = createInputScript(sql);
        File json = this.createTempJsonFile();
        CompilerMain.execute("--plan", json.getPath(), "--noRust", file.getPath());
        String jsonContents = Utilities.readFile(json.toPath());
        String expected = TestUtil.readStringFromResourceFile("metadataTests-generatePlan.json");
        Assert.assertEquals(expected, jsonContents);
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        Assert.assertNotNull(parsed);
    }

    @Test
    public void generateDFTest() throws IOException, SQLException {
        String sql = """
            CREATE TABLE T (COL1 INT NOT NULL, COL2 DOUBLE NOT NULL);
            CREATE VIEW V AS SELECT SUM(COL1) FROM T;""";
        File file = createInputScript(sql);
        File json = this.createTempJsonFile();
        CompilerMessages msg = CompilerMain.execute(
                "--dataflow", json.getPath(), "--noRust", file.getPath());
        Assert.assertEquals(0, msg.exitCode);
        String jsonContents = Utilities.readFile(json.toPath());
        String expected = TestUtil.readStringFromResourceFile("metadataTests-generateDF.json");
        Assert.assertEquals(expected, jsonContents);
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        Assert.assertNotNull(parsed);
    }

    @Test
    public void issue3861() throws IOException, SQLException {
        String sql = """
            CREATE TABLE T (COL1 INT NOT NULL, COL2 DOUBLE NOT NULL);
            CREATE VIEW V AS SELECT SUM(COL1) FROM T;""";
        File file = createInputScript(sql);
        File json = this.createTempJsonFile();
        CompilerMessages msg = CompilerMain.execute(
                "--dataflow", json.getPath(), file.getPath(), "--noRust");
        Assert.assertEquals(0, msg.exitCode);
        Assert.assertTrue(json.exists());
    }

    @Test
    public void generateDFRecursiveTest() throws IOException, SQLException {
        // input table currently unused
        String sql = """
                DECLARE RECURSIVE view fibonacci(n INT, value INT);
                create table input (x int);
                
                create view fibonacci AS
                (
                    -- Base case: first two Fibonacci numbers
                    select 0 as n, 0 as value
                    union all
                    select 1 as n, 1 as value
                )
                union all
                (
                    -- Compute F(n)=F(n-1)+F(n-2)
                    select
                        prev.n + 1 as n,
                        (prev.value + curr.value) as value
                    from fibonacci as curr
                    join fibonacci as prev
                    on prev.n = curr.n - 1
                    where curr.n < 10 and prev.n < 10
                );""";
        File file = createInputScript(sql);
        File json = this.createTempJsonFile();
        CompilerMessages msg = CompilerMain.execute(
                "--dataflow", json.getPath(), "--noRust", file.getPath());
        Assert.assertEquals(0, msg.exitCode);
        String jsonContents = Utilities.readFile(json.toPath());
        String expected = TestUtil.readStringFromResourceFile("metadataTests-generateDFRecursive.json");
        Assert.assertEquals(expected, jsonContents);
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        Assert.assertNotNull(parsed);
    }

    @Test
    public void testFibonacci() throws IOException, SQLException {
        String sql = """
                DECLARE RECURSIVE view fibonacci(n INT, value INT);
                create table input (x int);
                
                create view fibonacci AS
                (
                    -- Base case: first two Fibonacci numbers
                    select 0 as n, 0 as value
                    union all
                    select 1 as n, 1 as value
                )
                union all
                (
                    -- Compute F(n)=F(n-1)+F(n-2)
                    select
                        prev.n + 1 as n,
                        (prev.value + curr.value) as value
                    from fibonacci as curr
                    join fibonacci as prev
                    on prev.n = curr.n - 1
                    where curr.n < 10 and prev.n < 10
                );
                
                create view fib_outputs as select * from fibonacci;""";
        File file = createInputScript(sql);
        File json = this.createTempJsonFile();
        CompilerMessages msg = CompilerMain.execute(
                "-js", json.getPath(), "--noRust", file.getPath());
        Assert.assertEquals(0, msg.exitCode);
        String js = Utilities.readFile(json.toPath());
        Assert.assertFalse(js.contains("fibonacci" + DeclareViewStatement.declSuffix));
    }

    @Test
    public void testSchema() throws IOException, SQLException {
        String sql = """
                CREATE TABLE T (
                COL1 INT NOT NULL
                , COL2 DOUBLE NOT NULL FOREIGN KEY REFERENCES S(COL0) DEFAULT 1e0
                , COL3 VARCHAR(3) NOT NULL PRIMARY KEY
                , COL4 VARCHAR(3) ARRAY
                , COL5 MAP<INT, INT>
                , COL6 TIMESTAMP LATENESS INTERVAL '5 10:10' DAYS TO MINUTES
                );
                CREATE VIEW V AS SELECT COL1 AS "xCol" FROM T;
                CREATE VIEW V1 ("yCol") AS SELECT COL1 FROM T;""";
        File file = createInputScript(sql);
        File json = this.createTempJsonFile();
        CompilerMessages message = CompilerMain.execute(
                "-js", json.getPath(), "--noRust", file.getPath());
        if (message.exitCode != 0)
            System.err.println(message);
        Assert.assertEquals(0, message.exitCode);
        TestUtil.assertMessagesContain(message,
                "Table 's', referred in FOREIGN KEY constraint of table 't', does not exist");
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        Assert.assertNotNull(parsed);
        String jsonContents = Utilities.readFile(json.toPath());
        Assert.assertEquals("""
                {
                  "inputs" : [ {
                    "name" : "t",
                    "case_sensitive" : false,
                    "fields" : [ {
                      "name" : "col1",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : false,
                        "type" : "INTEGER"
                      },
                      "unused" : false
                    }, {
                      "name" : "col2",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : false,
                        "type" : "DOUBLE"
                      },
                      "default" : "1.0E0",
                      "unused" : true
                    }, {
                      "name" : "col3",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : false,
                        "precision" : 3,
                        "type" : "VARCHAR"
                      },
                      "unused" : false
                    }, {
                      "name" : "col4",
                      "case_sensitive" : false,
                      "columntype" : {
                        "component" : {
                          "nullable" : true,
                          "precision" : 3,
                          "type" : "VARCHAR"
                        },
                        "nullable" : true,
                        "type" : "ARRAY"
                      },
                      "unused" : true
                    }, {
                      "name" : "col5",
                      "case_sensitive" : false,
                      "columntype" : {
                        "key" : {
                          "nullable" : false,
                          "type" : "INTEGER"
                        },
                        "nullable" : true,
                        "type" : "MAP",
                        "value" : {
                          "nullable" : true,
                          "type" : "INTEGER"
                        }
                      },
                      "unused" : true
                    }, {
                      "name" : "col6",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : true,
                        "precision" : 3,
                        "type" : "TIMESTAMP"
                      },
                      "lateness" : "INTERVAL '5 10:10' DAY TO MINUTE",
                      "unused" : true
                    } ],
                    "primary_key" : [ "col3" ],
                    "materialized" : false,
                    "foreign_keys" : [ {
                      "source" : "t",
                      "columns" : [ "col2" ],
                      "refers" : "s",
                      "tocolumns" : [ "col0" ]
                    } ]
                  } ],
                  "outputs" : [ {
                    "name" : "error_view",
                    "case_sensitive" : false,
                    "fields" : [ {
                      "name" : "table_or_view_name",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : false,
                        "precision" : -1,
                        "type" : "VARCHAR"
                      },
                      "unused" : false
                    }, {
                      "name" : "message",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : false,
                        "precision" : -1,
                        "type" : "VARCHAR"
                      },
                      "unused" : false
                    }, {
                      "name" : "metadata",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : false,
                        "precision" : -1,
                        "type" : "VARCHAR"
                      },
                      "unused" : false
                    } ],
                    "materialized" : false
                  }, {
                    "name" : "v",
                    "case_sensitive" : false,
                    "fields" : [ {
                      "name" : "xCol",
                      "case_sensitive" : true,
                      "columntype" : {
                        "nullable" : false,
                        "type" : "INTEGER"
                      },
                      "unused" : false
                    } ],
                    "materialized" : false
                  }, {
                    "name" : "v1",
                    "case_sensitive" : false,
                    "fields" : [ {
                      "name" : "yCol",
                      "case_sensitive" : true,
                      "columntype" : {
                        "nullable" : false,
                        "type" : "INTEGER"
                      },
                      "unused" : false
                    } ],
                    "materialized" : false
                  } ]
                }""", jsonContents);
    }

    @Test
    public void jsonErrorTest() throws IOException, SQLException {
        File file = createInputScript("CREATE VIEW V AS SELECT * FROM T;");
        CompilerMessages messages = CompilerMain.execute("-je", file.getPath());
        Assert.assertEquals(1, messages.exitCode);
        Assert.assertEquals(1, messages.errorCount());
        String json = messages.toString();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(json);
        Assert.assertNotNull(jsonNode);
        Assert.assertNotNull(jsonNode.get(0).get("snippet").asText());
    }

    @Test
    public void unusedTable() throws IOException, SQLException {
        String sql = """
                CREATE TABLE T(x INT);
                CREATE TABLE S(x INT);
                CREATE VIEW V AS SELECT * FROM s;""";
        File file = createInputScript(sql);
        File json = this.createTempJsonFile();
        CompilerMain.execute("--alltables", "--dataflow", json.getPath(), "--noRust", file.getPath());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(json);
        // table t is not used in the calcite plan, so the lineage is an empty "AND" node.
        Assert.assertEquals("source_multiset",
                jsonNode.get("mir").get("s1").get("operation").asText());
        JsonNode node = jsonNode.get("mir").get("s1").get("calcite").get("and");
        Assert.assertTrue(node.isArray());
        Assert.assertTrue(node.isEmpty());
    }

    @Test
    public void issue3904() throws IOException, SQLException {
        String sql = """
                CREATE TABLE purchase (
                   customer_id INT,
                   ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
                   amount BIGINT
                ) WITH (
                    'materialized' = 'true',
                    'append_only' = 'true'
                );
                
                CREATE TABLE returns (
                    customer_id INT,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
                    amount BIGINT
                ) WITH (
                    'materialized' = 'true'
                );
                
                CREATE TABLE customer (
                    customer_id INT,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 DAY,
                    address VARCHAR
                ) WITH (
                    'materialized' = 'true'
                );
                
                -- Daily MAX purchase amount.
                CREATE MATERIALIZED VIEW daily_max AS
                SELECT
                    TIMESTAMP_TRUNC(ts, DAY) as d,
                    MAX(amount) AS max_amount
                FROM
                    purchase
                GROUP BY
                    TIMESTAMP_TRUNC(ts, DAY);
                
                -- Daily total purchase amount.
                CREATE MATERIALIZED VIEW daily_total AS
                SELECT
                    TIMESTAMP_TRUNC(ts, DAY) as d,
                    SUM(amount) AS total
                FROM
                    purchase
                GROUP BY
                    TIMESTAMP_TRUNC(ts, DAY);
                
                -- Like `daily_total`, but this view uses the 'emit_final' annotation to only
                -- produce the final value of the aggregate at the end of each day.
                CREATE MATERIALIZED VIEW daily_total_final
                WITH ('emit_final' = 'd')
                AS
                SELECT
                    TIMESTAMP_TRUNC(ts, DAY) as d,
                    SUM(amount) AS total
                FROM
                    purchase
                GROUP BY
                    TIMESTAMP_TRUNC(ts, DAY);
                
                -- Daily MAX purchase amount computed using tumbling windows.
                CREATE MATERIALIZED VIEW daily_max_tumbling AS
                SELECT
                    window_start,
                    MAX(amount)
                FROM TABLE(
                  TUMBLE(
                    "DATA" => TABLE purchase,
                    "TIMECOL" => DESCRIPTOR(ts),
                    "SIZE" => INTERVAL 1 DAY))
                GROUP BY
                    window_start;
                
                -- Daily MAX purchase amount computed as a rolling aggregate.
                CREATE MATERIALIZED VIEW daily_max_rolling AS
                SELECT
                    ts,
                    amount,
                    MAX(amount) OVER window_1_day
                FROM purchase
                WINDOW window_1_day AS (ORDER BY ts RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW);
                
                -- Use an OUTER JOIN to compute a daily transaction summary, including daily totals
                -- from `purchase` and `returns` tables.
                CREATE MATERIALIZED VIEW daily_totals AS
                WITH
                    purchase_totals AS (
                        SELECT
                            TIMESTAMP_TRUNC(purchase.ts, DAY) as purchase_date,
                            SUM(purchase.amount) as total_purchase_amount
                        FROM purchase
                        GROUP BY
                            TIMESTAMP_TRUNC(purchase.ts, DAY)
                    ),
                    return_totals AS (
                        SELECT
                            TIMESTAMP_TRUNC(returns.ts, DAY) as return_date,
                            SUM(returns.amount) as total_return_amount
                        FROM returns
                        GROUP BY
                            TIMESTAMP_TRUNC(returns.ts, DAY)
                    )
                SELECT
                    purchase_totals.purchase_date as d,
                    purchase_totals.total_purchase_amount,
                    return_totals.total_return_amount
                FROM
                    purchase_totals
                    FULL OUTER JOIN
                    return_totals
                ON
                    purchase_totals.purchase_date = return_totals.return_date;
                
                -- Use an ASOF JOIN to extract the customer’s address at the time of
                -- purchase from the `customer` table.
                CREATE MATERIALIZED VIEW purchase_with_address AS
                SELECT
                    purchase.ts,
                    purchase.customer_id,
                    customer.address
                FROM purchase
                LEFT ASOF JOIN customer MATCH_CONDITION(purchase.ts >= customer.ts)
                ON purchase.customer_id = customer.customer_id;
                
                -- Use LAG and LEAD operators to lookup previous and next purchase
                -- amounts for each record in the `purchase` table.
                CREATE MATERIALIZED VIEW purchase_with_prev_next AS
                SELECT
                    ts,
                    customer_id,
                    amount,
                    LAG(amount) OVER(PARTITION BY customer_id ORDER BY ts) as previous_amount,
                    LEAD(amount) OVER(PARTITION BY customer_id ORDER BY ts) as next_amount
                FROM
                    purchase;
                
                -- Temporal filter query that uses the current physical time (NOW())
                -- to compute transactions made in the last 7 days.
                CREATE MATERIALIZED VIEW recent_purchases AS
                SELECT * FROM purchase
                WHERE
                    ts >= NOW() - INTERVAL 7 DAYS;""";
        File file = createInputScript(sql);
        File json = this.createTempJsonFile();
        CompilerMessages msg = CompilerMain.execute("-i",
                "--dataflow", json.getPath(), "--noRust", file.getPath());
        Assert.assertEquals(0, msg.exitCode);
        String jsonContents = Utilities.readFile(json.toPath());
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        JsonNode parsed = mapper.readTree(jsonContents);
        Assert.assertNotNull(parsed);
    }

    @Test
    public void issue3926() throws IOException, SQLException {
        String sql = """
                CREATE TYPE my_struct AS (
                   i INT,
                   s VARCHAR
                );
                
                CREATE TABLE t (
                    i INT,
                    ti TINYINT,
                    si SMALLINT,
                    bi BIGINT,
                    r REAL,
                    d DOUBLE,
                    bin VARBINARY,
                    dt DATE,
                    t TIME,
                    ts TIMESTAMP,
                    a INT ARRAY,
                    m MAP<VARCHAR, VARCHAR>,
                    v VARIANT,
                    b BOOLEAN,
                    dc DECIMAL(7,2),
                    s VARCHAR,
                    ms MY_STRUCT
                ) with ('materialized' = 'true');
                
                CREATE FUNCTION bool2bool(i BOOLEAN) RETURNS BOOLEAN;
                CREATE FUNCTION nbool2nbool(i BOOLEAN NOT NULL) RETURNS BOOLEAN NOT NULL;
                
                CREATE FUNCTION i2i(i INT) RETURNS INT;
                CREATE FUNCTION ni2ni(i INT NOT NULL) RETURNS INT NOT NULL;
                
                CREATE FUNCTION ti2ti(i TINYINT) RETURNS TINYINT;
                CREATE FUNCTION nti2nti(i TINYINT NOT NULL) RETURNS TINYINT NOT NULL;
                
                CREATE FUNCTION si2si(i SMALLINT) RETURNS SMALLINT;
                CREATE FUNCTION nsi2nsi(i SMALLINT NOT NULL) RETURNS SMALLINT NOT NULL;
                
                CREATE FUNCTION bi2bi(i BIGINT) RETURNS BIGINT;
                CREATE FUNCTION nbi2nbi(i BIGINT NOT NULL) RETURNS BIGINT NOT NULL;
                
                CREATE FUNCTION r2r(i REAL) RETURNS REAL;
                CREATE FUNCTION nr2nr(i REAL NOT NULL) RETURNS REAL NOT NULL;
                
                CREATE FUNCTION d2d(i DOUBLE) RETURNS DOUBLE;
                CREATE FUNCTION nd2nd(i DOUBLE NOT NULL) RETURNS DOUBLE NOT NULL;
                
                CREATE FUNCTION bin2bin(i VARBINARY) RETURNS VARBINARY;
                CREATE FUNCTION nbin2nbin(i VARBINARY NOT NULL) RETURNS VARBINARY NOT NULL;
                
                CREATE FUNCTION date2date(i DATE) RETURNS DATE;
                CREATE FUNCTION ndate2ndate(i DATE NOT NULL) RETURNS DATE NOT NULL;
                
                CREATE FUNCTION ts2ts(i TIMESTAMP) RETURNS TIMESTAMP;
                CREATE FUNCTION nts2nts(i TIMESTAMP NOT NULL) RETURNS TIMESTAMP NOT NULL;
                
                CREATE FUNCTION t2t(i TIME) RETURNS TIME;
                CREATE FUNCTION nt2nt(i TIME NOT NULL) RETURNS TIME NOT NULL;
                
                CREATE FUNCTION arr2arr(i INT ARRAY) RETURNS INT ARRAY;
                CREATE FUNCTION narr2narr(i INT ARRAY NOT NULL) RETURNS INT ARRAY NOT NULL;
                
                CREATE FUNCTION map2map(i MAP<VARCHAR, VARCHAR>) RETURNS MAP<VARCHAR, VARCHAR>;
                CREATE FUNCTION nmap2nmap(i MAP<VARCHAR, VARCHAR> NOT NULL) RETURNS MAP<VARCHAR, VARCHAR> NOT NULL;
                
                CREATE FUNCTION var2var(i VARIANT) RETURNS VARIANT;
                CREATE FUNCTION nvar2nvar(i VARIANT NOT NULL) RETURNS VARIANT NOT NULL;
                
                CREATE FUNCTION dec2dec(i DECIMAL(7, 2)) RETURNS DECIMAL(7, 2);
                CREATE FUNCTION ndec2ndec(i DECIMAL(7, 2) NOT NULL) RETURNS DECIMAL(7, 2) NOT NULL;
                
                CREATE FUNCTION str2str(i VARCHAR) RETURNS VARCHAR;
                CREATE FUNCTION nstr2nstr(i VARCHAR NOT NULL) RETURNS VARCHAR NOT NULL;
                
                CREATE FUNCTION struct2struct(i my_struct) RETURNS my_struct;
                CREATE FUNCTION nstruct2nstruct(i my_struct NOT NULL) RETURNS my_struct NOT NULL;
                
                CREATE MATERIALIZED VIEW v AS
                SELECT
                    bool2bool(b),
                    nbool2nbool(COALESCE(b, FALSE)),
                    i2i(i),
                    ni2ni(COALESCE(i, 0)),
                    ti2ti(ti),
                    nti2nti(COALESCE(ti, 0)),
                    si2si(si),
                    nsi2nsi(COALESCE(si, 0)),
                    bi2bi(bi),
                    nbi2nbi(COALESCE(bi, 0)),
                    r2r(r),
                    nr2nr(COALESCE(r, 0.0)),
                    d2d(d),
                    nd2nd(COALESCE(d, 0.0)),
                    bin2bin(bin),
                    nbin2nbin(COALESCE(bin, x'')),
                    date2date(dt),
                    ndate2ndate(COALESCE(dt, DATE '2023-01-01')),
                    ts2ts(ts),
                    nts2nts(COALESCE(ts, TIMESTAMP '2023-01-01 00:00:00')),
                    t2t(t),
                    nt2nt(COALESCE(t, TIME '00:00:00')),
                    arr2arr(a),
                    narr2narr(a),
                    map2map(m),
                    nmap2nmap(m),
                    var2var(v),
                    nvar2nvar(COALESCE(v, VARIANTNULL())),
                    dec2dec(dc),
                    ndec2ndec(COALESCE(dc, 0)),
                    str2str(s),
                    nstr2nstr(COALESCE(s, ''))
                FROM
                    t;
                """;
        File file = createInputScript(sql);
        File json = this.createTempJsonFile();
        CompilerMessages msg = CompilerMain.execute("-i",
                "--dataflow", json.getPath(), "--noRust", file.getPath());
        Assert.assertEquals(0, msg.exitCode);
        String jsonContents = Utilities.readFile(json.toPath());
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        JsonNode parsed = mapper.readTree(jsonContents);
        Assert.assertNotNull(parsed);
    }

    @Test
    public void keyValidationTests() {
        this.statementsFailingInCompilation("""
                CREATE TABLE T (
                   c INT PRIMARY KEY
                );""",
                "PRIMARY KEY column 'c' has type INTEGER, which is nullable");
        this.statementsFailingInCompilation("""
                CREATE TABLE T (
                   c INT ARRAY NOT NULL PRIMARY KEY
                );""",
                "PRIMARY KEY column 'c' cannot have type INTEGER ARRAY");
        this.statementsFailingInCompilation("""
                CREATE TABLE T (
                   FOREIGN KEY (a, b) REFERENCES S(a)
                );""",
                "FOREIGN KEY section of table 't' contains 2 columns," +
                        " which does not match the size the REFERENCES, which is 1");
        this.shouldWarn("""
                        CREATE TABLE T (
                           FOREIGN KEY (a) REFERENCES UNKNOWN(a)
                        );""",
                "Table 'unknown', referred in " +
                        "FOREIGN KEY constraint of table 't', does not exist");
        this.statementsFailingInCompilation("""
                CREATE TABLE T (
                   FOREIGN KEY (a) REFERENCES S(a)
                );
                CREATE TABLE S (
                   nokey INT
                );""",
                "The PRIMARY KEY of table 's' does not match the FOREIGN KEY of 't'");
        this.statementsFailingInCompilation("""
                CREATE TABLE T (
                   FOREIGN KEY (a) REFERENCES S(a)
                );
                CREATE TABLE S (
                   key INT NOT NULL PRIMARY KEY
                );""",
                "Table 't' does not have a column named 'a'");
        this.statementsFailingInCompilation("""
                CREATE TABLE T (
                   a INT,
                   FOREIGN KEY (a) REFERENCES S(b)
                );
                CREATE TABLE S (
                   a INT NOT NULL PRIMARY KEY
                );""",
                "Table 's' does not have a column named 'b'");
        this.statementsFailingInCompilation("""
                CREATE TABLE T (
                   a INT,
                   FOREIGN KEY (a) REFERENCES S(b)
                );
                CREATE TABLE S (
                   a INT NOT NULL PRIMARY KEY,
                   b INT
                );""",
                "FOREIGN KEY column 't.a' refers to column 's.b' which is not a PRIMARY KEY");
        this.statementsFailingInCompilation("""
                CREATE TABLE T (
                   a INT,
                   FOREIGN KEY (a) REFERENCES S(a)
                );
                CREATE TABLE S (
                   a VARCHAR NOT NULL PRIMARY KEY
                );""",
                "FOREIGN KEY column 't.a' has type INT which does " +
                        "not match the type VARCHAR of the referenced column 's.a'");
    }

    @Test
    public void issue4466() throws IOException, SQLException {
        File file = createInputScript("""
                CREATE TABLE tbl(arr1 INT ARRAY);
                CREATE MATERIALIZED VIEW v AS SELECT
                ARRAY_EXISTS(arr1, x -> x > 0)  AS arr
                FROM tbl;""");
        File json = this.createTempJsonFile();
        CompilerMain.execute("--plan", json.getPath(), "--noRust", file.getPath());
        String jsonContents = Utilities.readFile(json.toPath());
        Assert.assertNotNull(jsonContents);
    }
}
