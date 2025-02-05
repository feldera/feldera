package org.dbsp.sqlCompiler.compiler.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DeclareViewStatement;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.HSQDBManager;
import org.dbsp.util.NameGen;
import org.dbsp.util.Utilities;
import org.hsqldb.server.ServerAcl;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/** Tests about table and view metadata */
public class MetadataTests extends BaseSQLTests {
    @Test
    public void systemView() {
        // Create a view named like a system view
        this.statementsFailingInCompilation("CREATE VIEW ERROR_VIEW AS SELECT 2;",
                "error: Duplicate declaration: 'error_view' already defined");
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
                "Nullable argument: Argument 0 is nullable, while 'f' expects a not nullable value");
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
    public void stripConnectors2() throws IOException, SQLException {
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
        CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        String rust = Utilities.readFile(Paths.get(BaseSQLTests.testFilePath));
        Assert.assertFalse(rust.contains("connectors"));
    }

    @Test
    public void stripConnectors() throws IOException, SQLException {
        // Test that the connectors property is stripped from the generated Rust
        NameGen.reset();
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
        CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        String rust = Utilities.readFile(Paths.get(BaseSQLTests.testFilePath));
        Assert.assertFalse(rust.contains("connectors"));

        NameGen.reset();
        sql = """
               CREATE TABLE T (COL1 INT);
               CREATE VIEW V AS SELECT * FROM T;""";
        file = createInputScript(sql);
        CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        String rust0 = Utilities.readFile(Paths.get(BaseSQLTests.testFilePath));
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
        TestUtil.assertMessagesContain(compiler, "Unused column: Column 'unused' of table 't' is unused");
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
                      }
                    } ],
                    "materialized" : false,
                    "foreign_keys" : [ ]
                  } ]"""));
    }

    @Test
    public void trimUnusedInputColumns() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.ioOptions.trimInputs = true;
        compiler.submitStatementsForCompilation("""
                CREATE TABLE T(used INTEGER, unused INTEGER);
                CREATE TABLE T1(used INTEGER, unused INTEGER) with ('materialized' = 'true');
                CREATE VIEW V AS SELECT used FROM ((SELECT * FROM T) UNION ALL (SELECT * FROM T1));""");
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        DBSPSourceTableOperator input = circuit.getInput(new ProgramIdentifier("t", false));
        Assert.assertNotNull(input);
        DBSPTypeTuple tuple = input.getOutputZSetElementType().to(DBSPTypeTuple.class);
        // Field 'unused' has been dropped
        Assert.assertEquals(1, tuple.size());

        input = circuit.getInput(new ProgramIdentifier("t1", false));
        Assert.assertNotNull(input);
        tuple = input.getOutputZSetElementType().to(DBSPTypeTuple.class);
        // Field 'unused' is not dropped from materialized tables
        Assert.assertEquals(2, tuple.size());
    }

    @Test
    public void issue3427() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.ioOptions.trimInputs = true;
        compiler.options.ioOptions.quiet = false;
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t1(c1 INTEGER);
                CREATE VIEW v1 AS SELECT ELEMENT(ARRAY [2, 3]) FROM t1;""");
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        TestUtil.assertMessagesContain(compiler.messages,
                "Unused column: Column 'c1' of table 't1' is unused");
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
        DBSPSourceTableOperator t = circuit.getInput(new ProgramIdentifier("t", false));
        Assert.assertNotNull(t);
        DBSPType ix = t.getOutputZSetElementType();
        Assert.assertTrue(ix.is(DBSPTypeTuple.class));
        DBSPTypeTuple tuple = ix.to(DBSPTypeTuple.class);
        // The type should not be nullable despite the declaration
        Assert.assertFalse(tuple.tupFields[0].mayBeNull);
    }

    // Test the --unquotedCasing command-line parameter
    @Test
    public void casing() throws IOException, InterruptedException, SQLException {
        String sql = """
                CREATE TABLE "T" (COL1 INT NOT NULL);
                CREATE TABLE "t" (COL1 INT NOT NULL, COL2 DOUBLE NOT NULL);
                // lowercase 'rlike' only works if we lookup function names case-insensitively
                CREATE VIEW V AS SELECT COL1, rlike(COL2, 'asf') FROM "t";""";
        File file = createInputScript(sql);
        CompilerMessages messages = CompilerMain.execute("--unquotedCasing", "lower",
                "-q", "-o", BaseSQLTests.testFilePath, file.getPath());
        messages.print();
        Assert.assertEquals(0, messages.errorCount());
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, true);
    }

    // Test illegal values for the --unquotedCasing command-line parameter
    @Test
    public void illegalCasing() throws IOException, SQLException {
        String sql = """
                CREATE TABLE T (COL1 INT NOT NULL, COL2 DOUBLE NOT NULL);
                CREATE VIEW V AS SELECT COL1 FROM T;""";
        File file = createInputScript(sql);
        CompilerMessages messages = CompilerMain.execute("--unquotedCasing", "to_lower",
                "-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertTrue(messages.errorCount() > 0);
        Assert.assertTrue(messages.toString().contains("Illegal value for option --unquotedCasing"));
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

    // Test that a schema for a table can be retrieved from a JDBC data source
    // in a separate process using a JDBC connection.
    @Test @Ignore("Does not find system table")
    public void jdbcSchemaTest2() throws SQLException, IOException, InterruptedException,
            ServerAcl.AclFormatException, ClassNotFoundException {
        HSQDBManager manager = new HSQDBManager(BaseSQLTests.rustDirectory);
        manager.start();
        Connection connection = manager.getConnection();
        try (Statement s = connection.createStatement()) {
            s.execute("DROP TABLE mytable IF EXISTS");
            s.execute("""
                    create table mytable(
                    id integer not null primary key,
                    strcol varchar(25))
                    """);
        }

        File script = createInputScript("CREATE VIEW V AS SELECT * FROM mytable");
        CompilerMessages messages = CompilerMain.execute(
                "--jdbcSource", manager.getConnectionString(), "-o", BaseSQLTests.testFilePath, script.getPath());
        manager.stop();
        if (messages.errorCount() > 0)
            throw new RuntimeException(messages.toString());
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void testUDFTypeError() throws IOException, SQLException {
        File file = createInputScript("""
                CREATE FUNCTION myfunction(d DATE, i INTEGER) RETURNS VARCHAR NOT NULL;
                CREATE VIEW V AS SELECT myfunction(DATE '2023-10-20', '5');""");
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(1, messages.errorCount());
        Assert.assertTrue(messages.toString().contains(
                "Cannot apply 'myfunction' to arguments of type 'myfunction(<DATE>, <CHAR(1)>)'. " +
                        "Supported form(s): myfunction(<DATE>, <INTEGER>)"));
    }

    @Test
    public void testUDF() throws IOException, InterruptedException, SQLException {
        File file = createInputScript("""
                CREATE FUNCTION contains_number(str VARCHAR NOT NULL, value INTEGER) RETURNS BOOLEAN NOT NULL;
                CREATE VIEW V0 AS SELECT contains_number(CAST('YES: 10 NO:5 MAYBE: 2' AS VARCHAR), 5);
                CREATE FUNCTION "EMPTY"() RETURNS VARCHAR;
                CREATE VIEW V1 AS SELECT "empty"();""");

        File udf = Paths.get(rustDirectory, "udf.rs").toFile();
        PrintWriter script = new PrintWriter(udf, StandardCharsets.UTF_8);
        script.println("""
                use feldera_sqllib::*;
                pub fn contains_number(str: SqlString, value: Option<i32>) -> Result<bool, Box<dyn std::error::Error>> {
                   match value {
                       None => Err("null value".into()),
                       Some(value) => Ok(str.str().contains(&format!("{}", value).to_string())),
                   }
                }
                pub fn EMPTY() -> Result<Option<SqlString>, Box<dyn std::error::Error>> {
                   Ok(Some(SqlString::new()))
                }""");
        script.close();
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        if (messages.errorCount() > 0)
            throw new RuntimeException(messages.toString());
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);

        Path protos = Paths.get(BaseSQLTests.rustDirectory, DBSPCompiler.STUBS_FILE_NAME);
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

                pub fn contains_number(str: SqlString, value: Option<i32>) -> Result<bool, Box<dyn std::error::Error>> {
                    udf::contains_number(
                        str,
                        value)
                }
                pub fn EMPTY() -> Result<Option<SqlString>, Box<dyn std::error::Error>> {
                    udf::EMPTY()
                }""", String.join(System.lineSeparator(), str));
        boolean success = protos.toFile().delete();
        Assert.assertTrue(success);

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
        CompilerMessages messages = CompilerMain.execute("-q", "-o", BaseSQLTests.testFilePath, file.getPath());
        messages.print();
        Assert.assertEquals(0, messages.errorCount());
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
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
                    --lenient
                      Lenient SQL validation.  If true it allows duplicate column names in a\s
                      view\s
                      Default: false
                    --no-restrict-io
                      Do not restrict the types of columns allowed in tables and views
                      Default: false
                    --nowstream
                      Implement NOW as a stream (true) or as an internal operator (false)
                      Default: false
                    --outputsAreSets
                      Ensure that outputs never contain duplicates
                      Default: false
                    --plan
                      Emit the Calcite plan of the optimized program instead of Rust
                      Default: false
                    --streaming
                      Compiling a streaming program, where only inserts are allowed
                      Default: false
                    --trimInputs
                      Do not ingest unused fields of input tables
                      Default: false
                    --unquotedCasing
                      How unquoted identifiers are treated.  Choices are: 'upper', 'lower',\s
                      'unchanged'\s
                      Default: lower
                    -O
                      Optimization level (0, 1, or 2)
                      Default: 2
                    -T
                      Specify logging level for a class (can be repeated)
                      Syntax: -Tkey=value
                      Default: {}
                    -d
                      SQL syntax dialect used
                      Default: ORACLE
                      Possible Values: [BIG_QUERY, ORACLE, MYSQL, MYSQL_ANSI, SQL_SERVER, JAVA]
                    -i
                      Generate an incremental circuit
                      Default: false
                    -je
                      Emit error messages as a JSON array to stderr
                      Default: false
                    -jpg
                      Emit a jpg image of the circuit instead of Rust
                      Default: false
                    -js
                      Emit a JSON file containing the schema of all views and tables involved
                    -o
                      Output file; stdout if null
                      Default: <empty string>
                    -png
                      Emit a png image of the circuit instead of Rust
                      Default: false
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
        File json = File.createTempFile("out", ".json", new File("."));
        json.deleteOnExit();
        CompilerMain.execute("--plan", "-o", json.getPath(), file.getPath());
        String jsonContents = Utilities.readFile(json.toPath());
        Assert.assertEquals("""
                {"error_view":{
                  "rels": [
                    {
                      "id": "0",
                      "relOp": "LogicalTableScan",
                      "table": [
                        "schema",
                        "error_table"
                      ],
                      "inputs": []
                    }
                  ]
                },
                "v1":{
                  "rels": [
                    {
                      "id": "0",
                      "relOp": "LogicalTableScan",
                      "table": [
                        "schema",
                        "t"
                      ],
                      "inputs": []
                    },
                    {
                      "id": "1",
                      "relOp": "LogicalProject",
                      "fields": [
                        "col1"
                      ],
                      "exprs": [
                        {
                          "input": 0,
                          "name": "$0"
                        }
                      ]
                    }
                  ]
                },
                "v2":{
                  "rels": [
                    {
                      "id": "0",
                      "relOp": "LogicalTableScan",
                      "table": [
                        "schema",
                        "t"
                      ],
                      "inputs": []
                    },
                    {
                      "id": "1",
                      "relOp": "LogicalProject",
                      "fields": [
                        "col1"
                      ],
                      "exprs": [
                        {
                          "input": 0,
                          "name": "$0"
                        }
                      ]
                    },
                    {
                      "id": "2",
                      "relOp": "LogicalAggregate",
                      "group": [],
                      "aggs": [
                        {
                          "agg": {
                            "name": "SUM",
                            "kind": "SUM",
                            "syntax": "FUNCTION"
                          },
                          "type": {
                            "type": "INTEGER",
                            "nullable": true
                          },
                          "distinct": false,
                          "operands": [
                            0
                          ],
                          "name": "EXPR$0"
                        }
                      ]
                    }
                  ]
                }
                }""", jsonContents);
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
        File json = File.createTempFile("out", ".json", new File("."));
        json.deleteOnExit();
        File tmp = File.createTempFile("out", ".rs", new File(rustDirectory));
        CompilerMessages message = CompilerMain.execute(
                "-js", json.getPath(), "-o", tmp.getPath(), file.getPath());
        assert message.exitCode == 0;
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
        File json = File.createTempFile("out", ".json", new File("."));
        json.deleteOnExit();
        File tmp = File.createTempFile("out", ".rs", new File(rustDirectory));
        CompilerMessages message = CompilerMain.execute(
                "-js", json.getPath(), "-o", tmp.getPath(), file.getPath());
        @SuppressWarnings("unused")
        boolean success = tmp.delete();
        if (message.exitCode != 0)
            System.err.println(message);
        Assert.assertEquals(message.exitCode, 0);
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
                      }
                    }, {
                      "name" : "col2",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : false,
                        "type" : "DOUBLE"
                      },
                      "default" : "1.0E0"
                    }, {
                      "name" : "col3",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : false,
                        "precision" : 3,
                        "type" : "VARCHAR"
                      }
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
                      }
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
                      }
                    }, {
                      "name" : "col6",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : true,
                        "precision" : 0,
                        "type" : "TIMESTAMP"
                      },
                      "lateness" : "INTERVAL '5 10:10' DAY TO MINUTE"
                    } ],
                    "primary_key" : [ "col3" ],
                    "materialized" : false,
                    "foreign_keys" : [ {
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
                      }
                    }, {
                      "name" : "message",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : false,
                        "precision" : -1,
                        "type" : "VARCHAR"
                      }
                    }, {
                      "name" : "metadata",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : false,
                        "precision" : -1,
                        "type" : "VARCHAR"
                      }
                    } ],
                    "materialized" : false
                  }, {
                    "name" : "v",
                    "case_sensitive" : false,
                    "fields" : [ {
                      "name" : "xCol",
                      "case_sensitive" : false,
                      "columntype" : {
                        "nullable" : false,
                        "type" : "INTEGER"
                      }
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
                      }
                    } ],
                    "materialized" : false
                  } ]
                }""", jsonContents);
    }

    @Test
    public void jsonErrorTest() throws IOException, SQLException {
        File file = createInputScript("CREATE VIEW V AS SELECT * FROM T;");
        CompilerMessages messages = CompilerMain.execute("-je", file.getPath());
        Assert.assertEquals(messages.exitCode, 1);
        Assert.assertEquals(messages.errorCount(), 1);
        String json = messages.toString();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(json);
        Assert.assertNotNull(jsonNode);
        Assert.assertNotNull(jsonNode.get(0).get("snippet").asText());
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
                "Table not found: Table 'unknown', referred in " +
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
                "Column not found: Table 't' does not have a column named 'a'");
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
}
