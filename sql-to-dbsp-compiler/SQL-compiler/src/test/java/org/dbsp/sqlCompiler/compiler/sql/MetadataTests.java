package org.dbsp.sqlCompiler.compiler.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPControlledFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.HSQDBManager;
import org.dbsp.util.Utilities;
import org.hsqldb.server.ServerAcl;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
    public void connectorPropertiesTest() {
        String ddl = """
               CREATE TABLE T (
                  COL1 INT
               ) WITH (
                  'connector' = 'kafka',
                  'url' = 'localhost'
               );
               CREATE VIEW V WITH (
                  'connector' = 'file',
                  'path' = '/tmp/x'
               ) AS SELECT * FROM T;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(ddl);
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
                  "connector" : {
                    "value" : "kafka",
                    "key_position" : {
                      "start_line_number" : 4,
                      "start_column" : 4,
                      "end_line_number" : 4,
                      "end_column" : 14
                    },
                    "value_position" : {
                      "start_line_number" : 4,
                      "start_column" : 18,
                      "end_line_number" : 4,
                      "end_column" : 24
                    }
                  },
                  "url" : {
                    "value" : "localhost",
                    "key_position" : {
                      "start_line_number" : 5,
                      "start_column" : 4,
                      "end_line_number" : 5,
                      "end_column" : 8
                    },
                    "value_position" : {
                      "start_line_number" : 5,
                      "start_column" : 12,
                      "end_line_number" : 5,
                      "end_column" : 22
                    }
                  }
                }""", str);

        JsonNode outputs = meta.get("outputs");
        Assert.assertNotNull(inputs);
        Assert.assertTrue(outputs.isArray());
        c = outputs.get(0).get("properties");
        Assert.assertNotNull(c);
        str = c.toPrettyString();
        Assert.assertEquals("""
                {
                  "connector" : {
                    "value" : "file",
                    "key_position" : {
                      "start_line_number" : 8,
                      "start_column" : 4,
                      "end_line_number" : 8,
                      "end_column" : 14
                    },
                    "value_position" : {
                      "start_line_number" : 8,
                      "start_column" : 18,
                      "end_line_number" : 8,
                      "end_column" : 23
                    }
                  },
                  "path" : {
                    "value" : "/tmp/x",
                    "key_position" : {
                      "start_line_number" : 9,
                      "start_column" : 4,
                      "end_line_number" : 9,
                      "end_column" : 9
                    },
                    "value_position" : {
                      "start_line_number" : 9,
                      "start_column" : 13,
                      "end_line_number" : 9,
                      "end_column" : 20
                    }
                  }
                }""", str);
    }

    @Test
    public void illegalConnectorPropertiesTest() {
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
        compiler.compileStatements(ddl);
        TestUtil.assertMessagesContain(compiler, "Duplicate key");
        TestUtil.assertMessagesContain(compiler, "Previous declaration");
    }

    @Test
    public void materializedProperty() {
        String ddl = "CREATE VIEW V WITH ('materialized' = 'true') AS SELECT 5;";
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.ioOptions.quiet = false;
        compiler.compileStatements(ddl);
        TestUtil.assertMessagesContain(compiler, "please use 'CREATE MATERIALIZED VIEW' instead");
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
        compiler.compileStatements(ddl);
        DBSPCircuit circuit = getCircuit(compiler);
        TestUtil.assertMessagesContain(compiler, "PRIMARY KEY cannot be nullable");
        DBSPSourceTableOperator t = circuit.getInput("t");
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
        String[] statements = new String[]{
                "CREATE TABLE \"T\" (\n" +
                        "COL1 INT NOT NULL" +
                        ")",
                "CREATE TABLE \"t\" (\n" +
                        "COL1 INT NOT NULL" +
                        ", COL2 DOUBLE NOT NULL" +
                        ")",
                // lowercase 'rlike' only works if we lookup function names case-insensitively
                "CREATE VIEW V AS SELECT COL1, rlike(COL2, 'asf') FROM \"t\""
        };
        File file = createInputScript(statements);
        CompilerMessages messages = CompilerMain.execute("--unquotedCasing", "lower",
                "-q", "-o", BaseSQLTests.testFilePath, file.getPath());
        System.out.println(messages);
        Assert.assertEquals(0, messages.errorCount());
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, true);
    }

    // Test illegal values for the --unquotedCasing command-line parameter
    @Test
    public void illegalCasing() throws IOException, SQLException {
        String[] statements = new String[] {
                """
                CREATE TABLE T (
                COL1 INT NOT NULL
                , COL2 DOUBLE NOT NULL

                )""",
                "CREATE VIEW V AS SELECT COL1 FROM T"
        };
        File file = createInputScript(statements);
        CompilerMessages messages = CompilerMain.execute("--unquotedCasing", "to_lower",
                "-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertTrue(messages.errorCount() > 0);
        Assert.assertTrue(messages.toString().contains("Illegal value for option --unquotedCasing"));
    }

    // Test that schema for a table can be retrieved from a JDBC data source
    @Test
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
        compiler.compileStatement("CREATE VIEW V AS SELECT * FROM mytable");
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        this.addRustTestCase(ccs);
        ObjectNode node = compiler.getIOMetadataAsJson();
        String json = node.toPrettyString();
        Assert.assertTrue(json.contains("MYTABLE"));
    }

    // Test that a schema for a table can be retrieved from a JDBC data source
    // in a separate process using a JDBC connection.
    @Test
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
        File file = createInputScript("CREATE FUNCTION myfunction(d DATE, i INTEGER) RETURNS VARCHAR NOT NULL",
                "CREATE VIEW V AS SELECT myfunction(DATE '2023-10-20', '5')");
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(1, messages.errorCount());
        Assert.assertTrue(messages.toString().contains(
                "Cannot apply 'myfunction' to arguments of type 'myfunction(<DATE>, <CHAR(1)>)'. " +
                        "Supported form(s): myfunction(<DATE>, <INTEGER>)"));
    }

    @Test
    public void testUDF() throws IOException, InterruptedException, SQLException {
        File file = createInputScript(
                "CREATE FUNCTION contains_number(str VARCHAR NOT NULL, value INTEGER) RETURNS BOOLEAN NOT NULL",
                "CREATE VIEW V0 AS SELECT contains_number(CAST('YES: 10 NO:5 MAYBE: 2' AS VARCHAR), 5)",
                "CREATE FUNCTION \"EMPTY\"() RETURNS VARCHAR",
                "CREATE VIEW V1 AS SELECT \"empty\"()");

        File udf = Paths.get(rustDirectory, "udf.rs").toFile();
        PrintWriter script = new PrintWriter(udf, StandardCharsets.UTF_8);
        script.println("""
                use feldera_sqllib::*;
                pub fn contains_number(str: String, value: Option<i32>) -> Result<bool, Box<dyn std::error::Error>> {
                   match value {
                       None => Err("null value".into()),
                       Some(value) => Ok(str.contains(&format!("{}", value).to_string())),
                   }
                }
                pub fn EMPTY() -> Result<Option<String>, Box<dyn std::error::Error>> {
                   Ok(Some("".to_string()))
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
                // Compiler-generated file
                // This file contains stubs for user-defined functions. Copy these stubs to
                // `udf.rs`, replacing their bodies with the actual UDF implementations.
                // See detailed documentation in https://docs.feldera.com/sql/udf.

                #![allow(non_snake_case)]

                use feldera_sqllib::*;
                use crate::*;

                pub fn contains_number(str: String, value: Option<i32>) -> Result<bool, Box<dyn std::error::Error>> {
                    udf::contains_number(
                        str,
                        value)
                }
                pub fn EMPTY() -> Result<Option<String>, Box<dyn std::error::Error>> {
                    udf::EMPTY(
                    )
                }""", String.join(System.lineSeparator(), str));
        boolean success = protos.toFile().delete();
        Assert.assertTrue(success);

        // Truncate file to 0 bytes
        FileWriter writer = new FileWriter(udf);
        writer.close();
    }

    @Test
    public void testViewLateness() {
        String query = """
                LATENESS V.COL1 INTERVAL 1 HOUR;
                -- no view called W
                LATENESS W.COL2 INTERVAL 1 HOUR;
                CREATE VIEW V AS SELECT T.COL1, T.COL2 FROM T;
                CREATE VIEW V1 AS SELECT * FROM V;
                """;
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.ioOptions.quiet = false;  // show warnings
        compiler.compileStatement(OtherTests.ddl);
        compiler.compileStatements(query);
        DBSPCircuit circuit = getCircuit(compiler);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
            boolean found = false;

            @Override
            public VisitDecision preorder(DBSPControlledFilterOperator filter) {
                found = true;
                return VisitDecision.CONTINUE;
            }

            @Override
            public void endVisit() {
                Assert.assertTrue(this.found);
            }
        };
        visitor.apply(circuit);
        TestUtil.assertMessagesContain(compiler, "No view named 'w' found");
    }

    @Test
    public void testDefaultColumnValueCompiler() throws IOException, InterruptedException, SQLException {
        String[] statements = new String[]{
                """
                CREATE TABLE T (
                COL1 INT NOT NULL DEFAULT 0
                , COL2 DOUBLE DEFAULT 0.0
                , COL3 VARCHAR DEFAULT NULL
                )""",
                "CREATE VIEW V AS SELECT COL1 FROM T"
        };
        File file = createInputScript(statements);
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        System.err.println(messages);
        Assert.assertEquals(0, messages.errorCount());
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void testSchema() throws IOException, SQLException {
        String[] statements = new String[]{
                """
                CREATE TABLE T (
                COL1 INT NOT NULL
                , COL2 DOUBLE NOT NULL FOREIGN KEY REFERENCES S(COL0)
                , COL3 VARCHAR(3) NOT NULL PRIMARY KEY
                , COL4 VARCHAR(3) ARRAY
                , COL5 MAP<INT, INT>
                )""",
                "CREATE VIEW V AS SELECT COL1 AS \"xCol\" FROM T",
                "CREATE VIEW V1 (\"yCol\") AS SELECT COL1 FROM T"
        };
        File file = createInputScript(statements);
        File json = File.createTempFile("out", ".json", new File("."));
        json.deleteOnExit();
        File tmp = File.createTempFile("out", ".rs", new File("."));
        tmp.deleteOnExit();
        CompilerMessages message = CompilerMain.execute(
                "-js", json.getPath(), "-o", tmp.getPath(), file.getPath());
        if (message.exitCode != 0)
            System.err.println(message);
        Assert.assertEquals(message.exitCode, 0);
        TestUtil.assertMessagesContain(message,
                "Table 's', referred in FOREIGN KEY constraint of table 't', does not exist");
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        Assert.assertNotNull(parsed);
        String jsonContents  = Utilities.readFile(json.toPath());
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
                      }
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
        String[] statements = new String[] {
                "CREATE VIEW V AS SELECT * FROM T"
        };
        File file = createInputScript(statements);
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
