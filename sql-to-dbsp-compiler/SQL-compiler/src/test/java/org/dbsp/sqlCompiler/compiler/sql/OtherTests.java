/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.sql.simple.EndToEndTests;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CollectIdentifiers;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeWeight;
import org.dbsp.util.FreshName;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.NameGen;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.imageio.ImageIO;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

public class OtherTests extends BaseSQLTests implements IWritesLogs {
    static final String ddl = """
            CREATE TABLE T (
            COL1 INT NOT NULL
            , COL2 DOUBLE NOT NULL
            , COL3 BOOLEAN NOT NULL
            , COL4 VARCHAR NOT NULL
            , COL5 INT
            , COL6 DOUBLE
            )""";

    private DBSPCompiler compileDef() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatement(ddl);
        return compiler;
    }

    @Test
    // This is also testing the deterministic node numbering
    // The numbering of the nodes will change when the optimizations are changed.
    public void toStringTest() {
        this.testIntCastWarning();

        NameGen.reset();
        DBSPNode.reset();
        String query = "CREATE VIEW V AS SELECT T.COL3 FROM T";
        DBSPCompiler compiler = this.compileDef();
        compiler.compileStatement(query);
        DBSPCircuit circuit = getCircuit(compiler);
        String str = circuit.toString();
        String expected = """
                Circuit circuit0 {
                    // DBSPSourceMultisetOperator 53
                    // CREATE TABLE `T` (`COL1` INTEGER NOT NULL, `COL2` DOUBLE NOT NULL, `COL3` BOOLEAN NOT NULL, `COL4` VARCHAR NOT NULL, `COL5` INTEGER, `COL6` DOUBLE)
                    let T = T();
                    // DBSPMapOperator 76
                    let stream0: stream<OrdZSet<Tuple1<b>, Weight>> = T.map((|t: &Tuple6<i32, d, b, s, i32?, d?>| Tuple1::new(((*t).2), )));
                    // CREATE VIEW `V` AS
                    // SELECT `T`.`COL3`
                    // FROM `T`
                    let V: stream<OrdZSet<Tuple1<b>, Weight>> = stream0;
                }
                """;
        Assert.assertEquals(expected, str);
    }

    @Test
    public void testIntCastWarning() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.ioOptions.quiet = false;
        String query = "CREATE VIEW V AS SELECT '1_000'::INT4";
        compiler.compileStatement(query);
        getCircuit(compiler);  // invokes optimizer
        TestUtil.assertMessagesContain(compiler.messages, "String '1_000' cannot be interpreted as a number");
    }

    // Test the ability to redirect logging streams.
    @Test
    public void loggerTest() {
        StringBuilder builder = new StringBuilder();
        Appendable save = Logger.INSTANCE.setDebugStream(builder);
        Logger.INSTANCE.setLoggingLevel(this.getClassName(), 1);
        Assert.assertEquals("OtherTests", this.getClassName());
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Logging one statement")
                .newline();
        Logger.INSTANCE.setLoggingLevel(this.getClassName(), 0);
        Logger.INSTANCE.belowLevel(this, 1)
                .append("This one is not logged")
                .newline();
        Logger.INSTANCE.setDebugStream(save);
        Assert.assertEquals("Logging one statement\n", builder.toString());
        Logger.INSTANCE.setLoggingLevel(this.getClassName(), 0);
    }

    // Test the -T command-line parameter
    @Test
    public void loggingParameter() throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        Appendable save = Logger.INSTANCE.setDebugStream(builder);
        String[] statements = new String[]{
                "CREATE TABLE T (\n" +
                        "COL1 INT NOT NULL" +
                        ", COL2 DOUBLE NOT NULL" +
                        ")",
                "CREATE VIEW V AS SELECT COL1 FROM T"
        };
        File file = createInputScript(statements);
        CompilerMain.execute("-TCalciteCompiler=2", "-TPasses=2",
                "-o", BaseSQLTests.testFilePath, file.getPath());
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, true);
        Logger.INSTANCE.setDebugStream(save);
        String messages = builder.toString();
        Assert.assertTrue(messages.contains("After optimizer"));
        Assert.assertTrue(messages.contains("CircuitRewriter:Simplify"));
        Logger.INSTANCE.setLoggingLevel(CalciteCompiler.class, 0);
        Logger.INSTANCE.setLoggingLevel(Passes.class, 0);
    }

    @Test
    public void toCsvTest() {
        DBSPCompiler compiler = testCompiler();
        DBSPZSetLiteral s = new DBSPZSetLiteral(new DBSPTypeWeight(), EndToEndTests.e0, EndToEndTests.e1);
        StringBuilder builder = new StringBuilder();
        ToCsvVisitor visitor = new ToCsvVisitor(compiler, builder, () -> "");
        visitor.traverse(s);
        String[] lines = builder.toString().split("\n");
        Arrays.sort(lines);
        Assert.assertEquals(
                "10,1.0,false,\"Hi\",1,0.0,\n" +
                "10,12.0,true,\"Hi\",,,",
                String.join("\n", lines));
    }

    @Test
    public void rustCsvTest() throws IOException, InterruptedException {
        DBSPCompiler compiler = testCompiler();
        DBSPZSetLiteral data = new DBSPZSetLiteral(new DBSPTypeWeight(), EndToEndTests.e0, EndToEndTests.e1);
        File file = File.createTempFile("test", ".csv", new File(BaseSQLTests.rustDirectory));
        file.deleteOnExit();
        ToCsvVisitor.toCsv(compiler, file, data);
        List<DBSPStatement> list = new ArrayList<>();
        DBSPLetStatement src = new DBSPLetStatement("src",
                new DBSPApplyExpression("read_csv", data.getType(),
                        new DBSPStrLiteral(file.getAbsolutePath())));
        list.add(src);
        list.add(new DBSPApplyExpression(
                "assert_eq!", new DBSPTypeVoid(), src.getVarReference(),
                data).toStatement());
        DBSPExpression body = new DBSPBlockExpression(list, null);
        DBSPFunction tester = new DBSPFunction("test", new ArrayList<>(),
                new DBSPTypeVoid(), body, Linq.list("#[test]"));

        RustFileWriter writer = new RustFileWriter(compiler, BaseSQLTests.testFilePath);
        writer.add(tester);
        writer.writeAndClose();
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void rustSqlTest() throws IOException, InterruptedException, SQLException {
        String filepath = BaseSQLTests.rustDirectory + "/" + "test.db";
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + filepath);
        Statement statement = connection.createStatement();
        statement.executeUpdate("drop table if exists t1");
        statement.executeUpdate("create table t1(c1 integer not null, c2 bool not null, " +
                                "c3 varcharnot null , c4 integer)");
        statement.executeUpdate("insert into t1 values(10, true, 'Hi', null)"); // e0NoDouble
        statement.executeUpdate("insert into t1 values(10, false, 'Hi', 1)"); // e1NoDouble
        connection.close();
        DBSPCompiler compiler = testCompiler();

        DBSPZSetLiteral data = new DBSPZSetLiteral(
                new DBSPTypeWeight(), EndToEndTests.e0NoDouble, EndToEndTests.e1NoDouble);
        List<DBSPStatement> list = new ArrayList<>();

        String connectionString = "sqlite://" + filepath;
        // Generates a read_table(<conn>, <table_name>, <mapper from |AnyRow| -> Tuple type>) invocation
        DBSPTypeUser sqliteRowType = new DBSPTypeUser(CalciteObject.EMPTY, USER, "AnyRow", false);
        DBSPVariablePath rowVariable = new DBSPVariablePath("row", sqliteRowType.ref());
        DBSPExpression[] fields = EndToEndTests.e0NoDouble.fields; // Should be the same for e1NoDouble too
        final List<DBSPExpression> rowGets = new ArrayList<>(fields.length);
        for (int i = 0; i < fields.length; i++) {
            DBSPApplyMethodExpression rowGet =
                    new DBSPApplyMethodExpression("get",
                            fields[i].getType(),
                            rowVariable.deref(), new DBSPUSizeLiteral(i));
            rowGets.add(rowGet);
        }
        DBSPTupleExpression tuple = new DBSPTupleExpression(rowGets, false);
        DBSPClosureExpression mapClosure = new DBSPClosureExpression(tuple,
               rowVariable.asParameter());
        DBSPApplyExpression readDb = new DBSPApplyExpression("read_db", data.getType(),
                new DBSPStrLiteral(connectionString), new DBSPStrLiteral("t1"), mapClosure);

        DBSPLetStatement src = new DBSPLetStatement("src", readDb);
        list.add(src);
        list.add(new DBSPApplyExpression(
                "assert_eq!", new DBSPTypeVoid(), src.getVarReference(),
                data).toStatement());
        DBSPExpression body = new DBSPBlockExpression(list, null);
        DBSPFunction tester = new DBSPFunction("test", new ArrayList<>(),
                new DBSPTypeVoid(), body, Linq.list("#[test]"));

        RustFileWriter writer = new RustFileWriter(compiler, BaseSQLTests.testFilePath);
        writer.add(tester);
        writer.writeAndClose();
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void rustCsvTest2() throws IOException, InterruptedException {
        DBSPCompiler compiler = this.testCompiler();
        DBSPZSetLiteral data = new DBSPZSetLiteral(
                new DBSPTypeWeight(),
                new DBSPTupleExpression(new DBSPI32Literal(1, true)),
                new DBSPTupleExpression(new DBSPI32Literal(2, true)),
                new DBSPTupleExpression(DBSPI32Literal.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))
        );
        File file = File.createTempFile("test", ".csv", new File(BaseSQLTests.rustDirectory));
        file.deleteOnExit();
        ToCsvVisitor.toCsv(compiler, file, data);
        List<DBSPStatement> list = new ArrayList<>();
        DBSPLetStatement src = new DBSPLetStatement("src",
                new DBSPApplyExpression("read_csv", data.getType(),
                        new DBSPStrLiteral(file.getAbsolutePath())));
        list.add(src);
        list.add(new DBSPApplyExpression(
                "assert_eq!", new DBSPTypeVoid(), src.getVarReference(),
                data).toStatement());
        DBSPExpression body = new DBSPBlockExpression(list, null);
        DBSPFunction tester = new DBSPFunction("test", new ArrayList<>(),
                new DBSPTypeVoid(), body, Linq.list("#[test]"));

        PrintStream outputStream = new PrintStream(BaseSQLTests.testFilePath, StandardCharsets.UTF_8);
        RustFileWriter writer = new RustFileWriter(compiler, outputStream);
        writer.add(tester);
        writer.writeAndClose();
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void testWith() throws IOException, InterruptedException {
        String statement =
                """
                        create table VENDOR (
                            id bigint not null primary key,
                            name varchar,
                            address varchar
                        );

                        create table PART (
                            id bigint not null primary key,
                            name varchar
                        );

                        create table PRICE (
                            part bigint not null,
                            vendor bigint not null,
                            price decimal
                        );

                        create view LOW_PRICE AS with LOW_PRICE_CTE AS (  select part, MIN(price) as price from PRICE group by part) select * FROM LOW_PRICE_CTE""";
        File file = createInputScript(statement);
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        if (messages.errorCount() > 0)
            throw new RuntimeException(messages.toString());
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void testUDFWarning() throws IOException {
        File file = createInputScript("CREATE FUNCTION myfunction(d DATE, i INTEGER) RETURNS VARCHAR",
                "CREATE VIEW V AS SELECT myfunction(DATE '2023-10-20', CAST(5 AS INTEGER))");
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(1, messages.warningCount());
        Assert.assertTrue(messages.toString().contains("the compiler was invoked without the `-udf` flag"));
    }

    @Test
    public void testUDFTypeError() throws IOException {
        File file = createInputScript("CREATE FUNCTION myfunction(d DATE, i INTEGER) RETURNS VARCHAR NOT NULL",
                "CREATE VIEW V AS SELECT myfunction(DATE '2023-10-20', '5')");
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(1, messages.errorCount());
        Assert.assertTrue(messages.toString().contains(
                "Cannot apply 'MYFUNCTION' to arguments of type 'MYFUNCTION(<DATE>, <CHAR(1)>)'. " +
                "Supported form(s): MYFUNCTION(<DATE>, <INTEGER>)"));
    }

    @Test
    public void testUDF() throws IOException, InterruptedException {
        File file = createInputScript(
                "CREATE FUNCTION contains_number(str VARCHAR NOT NULL, value INTEGER) RETURNS BOOLEAN NOT NULL",
                "CREATE VIEW V0 AS SELECT contains_number(CAST('YES: 10 NO:5 MAYBE: 2' AS VARCHAR), 5)",
                "CREATE FUNCTION \"empty\"() RETURNS VARCHAR",
                "CREATE VIEW V1 AS SELECT \"empty\"()");
        File implementation = File.createTempFile("impl", ".rs", new File(rustDirectory));
        createInputFile(implementation,
                System.lineSeparator(),
                "use sqllib::*;",
                "pub fn CONTAINS_NUMBER(pos: &SourcePositionRange, str: String, value: Option<i32>) -> " +
                "   Result<bool, Box<dyn std::error::Error>> {",
                "   match value {",
                "      None => Err(format!(\"{}: null value\", pos).into()),",
                "      Some(value) => Ok(str.contains(&format!(\"{}\", value).to_string())),",
                "   }",
                "}",
                "pub fn empty(pos: &SourcePositionRange) -> Result<Option<String>, Box<dyn std::error::Error>> {",
                "   Ok(Some(\"\".to_string()))",
                "}");
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, "--udf",
                implementation.getPath(), file.getPath());
        if (messages.errorCount() > 0)
            throw new RuntimeException(messages.toString());
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void testProjectFiles() throws IOException, InterruptedException {
        // Compiles all the programs in the tests directory
        final String projectsDirectory = "../../demo/";
        File dir = new File(projectsDirectory);
        File[] subdirs = dir.listFiles(File::isDirectory);
        Objects.requireNonNull(subdirs);
        for (File subdir: subdirs) {
            if (!subdir.getName().contains("project_"))
                continue;
            String path = subdir.getPath() + "/project.sql";
            CompilerMessages messages = CompilerMain.execute(
                    "-i", "--alltables", "-o", BaseSQLTests.testFilePath, path);
            System.out.println(messages);
            Assert.assertEquals(0, messages.errorCount());
            Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
        }
    }

    // Test the ignoreOrderBy compiler flag
    @Test
    public void testIgnoreOrderBy() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.languageOptions.ignoreOrderBy = true;
        String query = "CREATE VIEW V AS SELECT * FROM T ORDER BY T.COL2";
        compiler.compileStatement(ddl);
        compiler.compileStatements(query);
        DBSPCircuit circuit = compiler.getFinalCircuit("circuit");
        // Check that the output type does not include a vector.
        DBSPTypeZSet outputType = circuit.getOutputType(0).to(DBSPTypeZSet.class);
        DBSPTypeZSet inputType = circuit.getInputType(0).to(DBSPTypeZSet.class);
        Assert.assertTrue(inputType.sameType(outputType));
    }

    // Test the outputsAreSets compiler flag
    @Test
    public void testOutputsAreSets() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.languageOptions.outputsAreSets = true;
        String query = "CREATE VIEW V AS SELECT T.COL1 FROM T";
        compiler.compileStatement(ddl);
        compiler.compileStatements(query);
        compiler.optimize();
        DBSPCircuit circuit = compiler.getFinalCircuit("circuit");
        DBSPOperator sink = circuit.circuit.getOperator("V");
        Assert.assertNotNull(sink);
        Assert.assertEquals(1, sink.inputs.size());
        DBSPOperator op = sink.inputs.get(0);
        // There is no optimization I can imagine which will remove the distinct
        Assert.assertTrue(op.is(DBSPStreamDistinctOperator.class));
    }

    @Test
    public void testRustCompiler() throws IOException, InterruptedException {
        String[] statements = new String[]{
                "CREATE TABLE T (\n" +
                        "COL1 INT NOT NULL" +
                        ", COL2 DOUBLE NOT NULL" +
                        ")",
                "CREATE VIEW V AS SELECT COL1 FROM T"
        };
        File file = createInputScript(statements);
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        System.err.println(messages);
        Assert.assertEquals(0, messages.exitCode);
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void testDefaultColumnValueCompiler() throws IOException, InterruptedException {
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
    public void testSchema() throws IOException {
        String[] statements = new String[]{
                "CREATE TABLE T (\n" +
                        "COL1 INT NOT NULL" +
                        ", COL2 DOUBLE NOT NULL" +
                        ", COL3 VARCHAR(3) PRIMARY KEY" +
                        ", COL4 VARCHAR(3) ARRAY" +
                        ")",
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
        Assert.assertEquals(message.exitCode, 0);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        Assert.assertNotNull(parsed);
        String jsonContents  = Utilities.readFile(json.toPath());
        Assert.assertEquals("{\n" +
                "  \"inputs\" : [ {\n" +
                "    \"name\" : \"T\",\n" +
                "    \"fields\" : [ {\n" +
                "      \"name\" : \"COL1\",\n" +
                "      \"case_sensitive\" : false,\n" +
                "      \"columntype\" : {\n" +
                "        \"type\" : \"INTEGER\",\n" +
                "        \"nullable\" : false\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"name\" : \"COL2\",\n" +
                "      \"case_sensitive\" : false,\n" +
                "      \"columntype\" : {\n" +
                "        \"type\" : \"DOUBLE\",\n" +
                "        \"nullable\" : false\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"name\" : \"COL3\",\n" +
                "      \"case_sensitive\" : false,\n" +
                "      \"columntype\" : {\n" +
                "        \"type\" : \"VARCHAR\",\n" +
                "        \"nullable\" : true,\n" +
                "        \"precision\" : 3\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"name\" : \"COL4\",\n" +
                "      \"case_sensitive\" : false,\n" +
                "      \"columntype\" : {\n" +
                "        \"type\" : \"ARRAY\",\n" +
                "        \"nullable\" : true,\n" +
                "        \"component\" : {\n" +
                "          \"type\" : \"VARCHAR\",\n" +
                "          \"nullable\" : false,\n" +
                "          \"precision\" : 3\n" +
                "        }\n" +
                "      }\n" +
                "    } ],\n" +
                "    \"primary_key\" : [ \"COL3\" ]\n" +
                "  } ],\n" +
                "  \"outputs\" : [ {\n" +
                "    \"name\" : \"V\",\n" +
                "    \"fields\" : [ {\n" +
                "      \"name\" : \"xCol\",\n" +
                // TODO: the following should probably be 'true'
                "      \"case_sensitive\" : false,\n" +
                "      \"columntype\" : {\n" +
                "        \"type\" : \"INTEGER\",\n" +
                "        \"nullable\" : false\n" +
                "      }\n" +
                "    } ]\n" +
                "  }, {\n" +
                "    \"name\" : \"V1\",\n" +
                "    \"fields\" : [ {\n" +
                "      \"name\" : \"yCol\",\n" +
                "      \"case_sensitive\" : true,\n" +
                "      \"columntype\" : {\n" +
                "        \"type\" : \"INTEGER\",\n" +
                "        \"nullable\" : false\n" +
                "      }\n" +
                "    } ]\n" +
                "  } ]\n" +
                "}", jsonContents);
    }

    @Test @Ignore("Only run if we want to preserve casing for names")
    public void testCaseSensitive() throws IOException {
        String[] statements = new String[]{
                "CREATE TABLE MYTABLE (\n" +
                        "COL1 INT NOT NULL" +
                        ", COL2 DOUBLE NOT NULL" +
                        ")",
                "CREATE VIEW V AS SELECT COL1 FROM MYTABLE",
                "CREATE TABLE yourtable (\n" +
                        "  column1 INT NOT NULL" +
                        ", column2 DOUBLE NOT NULL" +
                        ")",
                "CREATE VIEW V2 AS SELECT column2 FROM yourtable"
        };
        File file = createInputScript(statements);
        File json = File.createTempFile("out", ".json", new File("."));
        json.deleteOnExit();
        File tmp = File.createTempFile("out", ".rs", new File("."));
        tmp.deleteOnExit();
        CompilerMessages message = CompilerMain.execute(
                "-js", json.getPath(), "-o", tmp.getPath(), file.getPath());
        Assert.assertEquals(message.exitCode, 0);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        Assert.assertNotNull(parsed);
        String jsonContents  = Utilities.readFile(json.toPath());
        Assert.assertTrue(jsonContents.contains("MYTABLE"));  // checks case sensitivity
        Assert.assertTrue(jsonContents.contains("COL1"));
        Assert.assertTrue(jsonContents.contains("yourtable"));
        Assert.assertTrue(jsonContents.contains("column1"));
    }

    @Test
    public void testCompilerToPng() throws IOException {
        if (!Utilities.isDotInstalled())
            return;
        String[] statements = new String[]{
                "CREATE TABLE T (\n" +
                        "COL1 INT NOT NULL" +
                        ", COL2 DOUBLE NOT NULL" +
                        ")",
                "CREATE VIEW V AS SELECT COL1 FROM T WHERE COL1 > 5"
        };
        File file = createInputScript(statements);
        File png = File.createTempFile("out", ".png", new File("."));
        png.deleteOnExit();
        CompilerMessages message = CompilerMain.execute("-png", "-o", png.getPath(), file.getPath());
        Assert.assertEquals(message.exitCode, 0);
        Assert.assertTrue(file.exists());
        ImageIO.read(new File(png.getPath()));
    }

    @Test
    public void testFreshName() {
        String query = "CREATE VIEW V AS SELECT T.COL1 FROM T WHERE T.COL2 > 0";
        DBSPCompiler compiler = this.compileDef();
        compiler.compileStatement(query);
        DBSPCircuit circuit = getCircuit(compiler);
        Set<String> used = new HashSet<>();
        CollectIdentifiers ci = new CollectIdentifiers(testCompiler(), used);
        ci.getCircuitVisitor().apply(circuit);
        Assert.assertTrue(used.contains("T")); // table name
        Assert.assertTrue(used.contains("V")); // view name
        FreshName gen = new FreshName(used);
        String t0 = gen.freshName("T");
        Assert.assertEquals(t0, "T_0");
        String t1 = gen.freshName("T");
        Assert.assertEquals(t1, "T_1");
    }

    @Test
    public void jsonErrorTest() throws IOException {
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
    }

    @Test
    public void rawCalciteTest() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        String query = "SELECT timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59')";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.execute();
            try (ResultSet resultSet = ps.getResultSet()) {
                while (resultSet.next()) {
                    int result = resultSet.getInt(1);
                    Assert.assertEquals(0, result);
                }
            }
        }
    }
}
