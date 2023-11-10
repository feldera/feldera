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

package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitFileAndSerialization;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitIODescription;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitSerializationKind;
import org.dbsp.sqlCompiler.compiler.backend.jit.ToJitVisitor;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITProgram;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CollectIdentifiers;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
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
import java.io.*;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

public class OtherTests extends BaseSQLTests implements IWritesLogs {
    static final String ddl = "CREATE TABLE T (\n" +
            "COL1 INT NOT NULL\n" +
            ", COL2 DOUBLE NOT NULL\n" +
            ", COL3 BOOLEAN NOT NULL\n" +
            ", COL4 VARCHAR NOT NULL\n" +
            ", COL5 INT\n" +
            ", COL6 DOUBLE\n" +
            ")";

    private DBSPCompiler compileDef() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatement(ddl);
        return compiler;
    }

    File fileFromName(String name) {
        return new File(Paths.get(rustDirectory, name).toUri());
    }

    void runJitServiceProgram(String[] statements, InputOutputPair data) throws IOException, InterruptedException {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.ioOptions.jit = true;
        compiler.compileStatements(String.join(";\n", statements));
        compiler.optimize();
        DBSPCircuit circuit = getCircuit(compiler);
        // Serialize circuit as JSON for the JIT executor
        JITProgram program = ToJitVisitor.circuitToJIT(compiler, circuit);
        String json = program.asJson().toPrettyString();
        File programFile = this.fileFromName("program.json");
        Utilities.writeFile(programFile.toPath(), json);

        // Prepare input files for the JIT runtime
        List<JitFileAndSerialization> inputFiles = new ArrayList<>();
        int index = 0;
        for (DBSPZSetLiteral.Contents inputData: data.inputs) {
            File input = this.fileFromName("input" + index++ + ".csv");
            input.deleteOnExit();
            ToCsvVisitor.toCsv(compiler, input, new DBSPZSetLiteral(compiler.getWeightTypeImplementation(), inputData));
            inputFiles.add(new JitFileAndSerialization(
                    input.getAbsolutePath(),
                    JitSerializationKind.Csv));
        }
        List<JitIODescription> inputDescriptions = compiler.getInputDescriptions(inputFiles);

        // Allocate output files
        index = 0;
        List<JitFileAndSerialization> outputFiles = new ArrayList<>();
        for (DBSPZSetLiteral.Contents ignored1 : data.outputs) {
            File output = this.fileFromName("output" + index++ + ".json");
            outputFiles.add(new JitFileAndSerialization(
                    output.getAbsolutePath(),
                    JitSerializationKind.Json));
            boolean ignored = output.delete(); // The program will create this file, we just care about its name
        }
        List<JitIODescription> outputDescriptions = compiler.getOutputDescriptions(outputFiles);

        // Invoke the JIT runtime with the program and the configuration file describing inputs and outputs
        JsonNode jitInputDescription = compiler.createJitRuntimeConfig(inputDescriptions, outputDescriptions);
        String s = jitInputDescription.toPrettyString();
        File configFile = this.fileFromName("config.json");
        Utilities.writeFile(configFile.toPath(), s);
        Utilities.runJIT(BaseSQLTests.projectDirectory, programFile.getAbsolutePath(), configFile.getAbsolutePath());

        // If we don't reach this point these files won't be deleted
        programFile.deleteOnExit();
        configFile.deleteOnExit();
        // Validate outputs and delete them
        for (int i = 0; i < data.outputs.length; i++) {
            DBSPZSetLiteral.Contents expected = data.outputs[i];
            JitIODescription outFile = outputDescriptions.get(i);
            File file = new File(outFile.path);
            DBSPZSetLiteral.Contents actual = outFile.parse(expected.getElementType());
            DBSPZSetLiteral.Contents diff = expected.minus(actual);
            Assert.assertTrue(diff.isEmpty());
            file.deleteOnExit();
        }
    }

    // Test using the JIT executor as a service.
    @Test
    public void runJitServiceTest() throws IOException, InterruptedException {
        String[] statements = new String[]{
                ddl,
                "CREATE VIEW V AS SELECT T.COL2 FROM T"
        };
        // Input test data
        InputOutputPair data = new InputOutputPair(
                new DBSPZSetLiteral.Contents(EndToEndTests.e0, EndToEndTests.e1),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPDoubleLiteral(12)),
                        new DBSPTupleExpression(new DBSPDoubleLiteral(1))));
        this.runJitServiceProgram(statements, data);
    }

    // Test using the JIT executor as a service with a table with a primary key.
    @Test 
    public void runJitServiceTestKey() throws IOException, InterruptedException {
        String[] statements = new String[]{
                "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL PRIMARY KEY\n" +
                ", COL2 DOUBLE NOT NULL PRIMARY KEY\n" +
                ", COL3 BOOLEAN NOT NULL PRIMARY KEY\n" +
                ", COL4 VARCHAR NOT NULL\n" +
                ", COL5 INT\n" +
                ", COL6 DOUBLE\n" +
                ")",
                "CREATE VIEW V AS SELECT T.COL2 FROM T"
        };
        // Input test data
        InputOutputPair data = new InputOutputPair(
                new DBSPZSetLiteral.Contents(EndToEndTests.e0, EndToEndTests.e1),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPDoubleLiteral(12)),
                        new DBSPTupleExpression(new DBSPDoubleLiteral(1))));
        this.runJitServiceProgram(statements, data);
    }

    @Test
    public void toRustTest() {
        String query = "SELECT T.COL3 FROM T";
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileDef();
        compiler.compileStatement(query);
        DBSPCircuit circuit = getCircuit(compiler);
        String rust = ToRustVisitor.toRustString(compiler, circuit);
        Assert.assertNotNull(rust);
    }

    @Test
    // This is also testing the deterministic node numbering
    // The numbering of the nodes will change when the optimizations are changed.
    public void toStringTest() {
        this.toRustTest();

        NameGen.reset();
        DBSPNode.reset();
        String query = "CREATE VIEW V AS SELECT T.COL3 FROM T";
        DBSPCompiler compiler = this.compileDef();
        compiler.compileStatement(query);
        DBSPCircuit circuit = getCircuit(compiler);
        String str = circuit.toString();
        String expected = "Circuit circuit0 {\n" +
                "    // DBSPSourceMultisetOperator 53\n" +
                "    // CREATE TABLE `T` (`COL1` INTEGER NOT NULL, `COL2` DOUBLE NOT NULL, `COL3` BOOLEAN NOT NULL, `COL4` VARCHAR NOT NULL, `COL5` INTEGER, `COL6` DOUBLE)\n" +
                "    let T = T();\n" +
                "    // DBSPMapOperator 74\n" +
                "    let stream0: stream<OrdZSet<Tuple1<b>, Weight>> = T.map((|t: &Tuple6<i32, d, b, s, i32?, d?>| Tuple1::new((t.2))));\n" +
                "    // CREATE VIEW `V` AS\n" +
                "    // SELECT `T`.`COL3`\n" +
                "    // FROM `T`\n" +
                "    // DBSPSinkOperator 80\n" +
                "    let V: stream<OrdZSet<Tuple1<b>, Weight>> = stream0;\n" +
                "}\n";
        Assert.assertEquals(expected, str);
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
        list.add(new DBSPExpressionStatement(new DBSPApplyExpression(
                "assert_eq!", new DBSPTypeVoid(), src.getVarReference(),
                data)));
        DBSPExpression body = new DBSPBlockExpression(list, null);
        DBSPFunction tester = new DBSPFunction("test", new ArrayList<>(),
                new DBSPTypeVoid(), body, Linq.list("#[test]"));

        RustFileWriter writer = new RustFileWriter(compiler, BaseSQLTests.testFilePath);
        writer.add(tester);
        writer.writeAndClose();
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @SuppressWarnings("SqlDialectInspection")
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
        DBSPVariablePath rowVariable = new DBSPVariablePath("row", sqliteRowType);
        DBSPExpression[] fields = EndToEndTests.e0NoDouble.fields; // Should be the same for e1NoDouble too
        final List<DBSPExpression> rowGets = new ArrayList<>(fields.length);
        for (int i = 0; i < fields.length; i++) {
            DBSPApplyMethodExpression rowGet =
                    new DBSPApplyMethodExpression("get",
                            fields[i].getType(),
                            rowVariable, new DBSPUSizeLiteral(i));
            rowGets.add(rowGet);
        }
        DBSPTupleExpression tuple = new DBSPTupleExpression(rowGets, false);
        DBSPClosureExpression mapClosure = new DBSPClosureExpression(tuple,
               rowVariable.asRefParameter());
        DBSPApplyExpression readDb = new DBSPApplyExpression("read_db", data.getType(),
                new DBSPStrLiteral(connectionString), new DBSPStrLiteral("t1"), mapClosure);

        DBSPLetStatement src = new DBSPLetStatement("src", readDb);
        list.add(src);
        list.add(new DBSPExpressionStatement(new DBSPApplyExpression(
                "assert_eq!", new DBSPTypeVoid(), src.getVarReference(),
                data)));
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
        list.add(new DBSPExpressionStatement(new DBSPApplyExpression(
                "assert_eq!", new DBSPTypeVoid(), src.getVarReference(),
                data)));
        DBSPExpression body = new DBSPBlockExpression(list, null);
        DBSPFunction tester = new DBSPFunction("test", new ArrayList<>(),
                new DBSPTypeVoid(), body, Linq.list("#[test]"));

        PrintStream outputStream = new PrintStream(BaseSQLTests.testFilePath, "UTF-8");
        RustFileWriter writer = new RustFileWriter(compiler, outputStream);
        writer.add(tester);
        writer.writeAndClose();
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void testWith() throws IOException, InterruptedException {
        String statement =
                "create table VENDOR (\n" +
                        "    id bigint not null primary key,\n" +
                        "    name varchar,\n" +
                        "    address varchar\n" +
                        ");\n" +
                        "\n" +
                        "create table PART (\n" +
                        "    id bigint not null primary key,\n" +
                        "    name varchar\n" +
                        ");\n" +
                        "\n" +
                        "create table PRICE (\n" +
                        "    part bigint not null,\n" +
                        "    vendor bigint not null,\n" +
                        "    price decimal\n" +
                        ");\n" +
                        "\n" +
                        "create view LOW_PRICE AS " +
                        "with LOW_PRICE_CTE AS (" +
                        "  select part, MIN(price) as price from PRICE group by part" +
                        ") select * FROM LOW_PRICE_CTE";
        File file = createInputScript(statement);
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
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
        Assert.assertTrue(op.is(DBSPDistinctOperator.class));
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
        CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
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
    public void testCompilerToJson() throws IOException {
        String[] statements = new String[]{
                "CREATE TABLE T (\n" +
                        "COL1 INT NOT NULL" +
                        ", COL2 DOUBLE NOT NULL" +
                        ")",
                "CREATE VIEW V AS SELECT COL1 FROM T WHERE COL1 > 5"
        };
        File file = createInputScript(statements);
        File json = File.createTempFile("out", ".json", new File("."));
        json.deleteOnExit();
        CompilerMessages message = CompilerMain.execute("-j", "-o", json.getPath(), file.getPath());
        Assert.assertEquals(message.exitCode, 0);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        Assert.assertNotNull(parsed);
    }

    @Test
    public void testCompilerToPng() throws IOException {
        try {
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
        } catch (Exception ex) {
            // if graphviz is not installed this test fails early
        }
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
    public void testSanitizeNames() throws IOException, InterruptedException {
        String statements = "create table t1(\n" +
                "c1 integer,\n" +
                "\"col\" boolean,\n" +
                "\"SPACES INSIDE\" CHAR,\n" +
                "\"CC\" CHAR,\n" +
                "\"quoted \"\" with quote\" CHAR,\n" +
                "U&\"d\\0061t\\0061\" CHAR,\n" + // 'data' spelled in Unicode
                "José CHAR,\n"  +
                "\"Gosé\" CHAR,\n" +
                "\"\uD83D\uDE00❤\" varchar not null,\n" +
                "\"αβγ\" boolean not null,\n" +
                "ΔΘ boolean not null" +
        ");\n" +
                "create view v1 as select * from t1;";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(statements);
        DBSPCircuit circuit = getCircuit(compiler);
        RustFileWriter writer = new RustFileWriter(compiler, testFilePath);
        // Check that the structs generated have legal names.
        writer.emitCodeWithHandle(true);
        writer.add(circuit);
        writer.writeAndClose();
        Utilities.compileAndTestRust(rustDirectory, true);
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
