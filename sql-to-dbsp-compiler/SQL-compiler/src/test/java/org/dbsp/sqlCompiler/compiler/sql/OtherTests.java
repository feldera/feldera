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

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.simple.EndToEndTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.NameGen;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import javax.imageio.ImageIO;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Miscellaneous tests that do not fit into standard categories */
public class OtherTests extends BaseSQLTests implements IWritesLogs { // interface used for testing
    public static final String ddl = """
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

    // This is also testing the deterministic node numbering
    // The numbering of the nodes will change when the optimizations are changed.
    @Test public void toStringTest() {
        this.testIntCastWarning();

        NameGen.reset();
        DBSPNode.reset();
        DBSPVariablePath.reset();
        String query = "CREATE VIEW V AS SELECT T.COL3 FROM T";
        DBSPCompiler compiler = this.compileDef();
        compiler.compileStatement(query);
        // Deterministically name the circuit function.
        DBSPCircuit circuit = compiler.getFinalCircuit("circuit");
        String str = circuit.toString();
        String expected = """
                Circuit circuit {
                    // DBSPSourceMultisetOperator 59
                    // CREATE TABLE `T` (`COL1` INTEGER NOT NULL, `COL2` DOUBLE NOT NULL, `COL3` BOOLEAN NOT NULL, `COL4` VARCHAR NOT NULL, `COL5` INTEGER, `COL6` DOUBLE)
                    let stream59 = T();
                    // DBSPMapOperator 81
                    let stream81: stream<WSet<Tup1<b>>> = stream59.map((|t_1: &Tup6<i32, d, b, s, i32?, d?>| Tup1::new(((*t_1).2), )));
                    // CREATE VIEW `V` AS
                    // SELECT `T`.`COL3`
                    // FROM `T`
                    let stream208: stream<WSet<Tup1<b>>> = stream81;
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
        TestUtil.assertMessagesContain(compiler, "String '1_000' cannot be interpreted as a number");
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
    public void loggingParameter() throws IOException, InterruptedException, SQLException {
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
        Assert.assertTrue(messages.contains("MergeSums"));
        Logger.INSTANCE.setLoggingLevel(CalciteCompiler.class, 0);
        Logger.INSTANCE.setLoggingLevel(Passes.class, 0);
    }

    @Test
    public void toCsvTest() {
        DBSPCompiler compiler = testCompiler();
        DBSPZSetLiteral s = new DBSPZSetLiteral(EndToEndTests.e0, EndToEndTests.e1);
        StringBuilder builder = new StringBuilder();
        ToCsvVisitor visitor = new ToCsvVisitor(compiler, builder, () -> "");
        visitor.apply(s);
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
        DBSPZSetLiteral data = new DBSPZSetLiteral(EndToEndTests.e0, EndToEndTests.e1);
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

        RustFileWriter writer = new RustFileWriter(BaseSQLTests.testFilePath);
        writer.add(tester);
        writer.writeAndClose(compiler);
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void rustCsvTest2() throws IOException, InterruptedException {
        DBSPCompiler compiler = this.testCompiler();
        DBSPZSetLiteral data = new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPI32Literal(1, true)),
                new DBSPTupleExpression(new DBSPI32Literal(2, true)),
                new DBSPTupleExpression(DBSPI32Literal.none(
                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))
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
        RustFileWriter writer = new RustFileWriter(outputStream);
        writer.add(tester);
        writer.writeAndClose(compiler);
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void testWith() throws IOException, InterruptedException, SQLException {
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

                create view LOW_PRICE AS
                WITH LOW_PRICE_CTE AS (  select part, MIN(price) as price from PRICE group by part)
                SELECT * FROM LOW_PRICE_CTE""";
        File file = createInputScript(statement);
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        if (messages.errorCount() > 0)
            throw new RuntimeException(messages.toString());
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void testProjectionSimplify() {
        // Test for https://github.com/feldera/feldera/issues/1628
        String sql = EndToEndTests.E2E_TABLE + "; " +
                "CREATE VIEW V AS SELECT T1.COL3 FROM T AS T1 JOIN T AS T2 ON T1.COL2 = T2.COL6";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        CircuitVisitor noMap = new CircuitVisitor(compiler) {
            @Override
            public VisitDecision preorder(DBSPMapOperator node) {
                Assert.fail("Circuit should contain no map operators");
                return super.preorder(node);
            }
        };
        noMap.apply(circuit);
    }

    @Test
    public void testIOT() throws IOException {
        // Iot code from different repository checked out in a specific place
        String[] iotSql = new String[] {
                "../../../iot/iot.sql",
                "../../../iot/net.sql"
        };
        for (String sql: iotSql) {
            File file = new File(sql);
            if (!file.exists())
                continue;
            String script = Utilities.readFile(file.toPath());
            DBSPCompiler compiler = this.testCompiler();
            compiler.options.languageOptions.throwOnError = true;
            compiler.options.ioOptions.emitHandles = false;
            compiler.options.languageOptions.incrementalize = true;
            compiler.compileStatements(script);
            CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
            this.addRustTestCase(ccs);
        }
    }

    @Test
    public void testProjectFiles() throws IOException, InterruptedException, SQLException {
        // Compiles all the programs in the tests directory
        final String projectsDirectory = "../../demo/";
        File dir = new File(projectsDirectory);
        File[] subdirs = dir.listFiles(File::isDirectory);
        Objects.requireNonNull(subdirs);
        for (File subdir: subdirs) {
            if (!subdir.getName().contains("project_"))
                continue;
            FilenameFilter filter = (_d, name) -> !name.contains("setup") && name.endsWith(".sql");
            String[] sqlFiles = subdir.list(filter);
            assert sqlFiles != null;
            for (String sqlFile: sqlFiles) {
                String path = subdir.getPath() + "/" + sqlFile;
                CompilerMessages messages = CompilerMain.execute(
                        "-i", "--alltables", "-o", BaseSQLTests.testFilePath, path);
                if (!messages.isEmpty())
                    System.out.println(messages);
                Assert.assertEquals(0, messages.errorCount());
            }
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
        DBSPTypeZSet outputType = circuit.getSingleOutputType().to(DBSPTypeZSet.class);
        DBSPOperator source = circuit.getInput("T");
        Assert.assertNotNull(source);
        DBSPTypeZSet inputType = source.getType().to(DBSPTypeZSet.class);
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
        DBSPCircuit circuit = compiler.getFinalCircuit("circuit");
        DBSPSinkOperator sink = circuit.circuit.getSink("V");
        Assert.assertNotNull(sink);
        DBSPOperator op = sink.input();
        // There is no optimization I can imagine which will remove the distinct
        Assert.assertTrue(op.is(DBSPStreamDistinctOperator.class));
    }

    @Test
    public void testNoOutput() throws IOException, SQLException {
        String[] statements = new String[]{
                "CREATE TABLE T (\n" +
                        "COL1 INT NOT NULL" +
                        ", COL2 DOUBLE NOT NULL" +
                        ")",
                "CREATE VIEW V AS SELECT COL1 FROM T"
        };
        File file = createInputScript(statements);
        // Redirect error stream
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(os);
        PrintStream old = System.err;
        System.setErr(ps);
        CompilerMain.execute(file.getPath(), file.getPath());
        // Restore error stream
        System.setErr(old);
        Assert.assertTrue(os.toString().contains("Did you forget to specify"));
    }

    @Test
    public void testRustCompiler() throws IOException, InterruptedException, SQLException {
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
    public void testCompilerExample() throws IOException, InterruptedException, SQLException {
        // The example in docs/contributors/compiler.md
        String sql = """
                -- define Person table
                CREATE TABLE Person
                (
                    name    VARCHAR NOT NULL,
                    age     INT,
                    present BOOLEAN
                );
                CREATE VIEW Adult AS SELECT Person.name FROM Person WHERE Person.age > 18;
                """;
        String rustHandlesTest = """
                #[test]
                pub fn test() {
                    let (mut circuit, (person, adult) ) = circuit(CircuitConfig::with_workers(2)).unwrap();
                    // Feed two input records to the circuit.
                    // First input has a count of "1"
                    person.push( ("Bob".to_string(), Some(12), Some(true)).into(), 1 );
                    // Second input has a count of "2"
                    person.push( ("Tom".to_string(), Some(20), Some(false)).into(), 2 );
                    // Execute the circuit on these inputs
                    circuit.step().unwrap();
                    // Read the produced output
                    let out = adult.consolidate();
                    // Print the produced output
                    println!("{:?}", out);
                }
                """;
        String rustCatalogTest = """
                #[test]
                pub fn test() {
                    use dbsp_adapters::{CircuitCatalog, RecordFormat};
                
                    let (mut circuit, catalog) = circuit(CircuitConfig::with_workers(2))
                        .expect("Failed to build circuit");
                    let persons = catalog
                        .input_collection_handle(&SqlIdentifier::from("PERSON"))
                        .expect("Failed to get input collection handle");
                    let mut persons_stream = persons
                        .handle
                        .configure_deserializer(RecordFormat::Csv)
                        .expect("Failed to configure deserializer");
                    persons_stream
                        .insert(b"Bob,12,true")
                        .expect("Failed to insert data");
                    persons_stream
                        .insert(b"Tom,20,false")
                        .expect("Failed to insert data");
                    persons_stream
                        .insert(b"Tom,20,false")
                        .expect("Failed to insert data");  // Insert twice
                    persons_stream.flush();
                    // Execute the circuit on these inputs
                    circuit
                        .step()
                        .unwrap();
                
                    let adult = &catalog
                        .output_handles(&SqlIdentifier::from("ADULT"))
                        .expect("Failed to get output collection handles")
                        .delta_handle;
                
                    // Read the produced output
                    let out = adult.consolidate();
                    // Print the produced output
                    println!("{:?}", out);
                }
                """;
        File file = createInputScript(sql);
        CompilerMessages message = CompilerMain.execute(
                "--handles", "-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(message.exitCode, 0);
        Assert.assertTrue(file.exists());

        File rust = new File(BaseSQLTests.testFilePath);
        try (FileWriter fr = new FileWriter(rust, true)) { // append
            fr.write(rustHandlesTest);
        }
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);

        // Second test
        message = CompilerMain.execute(
                "-i", "-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(message.exitCode, 0);
        Assert.assertTrue(file.exists());

        rust = new File(BaseSQLTests.testFilePath);
        try (FileWriter fr = new FileWriter(rust, true)) { // append
            fr.write(rustCatalogTest);
        }
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void testCompilerToPng() throws IOException, SQLException {
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

    @Test
    public void testLocalView() {
        String sql = """
                    CREATE TABLE T(I INTEGER, S VARCHAR);
                    CREATE LOCAL VIEW V AS SELECT * FROM T;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        Assert.assertEquals(0, circuit.getOutputCount());

        String oneMore = "CREATE VIEW W AS SELECT * FROM V;";
        compiler = this.testCompiler();
        compiler.compileStatements(sql + oneMore);
        circuit = getCircuit(compiler);
        Assert.assertEquals(1, circuit.getOutputCount());
    }

    @Test
    public void testRemove() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatement("CREATE TABLE T(I INTEGER, S VARCHAR)");
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        Change change = ccs.toChange("""
                INSERT INTO T VALUES(1, 'x');
                REMOVE FROM T VALUES(2, 'Y');
                REMOVE FROM T VALUES(3, 'Z');""").simplify();
        DBSPZSetLiteral expected = DBSPZSetLiteral.emptyWithElementType(
                new DBSPTypeTuple(
                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true),
                        DBSPTypeString.varchar(true)));
        expected.add(new DBSPTupleExpression(
                new DBSPI32Literal(1, true),
                new DBSPStringLiteral("x", true)));
        expected.add(new DBSPTupleExpression(
                new DBSPI32Literal(2, true),
                new DBSPStringLiteral("Y", true)), -1);
        expected.add(new DBSPTupleExpression(
                new DBSPI32Literal(3, true),
                new DBSPStringLiteral("Z", true)), -1);
        boolean same = change.getSet(0).sameValue(expected);
        Assert.assertTrue(same);
    }

    @Test
    public void rustFmt() throws IOException, InterruptedException {
        // Check that the rust library is properly formatted
        Utilities.runProcess(BaseSQLTests.projectDirectory + "/lib/sqllib",
                "cargo", "+nightly", "fmt", "--check");
        Utilities.runProcess(BaseSQLTests.projectDirectory + "/lib/sqllib",
                "cargo", "clippy", "--", "-D", "warnings");
    }
}
