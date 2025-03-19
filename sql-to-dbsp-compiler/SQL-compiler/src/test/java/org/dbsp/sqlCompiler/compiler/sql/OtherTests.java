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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.MerkleOuter;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.FunctionDocumentation;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.simple.EndToEndTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Ignore;
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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
        compiler.submitStatementForCompilation(ddl);
        return compiler;
    }

    @Test public void toStringTest() {
        String query = "CREATE VIEW V AS SELECT T.COL3 FROM T";
        DBSPCompiler compiler = this.compileDef();
        compiler.submitStatementForCompilation(query);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        String str = circuit.toString();
        String expected = """
                Circuit circuit {
                    // DBSPConstantOperator s0
                    let s0: stream<WSet<Tup3<s, s, s>>> = constant(zset!());
                    // DBSPSourceMultisetOperator s1
                    // CREATE TABLE `t` (`col1` INTEGER NOT NULL, `col2` DOUBLE NOT NULL, `col3` BOOLEAN NOT NULL, `col4` VARCHAR NOT NULL, `col5` INTEGER, `col6` DOUBLE)
                    let s1 = t();
                    // DBSPMapOperator s2
                    let s2: stream<WSet<Tup1<b>>> = s1.map((|p0: &Tup6<i32, d, b, s, i32?, d?>|
                    Tup1::new(((*p0).2), )));
                    // CREATE VIEW `v` AS
                    // SELECT `t`.`col3`
                    // FROM `schema`.`t` AS `t`
                    let s3: stream<WSet<Tup1<b>>> = s2;
                    // CREATE VIEW `error_view` AS
                    // SELECT `error_table`.`table_or_view_name`, `error_table`.`message`, `error_table`.`metadata`
                    // FROM `schema`.`error_table` AS `error_table`
                    let s4: stream<WSet<Tup3<s, s, s>>> = s0;
                }
                """;
        Assert.assertEquals(expected, str);
    }

    @Test
    public void testIntCastWarning() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.ioOptions.quiet = false;
        String query = "CREATE VIEW V AS SELECT '1_000'::INT4";
        compiler.submitStatementForCompilation(query);
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
        String sql = """
                CREATE TABLE T (COL1 INT NOT NULL, COL2 DOUBLE NOT NULL);
                CREATE VIEW V AS SELECT COL1 FROM T;""";
        File file = createInputScript(sql);
        CompilerMain.execute("-TSqlToRelCompiler=2", "-TPasses=2",
                "-o", BaseSQLTests.testFilePath, file.getPath());
        Utilities.compileAndCheckRust(BaseSQLTests.rustDirectory, true);
        Logger.INSTANCE.setDebugStream(save);
        String messages = builder.toString();
        Assert.assertTrue(messages.contains("After optimizer"));
        Assert.assertTrue(messages.contains("MergeSums"));
        Logger.INSTANCE.setLoggingLevel(SqlToRelCompiler.class, 0);
        Logger.INSTANCE.setLoggingLevel(Passes.class, 0);
    }

    @Test
    public void toCsvTest() {
        DBSPCompiler compiler = testCompiler();
        DBSPZSetExpression s = new DBSPZSetExpression(EndToEndTests.E0, EndToEndTests.E1);
        StringBuilder builder = new StringBuilder();
        ToCsvVisitor visitor = new ToCsvVisitor(compiler, builder, () -> "");
        visitor.apply(s);
        String[] lines = builder.toString().split("\n");
        Arrays.sort(lines);
        Assert.assertEquals(
                "10,1.0,false,\"Hi\",1,0,\n" +
                        "10,12.0,true,\"Hi\",,,",
                String.join("\n", lines));
    }

    @Test
    public void rustCsvTest() throws IOException, InterruptedException {
        DBSPCompiler compiler = testCompiler();
        DBSPZSetExpression data = new DBSPZSetExpression(EndToEndTests.E0, EndToEndTests.E1);
        File file = File.createTempFile("test", ".csv", new File(BaseSQLTests.rustDirectory));
        file.deleteOnExit();
        ToCsvVisitor.toCsv(compiler, file, data);
        List<DBSPStatement> list = new ArrayList<>();
        DBSPLetStatement src = new DBSPLetStatement("src",
                new DBSPApplyExpression("read_csv", data.getType(),
                        new DBSPStrLiteral(file.getAbsolutePath())));
        list.add(src);
        list.add(new DBSPApplyExpression(
                "assert_eq!", DBSPTypeVoid.INSTANCE, src.getVarReference(),
                data).toStatement());
        DBSPExpression body = new DBSPBlockExpression(list, null);
        DBSPFunction tester = new DBSPFunction("test", new ArrayList<>(),
                DBSPTypeVoid.INSTANCE, body, Linq.list("#[test]"));

        RustFileWriter writer = new RustFileWriter(BaseSQLTests.testFilePath);
        writer.add(tester);
        writer.writeAndClose(compiler);
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
    }

    @Test
    public void rustCsvTest2() throws IOException, InterruptedException {
        DBSPCompiler compiler = this.testCompiler();
        DBSPZSetExpression data = new DBSPZSetExpression(
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
                "assert_eq!", DBSPTypeVoid.INSTANCE, src.getVarReference(),
                data).toStatement());
        DBSPExpression body = new DBSPBlockExpression(list, null);
        DBSPFunction tester = new DBSPFunction("test", new ArrayList<>(),
                DBSPTypeVoid.INSTANCE, body, Linq.list("#[test]"));

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
                WITH LOW_PRICE_CTE AS (select part, MIN(price) as price from PRICE group by part)
                SELECT * FROM LOW_PRICE_CTE""";
        File file = createInputScript(statement);
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        if (messages.errorCount() > 0)
            throw new RuntimeException(messages.toString());
        Utilities.compileAndCheckRust(BaseSQLTests.rustDirectory, true);
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
            compiler.submitStatementsForCompilation(script);
            this.getCCS(compiler);
        }
    }

    void compileFile(String file, boolean run) throws SQLException, IOException, InterruptedException {
        CompilerMessages messages = CompilerMain.execute(
                "-i", "--alltables", "-q", "--ignoreOrder", "-o", BaseSQLTests.testFilePath, file);
        messages.print();
        Assert.assertEquals(0, messages.errorCount());
        if (run)
            Utilities.compileAndCheckRust(BaseSQLTests.rustDirectory, true);
        // cleanup after ourselves
        createEmptyStubs();
    }

    @Test
    public void testProjectFiles() throws IOException, InterruptedException, SQLException {
        // Compiles all the programs in the tests directory
        final String projectsDirectory = "../../demo/";
        final File dir = new File(projectsDirectory);
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
                this.compileFile(path, true);
            }
        }
    }

    @Test
    public void testPackagedDemos() throws SQLException, IOException, InterruptedException {
        final String projectsDirectory = "../../demo/packaged/sql";
        final File dir = new File(projectsDirectory);
        FilenameFilter filter = (_d, name) -> !name.contains("setup") && name.endsWith(".sql");
        String[] sqlFiles = dir.list(filter);
        assert sqlFiles != null;
        for (String sqlFile: sqlFiles) {
            // if (!sqlFile.contains("feature")) continue;
            // System.out.println(sqlFile);
            String basename = Utilities.getBaseName(sqlFile);
            String udf = basename + ".udf.rs";
            File udfFile = new File(dir.getPath() + "/" + udf);
            this.compileFile(dir.getPath() + "/" + sqlFile, !udfFile.exists());
        }
    }

    // Test the ignoreOrderBy compiler flag
    @Test
    public void testIgnoreOrderBy() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.languageOptions.ignoreOrderBy = true;
        compiler.options.ioOptions.quiet = false;
        String query = "CREATE VIEW V AS SELECT * FROM T ORDER BY T.COL2";
        compiler.submitStatementForCompilation(ddl);
        compiler.submitStatementsForCompilation(query);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        DBSPTypeZSet outputType = circuit.getSingleOutputType().to(DBSPTypeZSet.class);
        DBSPSimpleOperator source = circuit.getInput(compiler.canonicalName("T", false));
        Assert.assertNotNull(source);
        DBSPTypeZSet inputType = source.getType().to(DBSPTypeZSet.class);
        Assert.assertTrue(inputType.sameType(outputType));
        TestUtil.assertMessagesContain(compiler.messages, """
             ORDER BY clause is currently ignored
             (the result will contain the correct data, but the data is not ordered)""");
    }

    // Test the outputsAreSets compiler flag
    @Test
    public void testOutputsAreSets() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.languageOptions.outputsAreSets = true;
        String query = "CREATE VIEW V AS SELECT T.COL1 FROM T";
        compiler.submitStatementForCompilation(ddl);
        compiler.submitStatementsForCompilation(query);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        DBSPSinkOperator sink = circuit.getSink(compiler.canonicalName("V", false));
        Assert.assertNotNull(sink);
        OutputPort op = sink.input();
        // There is no optimization I can imagine which will remove the distinct
        Assert.assertTrue(op.node().is(DBSPStreamDistinctOperator.class));
    }

    @Test
    public void testNoOutput() throws IOException, SQLException {
        String sql = """
                 CREATE TABLE T (COL1 INT NOT NULL COL2 DOUBLE NOT NULL);
                 CREATE VIEW V AS SELECT COL1 FROM T""";
        File file = createInputScript(sql);
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
        String sql = """
                CREATE TABLE T (COL1 INT NOT NULL, COL2 DOUBLE NOT NULL);
                CREATE VIEW V AS SELECT COL1 FROM T;""";
        File file = createInputScript(sql);
        CompilerMessages messages = CompilerMain.execute("-q", "-o", BaseSQLTests.testFilePath, file.getPath());
        messages.print();
        Assert.assertEquals(0, messages.exitCode);
        Utilities.compileAndCheckRust(BaseSQLTests.rustDirectory, true);
    }

    @Test
    public void testCompilerExample() throws IOException, InterruptedException, SQLException {
        // The example in sql-to-dbsp-compiler/using.md
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
                    let (mut circuit, (person, errors, adult) ) = circuit(CircuitConfig::with_workers(2)).unwrap();
                    // Feed two input records to the circuit.
                    // First input has a count of "1"
                    person.push( (SqlString::from_ref("Bob"), Some(12), Some(true)).into(), 1 );
                    // Second input has a count of "2"
                    person.push( (SqlString::from_ref("Tom"), Some(20), Some(false)).into(), 2 );
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
                        .configure_deserializer(RecordFormat::Csv(Default::default()))
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
        Assert.assertEquals(0, message.exitCode);
        Assert.assertTrue(file.exists());

        File rust = new File(BaseSQLTests.testFilePath);
        try (FileWriter fr = new FileWriter(rust, true)) { // append
            fr.write(rustHandlesTest);
        }
        Utilities.compileAndCheckRust(BaseSQLTests.rustDirectory, true);

        // Second test
        message = CompilerMain.execute(
                "-i", "-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(0, message.exitCode);
        Assert.assertTrue(file.exists());

        rust = new File(BaseSQLTests.testFilePath);
        try (FileWriter fr = new FileWriter(rust, true)) { // append
            fr.write(rustCatalogTest);
        }
        Utilities.compileAndCheckRust(BaseSQLTests.rustDirectory, true);
    }

    @Test
    public void testCompilerToPng() throws IOException, SQLException {
        if (!Utilities.isDotInstalled())
            return;
        String sql ="""
                 CREATE TABLE T (COL1 INT NOT NULL, COL2 DOUBLE NOT NULL);
                 CREATE VIEW V AS SELECT COL1 FROM T WHERE COL1 > 5;""";
        File file = createInputScript(sql);
        File png = File.createTempFile("out", ".png", new File("."));
        png.deleteOnExit();
        CompilerMessages message = CompilerMain.execute("-png", "-o", png.getPath(), file.getPath());
        Assert.assertEquals(0, message.exitCode);
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
        compiler.submitStatementsForCompilation(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        Assert.assertEquals(1, circuit.getOutputCount());

        String oneMore = "CREATE VIEW W AS SELECT * FROM V;";
        compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql + oneMore);
        circuit = getCircuit(compiler);
        Assert.assertEquals(2, circuit.getOutputCount());
    }

    @Test
    public void testRemove() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementForCompilation("CREATE TABLE T(I INTEGER, S VARCHAR)");
        CompilerCircuitStream ccs = this.getCCS(compiler);
        Change change = ccs.toChange("""
                INSERT INTO T VALUES(1, 'x');
                REMOVE FROM T VALUES(2, 'Y');
                REMOVE FROM T VALUES(3, 'Z');""").simplify(compiler);
        DBSPZSetExpression expected = DBSPZSetExpression.emptyWithElementType(
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
    public void indexTest() {
        String sql = """
                CREATE TABLE T(id int, v VARCHAR, z INT ARRAY);
                CREATE VIEW V AS SELECT * FROM T;
                CREATE INDEX IX ON V(id, v);""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void merkleTest() {
        // Compile two programs that have some query in common and check that the circuts produced
        // have nodes in common.
        var cc0 = this.getCC("""
                CREATE TABLE tab0(pk INTEGER, col0 INTEGER, col1 REAL, col2 TEXT, col3 INTEGER);
                CREATE VIEW V AS SELECT pk FROM tab0
                WHERE (col3 < 73 AND col3 IN (SELECT col0 FROM tab0 WHERE col0 = 3)) OR col1 > 8.64""");
        MerkleOuter visitor0 = new MerkleOuter(cc0.compiler);
        visitor0.apply(cc0.getCircuit());

        var cc1 = this.getCC("""
                CREATE TABLE tab0(pk INTEGER, col0 INTEGER, col1 REAL, col2 TEXT, col3 INTEGER);
                CREATE VIEW W AS SELECT pk + col0 FROM tab0;
                CREATE VIEW V AS SELECT pk FROM tab0
                WHERE (col3 < 73 AND col3 IN (SELECT col0 FROM tab0 WHERE col0 = 3)) OR col1 > 8.64""");
        MerkleOuter visitor1 = new MerkleOuter(cc1.compiler);
        visitor1.apply(cc1.getCircuit());
        Set<String> c0 = new HashSet<>(visitor0.operatorHash.values());
        Set<String> c1 = new HashSet<>(visitor1.operatorHash.values());
        Set<String> common = new HashSet<>(c0);
        // All nodes in circuit0 must be in circuit1
        common.retainAll(c1);
        assert common.size() == c0.size();
    }

    @Test
    public void serializationTest() throws JsonProcessingException {
        var cc = this.getCC("""
                CREATE TABLE tab0(pk INTEGER, col0 INTEGER, col1 REAL, col2 TEXT, col3 INTEGER);
                CREATE VIEW V AS SELECT pk FROM tab0
                WHERE (col3 < 73 AND col3 IN (SELECT col0 FROM tab0 WHERE col0 = 3)) OR col1 > 8.64""");
        ToJsonOuterVisitor visitor = ToJsonOuterVisitor.create(cc.compiler, 1);
        visitor.apply(cc.getCircuit());
        String str = visitor.getJsonString();
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        JsonNode parsed = mapper.readTree(str);
        JsonDecoder decoder = new JsonDecoder();
        DBSPCircuit decoded = decoder.decodeOuter(parsed, DBSPCircuit.class);
        Assert.assertNotNull(decoded);
    }

    @Test @Ignore("To be invoked manually every time a new function is added")
    public void generateFunctionIndex() throws IOException {
        // When invoked it generates documentation for the supported functions and operators
        // in the specified file.
        String file = "../../docs/sql/function-index.md";
        FunctionDocumentation.generateIndex(file);
    }

    @Test
    public void rustFmt() throws IOException, InterruptedException {
        // Don't run this in CI we don't recompile at the point where we run the tests
        if (System.getenv("CI") != null)
            return;
        // Check that the rust library is properly formatted
        Utilities.runProcess(BaseSQLTests.projectDirectory + "/../crates/sqllib",
                "cargo", "fmt", "--all", "--", "--check");
        Utilities.runProcess(BaseSQLTests.projectDirectory + "/../crates/sqllib",
                "cargo", "clippy", "--", "-D", "warnings");
    }
}
