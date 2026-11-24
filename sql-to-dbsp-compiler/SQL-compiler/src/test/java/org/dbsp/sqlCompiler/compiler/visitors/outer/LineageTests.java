package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPReturnExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampTzLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.ZSetShape;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestampTz;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LineageTests extends BaseSQLTests {
    @Test
    public void testInnerLineage() {
        Lineage outer = new Lineage(this.testCompiler(), false);
        Lineage.InnerLineage lineage = new Lineage.InnerLineage(this.testCompiler(), outer);

        DBSPType i32 = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, false);
        var var = new DBSPVariablePath(new DBSPTypeTuple(i32, i32, i32).ref());
        DBSPExpression plus = new DBSPBinaryExpression(
                CalciteObject.EMPTY, i32, DBSPOpcode.ADD, var.deref().field(0), var.deref().field(1));
        DBSPClosureExpression tup = new DBSPTupleExpression(plus, var.deref().field(2)).closure(var);

        List<Lineage.ValueSource> cols = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Lineage.ValueSource col = new Lineage.Atom<>(
                    new Lineage.ColumnSet(new Lineage.TableColumn("t", "col" + i)));
            cols.add(col);
        }
        DBSPParameter param = tup.parameters[0];
        Map<DBSPParameter, Lineage.ValueSource> initial = new HashMap<>();
        initial.put(param, new Lineage.Ref(new Lineage.Tuple(cols)));

        Lineage.ValueSource result = lineage.analyze(tup, initial);
        Assert.assertNotNull(result);
        Assert.assertEquals("Tuple[fields=[Unknown[], [t.col2]]]", result.toString());

        ///////////////

        var = new DBSPVariablePath(new DBSPTypeRawTuple(new DBSPTypeTuple(i32, i32).ref(), new DBSPTypeTuple(i32, i32, i32).ref()));
        DBSPExpression func = new DBSPApplyExpression("func", i32, var.field(1).deref()).applyClone();
        DBSPExpression comp = new DBSPBinaryExpression(CalciteObject.EMPTY, DBSPTypeBool.INSTANCE,
                DBSPOpcode.EQ, var.field(1).deref().field(0), var.field(0).deref().field(0));
        DBSPIfExpression cond = new DBSPIfExpression(CalciteObject.EMPTY, comp,
                var.field(0).deref().field(1), var.field(1).deref().field(2));
        tup = new DBSPTupleExpression(
                cond,
                func,
                var.field(1).deref().field(1).applyClone(),
                new DBSPI32Literal(1)).closure(var);

        List<Lineage.ValueSource> cols0 = new ArrayList<>();
        List<Lineage.ValueSource> cols1 = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Lineage.ValueSource col = new Lineage.Atom<>(
                    new Lineage.ColumnSet(new Lineage.TableColumn("t", "col" + i)));
            cols0.add(col);
        }
        for (int i = 2; i < 5; i++) {
            Lineage.ValueSource col = new Lineage.Atom<>(
                    new Lineage.ColumnSet(new Lineage.TableColumn("t", "col" + i)));
            cols1.add(col);
        }

        param = tup.parameters[0];
        initial.put(param, new Lineage.Tuple(Linq.list(
                new Lineage.Ref(new Lineage.Tuple(cols0)),
                new Lineage.Ref(new Lineage.Tuple(cols1)))));
        result = lineage.analyze(tup, initial);
        Assert.assertNotNull(result);
        Assert.assertEquals("Tuple[fields=[Unknown[], Unknown[], [t.col3], Constant[value=1]]]", result.toString());
    }

    /** COALESCE keeps the lineage its operands share */
    @Test
    public void testCoalesceLineage() {
        Lineage outer = new Lineage(this.testCompiler(), false);
        Lineage.InnerLineage lineage = new Lineage.InnerLineage(this.testCompiler(), outer);

        DBSPType i32 = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, true);
        var var = new DBSPVariablePath(new DBSPTypeTuple(i32, i32, i32).ref());
        DBSPExpression same = new DBSPBinaryExpression(
                CalciteObject.EMPTY, i32, DBSPOpcode.COALESCE, var.deref().field(0), var.deref().field(1));
        DBSPExpression different = new DBSPBinaryExpression(
                CalciteObject.EMPTY, i32, DBSPOpcode.COALESCE, var.deref().field(0), var.deref().field(2));
        DBSPClosureExpression tup = new DBSPTupleExpression(same, different).closure(var);

        // Fields 0 and 1 come from the same column
        Lineage.ValueSource x = new Lineage.Atom<>(new Lineage.ColumnSet(new Lineage.TableColumn("t", "x")));
        Lineage.ValueSource y = new Lineage.Atom<>(new Lineage.ColumnSet(new Lineage.TableColumn("t", "y")));
        Map<DBSPParameter, Lineage.ValueSource> initial = new HashMap<>();
        initial.put(tup.parameters[0], new Lineage.Ref(new Lineage.Tuple(Linq.list(x, x, y))));

        Lineage.ValueSource result = lineage.analyze(tup, initial);
        Assert.assertNotNull(result);
        Assert.assertEquals("Tuple[fields=[[t.x], Unknown[]]]", result.toString());
    }

    @Test
    public void testCorrelateFlag() throws IOException, SQLException {
        String sql = """
                CREATE TABLE T(X INT, Y INT, Z INT);
                CREATE TABLE S(X INT, Y INT);
                CREATE LOCAL VIEW W AS SELECT T.X AS X, S.Y AS Y FROM T JOIN S ON T.X = S.X;
                CREATE VIEW V AS SELECT X+1, Y, 2 FROM W WHERE X - 2 < Y AND x > 0;""";
        File file = createInputScript(sql);
        String expected = """
                [Correlated:] [s.x, t.x]
                [Compared:] [ s.x > 0 ]
                [Compared:] [ t.x > 0 ]
                """;
        Assert.assertEquals(expected, report(file, false));
        if (Boolean.getBoolean("feldera.optimizer.compareRustLineage"))
            Assert.assertEquals(expected, report(file, true));
    }

    @Test
    public void correlateColumnsAfterExceptAll() throws IOException, SQLException {
        File input = createInputScript("""
                CREATE TABLE l(k INT NOT NULL, flag BOOLEAN NOT NULL);
                CREATE TABLE r(k INT NOT NULL);
                CREATE LOCAL VIEW d AS SELECT k FROM l EXCEPT ALL SELECT k FROM l WHERE flag;
                CREATE VIEW v AS SELECT d.k FROM d JOIN r ON d.k = r.k;""");
        String expected = "[Correlated:] [l.k, r.k]\n";
        Assert.assertEquals(expected, report(input, false));
        if (Boolean.getBoolean("feldera.optimizer.compareRustLineage"))
            Assert.assertEquals(expected, report(input, true));
    }

    @Test
    public void lineageChecksThePhysicalKeyValueWidthAfterEarlierComparisons() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLineage"));
        var compiler = this.testCompiler();
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t(x INT NOT NULL);
                CREATE TABLE keyed(id INT NOT NULL PRIMARY KEY, a INT, b INT);
                CREATE VIEW v AS SELECT x FROM t UNION ALL SELECT id FROM keyed;
                """);
        var compiled = compiler.getFinalCircuit(true);
        var t = compiled.getInput(new ProgramIdentifier("t", false)).asOperator();
        var keyed = compiled.getInput(new ProgramIdentifier("keyed", false)).asOperator();
        Assert.assertEquals(2, keyed.getOutput(0).getOutputRowType().to(DBSPTypeTupleBase.class).size());
        var row = new DBSPVariablePath("row", t.getOutput(0).getOutputRowType().ref());
        var site = CalciteEmptyRel.INSTANCE;
        var compared = new DBSPBinaryExpression(site, DBSPTypeBool.INSTANCE, DBSPOpcode.GT,
                row.deref().field(0), new DBSPI32Literal(0));
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        circuit.addOperator(t);
        circuit.addOperator(new DBSPFilterOperator(site, compared.closure(row), t.getOutput(0)));
        circuit.addOperator(keyed);
        var message = assertLineageFailure(compiler, circuit, InternalCompilerError.class, List.of("[ t.x > 0 ]"));
        Assert.assertEquals("Assertion failed", message);
    }

    @Test
    public void lineageReportsEarlierComparisonsBeforeReadingAMissingPort() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLineage"));
        var compiler = this.testCompiler();
        compiler.submitStatementsForCompilation("CREATE TABLE t(x INT NOT NULL); CREATE VIEW v AS SELECT x FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var site = CalciteEmptyRel.INSTANCE;
        var row = new DBSPVariablePath("row", source.getOutput(0).getOutputRowType().ref());
        var positive = new DBSPBinaryExpression(site, DBSPTypeBool.INSTANCE, DBSPOpcode.GT,
                row.deref().field(0), new DBSPI32Literal(0));
        var first = new DBSPFilterOperator(site, positive.closure(row), source.getOutput(0));
        // The lineage pass normally runs before incrementalization. It has no
        // lineage entry for an incremental distinct when replayed on this circuit.
        var distinct = new DBSPDistinctOperator(site, first.outputPort());
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        circuit.addOperator(source);
        circuit.addOperator(first);
        circuit.addOperator(distinct);
        circuit.addOperator(new DBSPFilterOperator(site, first.getClosureFunction(), distinct.outputPort()));
        String message = assertLineageFailure(compiler, circuit, RuntimeException.class, List.of("[ t.x > 0 ]"));
        Assert.assertEquals("Key '" + distinct.outputPort() + "' does not exist in map", message);
    }

    @Test
    public void lineageReportsEarlierOperatorsBeforeRejectingAnInvalidBinding() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLineage"));
        var compiler = this.testCompiler();
        compiler.submitStatementsForCompilation("CREATE TABLE t(x INT NOT NULL); CREATE VIEW v AS SELECT x FROM t;");
        var table = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false));
        var source = table.asOperator();
        var site = CalciteEmptyRel.INSTANCE;
        var row = new DBSPVariablePath("row", source.getOutput(0).getOutputRowType().ref());
        var positive = new DBSPBinaryExpression(site, DBSPTypeBool.create(false), DBSPOpcode.GT,
                row.deref().field(0), new DBSPI32Literal(0));
        var first = new DBSPFilterOperator(site, positive.closure(row), source.getOutput(0));
        var shared = row.deepCopy();
        var other = new DBSPVariablePath("row", row.getType());
        var free = new DBSPVariablePath("missing", row.getType());
        for (var read : List.of(free, shared)) {
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            circuit.addOperator(source);
            circuit.addOperator(first);
            // The second comparison is in the failing closure. Resolution must
            // inspect the nested lambda before the interpreter reports that comparison.
            var second = new DBSPBinaryExpression(site, DBSPTypeBool.create(false), DBSPOpcode.LT,
                    shared.deref().field(0), new DBSPI32Literal(10));
            var body = new DBSPTupleExpression(second, read.deref().field(0).closure(other));
            circuit.addOperator(new DBSPMapOperator(site, body.closure(row), first.outputPort()));
            assertLineageFailure(compiler, circuit, InternalCompilerError.class, List.of("[ t.x > 0 ]"));
        }
    }

    @Test
    public void lineageVisitsOperandsBeforeRejectingAssignmentOrReturn() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLineage"));
        var compiler = this.testCompiler();
        compiler.submitStatementsForCompilation("CREATE TABLE t(x INT NOT NULL); CREATE VIEW v AS SELECT x FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var site = CalciteEmptyRel.INSTANCE;
        var row = new DBSPVariablePath("row", source.getOutput(0).getOutputRowType().ref());
        var compared = new DBSPBinaryExpression(site, DBSPTypeBool.INSTANCE, DBSPOpcode.GT,
                row.deref().field(0), new DBSPI32Literal(3));
        var returned = new DBSPReturnExpression(site, compared);
        var assignment = new DBSPAssignmentExpression(row.deref().field(0), compared);
        for (var failed : List.of(returned, assignment, new DBSPReturnExpression(site, assignment),
                new DBSPAssignmentExpression(returned, new DBSPI32Literal(10)))) {
            // A following comparison must never reach the report.
            var later = new DBSPBinaryExpression(site, DBSPTypeBool.INSTANCE, DBSPOpcode.LT,
                    row.deepCopy().deref().field(0), new DBSPI32Literal(5));
            var closure = new DBSPTupleExpression(failed, later).closure(row);
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            circuit.addOperator(source);
            circuit.addOperator(new DBSPMapOperator(site, closure, source.getOutput(0)));
            var message = assertLineageFailure(compiler, circuit, InternalCompilerError.class, List.of("[ t.x > 3 ]"));
            for (boolean lossless : List.of(false, true)) {
                var shape = new ZSetShape(2);
                var copy = Assert.assertThrows(InternalCompilerError.class,
                        () -> RustCircuitPass.columnCopies(compiler, closure, shape, lossless));
                var fixed = Assert.assertThrows(InternalCompilerError.class,
                        () -> RustCircuitPass.fixedColumns(compiler, closure, shape, lossless));
                Assert.assertEquals(message, lineageMessage(copy));
                Assert.assertEquals(message, lineageMessage(fixed));
            }
        }
    }

    @Test
    public void lineageIntersectsZonedConstantsInJavaOperandOrder() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLineage"));
        var compiler = this.testCompiler();
        compiler.submitStatementsForCompilation("CREATE TABLE t(x INT NOT NULL); CREATE VIEW v AS SELECT x FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var site = CalciteEmptyRel.INSTANCE;
        var row = new DBSPVariablePath("row", source.getOutput(0).getOutputRowType().ref());
        var type = DBSPTypeTimestampTz.INSTANCE;
        var east = new DBSPTimestampTzLiteral(site, type, new TimestampWithTimeZoneString("2020-01-01 00:00:00 GMT+19:00"));
        var west = new DBSPTimestampTzLiteral(site, type, new TimestampWithTimeZoneString("2020-01-01 00:00:00 GMT-20:00"));
        for (var values : List.of(List.of(east, west), List.of(west, east))) {
            var compared = new DBSPBinaryExpression(site, DBSPTypeBool.INSTANCE, DBSPOpcode.GT,
                    row.deref().field(0), new DBSPI32Literal(7));
            var merged = new DBSPIfExpression(site, compared, values.get(0), values.get(1));
            var closure = new DBSPTupleExpression(merged).closure(row);
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            circuit.addOperator(source);
            circuit.addOperator(new DBSPMapOperator(site, closure, source.getOutput(0)));
            var message = assertLineageFailure(compiler, circuit, java.time.DateTimeException.class, List.of("[ t.x > 7 ]"));
            var error = Assert.assertThrows(java.time.DateTimeException.class,
                    () -> RustCircuitPass.columnCopies(compiler, closure, new ZSetShape(1), false));
            Assert.assertEquals(message, error.getMessage());
        }
    }

    private static String assertLineageFailure(DBSPCompiler compiler, DBSPCircuit circuit,
                                               Class<? extends RuntimeException> errorClass,
                                               List<String> expectedFindings) {
        String expected = null;
        for (boolean hybrid : List.of(false, true)) {
            var findings = new ArrayList<String>();
            var report = new Lineage(compiler, false) {
                @Override
                public void reportCompared(ColumnCompared compared) {
                    findings.add(compared.toString());
                }
            };
            var decisions = new ArrayList<Boolean>();
            compiler.stateObserver = new DBSPCompiler.StateObserver() {
                @Override
                public void nativeDecision(String pass, boolean answered) {
                    if (pass.equals("Lineage"))
                        decisions.add(answered);
                }

                @Override
                public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit graph) {}

                @Override
                public void after(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit graph, boolean done) {}
            };
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            CircuitTransform pass = hybrid ? new RustCircuitPass(compiler, report, new OptimizerSnapshots(compiler)) : report;
            var error = Assert.assertThrows(errorClass, () -> pass.apply(circuit));
            Assert.assertEquals(expectedFindings, findings);
            Assert.assertEquals(hybrid ? List.of(true) : List.of(), decisions);
            String message = lineageMessage(error);
            if (hybrid)
                Assert.assertEquals(expected, message);
            else
                expected = message;
        }
        return expected;
    }

    private static String lineageMessage(RuntimeException error) {
        // Utilities.fail and Utilities.enforce append the active optimizer's stack.
        String message = error.getMessage();
        return message.startsWith("Lineage analysis does not support return:") || message.startsWith("Assertion failed\n")
                ? message.lines().findFirst().orElseThrow() : message;
    }

    private static String report(File input, boolean rust) throws SQLException {
        List<String> options = new ArrayList<>(List.of("--noRust", "--correlatedColumns"));
        if (rust)
            options.add("--rust-optimizer-passes");
        options.add(input.getPath());
        PrintStream save = System.out;
        var capture = new ByteArrayOutputStream();
        try (var stream = new PrintStream(capture)) {
            System.setOut(stream);
            var messages = CompilerMain.execute(options.toArray(String[]::new));
            Assert.assertEquals(messages.toString(), 0, messages.exitCode);
        } finally {
            System.setOut(save);
        }
        return capture.toString();
    }
}
