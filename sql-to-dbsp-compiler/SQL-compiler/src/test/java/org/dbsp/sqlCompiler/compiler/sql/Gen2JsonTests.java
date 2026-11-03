package org.dbsp.sqlCompiler.compiler.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveTypedBox;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** The circuit compiled for the Gen-2 engine ({@code --gen2}) carries no Rust-codegen
 * artifacts: it has no {@code TYPEDBOX}, no {@code TypedBox<T, _>}, and no {@code clone()}, a
 * constant stays where it is used instead of moving to a {@code static} declaration, and an
 * aggregate operator keeps its per-aggregate list instead of one fold over a tuple accumulator.
 * The {@code --jit} circuit keeps the Rust forms. */
public class Gen2JsonTests extends SqlIoTest {
    /** A temporal filter against NOW(): the Rust backend boxes its window bounds. */
    static final String WINDOW_PROGRAM = """
            CREATE TABLE events(ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR, id BIGINT NOT NULL);
            CREATE VIEW recent AS SELECT id FROM events WHERE ts >= NOW() - INTERVAL 1 HOUR;""";

    /** Three aggregates that cannot use the linear form: a floating-point SUM, an ARRAY_AGG
     * (an in-place step), and a BIT_XOR. */
    static final String FOLD_PROGRAM = """
            CREATE TABLE sales(region VARCHAR NOT NULL, qty INT, score DOUBLE, tag VARCHAR);
            CREATE VIEW fold_agg AS
            SELECT region, SUM(score), ARRAY_AGG(tag), BIT_XOR(qty) FROM sales GROUP BY region;""";

    /** A GROUP BY that is incremental, so it compiles to an aggregate operator over a trace. */
    static final String INCREMENTAL_PROGRAM = """
            CREATE TABLE sales(region VARCHAR NOT NULL, score DOUBLE, tag VARCHAR);
            CREATE VIEW g AS SELECT region, SUM(score), ARRAY_AGG(tag) FROM sales GROUP BY region;""";

    /** A RANGE window: a partitioned rolling aggregate. */
    static final String ROLLING_PROGRAM = """
            CREATE TABLE t(region VARCHAR NOT NULL, ts INT NOT NULL, score DOUBLE);
            CREATE VIEW w AS SELECT region, SUM(score) OVER (
                PARTITION BY region ORDER BY ts RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM t;""";

    /** A RANGE window over a column with LATENESS: a rolling aggregate with a waterline. */
    static final String WATERLINE_PROGRAM = """
            CREATE TABLE t(region VARCHAR NOT NULL, ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR, score DOUBLE);
            CREATE VIEW w AS SELECT region, SUM(score) OVER (
                PARTITION BY region ORDER BY ts RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) FROM t;""";

    /** A string and a decimal constant, which the Rust backend hoists into statics. */
    static final String CONSTANT_PROGRAM = """
            CREATE TABLE sales(region VARCHAR NOT NULL, price DECIMAL(10, 2));
            CREATE VIEW labelled AS SELECT region || '-suffix', price * 1.25 FROM sales;""";

    /** Views that compile to the expressions whose type follows from their children: IS NULL, a
     * sort, a top-k with comparators, aggregates, a fallible cast, a common subexpression bound
     * by a Let or a Lazy, an ASOF join with a custom order, and a string constant. */
    static final String COVERAGE_PROGRAM = """
            CREATE TABLE t(id INT NOT NULL, x INT, s VARCHAR, d DOUBLE, ts TIMESTAMP NOT NULL);
            CREATE TABLE u(id INT NOT NULL, ts TIMESTAMP NOT NULL, y INT);
            CREATE VIEW nulls AS SELECT id, x IS NULL AS xn FROM t;
            CREATE VIEW sorted AS SELECT id, x FROM t ORDER BY x;
            CREATE VIEW top AS SELECT id, x FROM t ORDER BY x DESC LIMIT 3;
            CREATE VIEW agg AS SELECT id, SUM(x), MIN(s), ARRAY_AGG(d) FROM t GROUP BY id;
            CREATE VIEW casts AS SELECT CAST(s AS INT) AS i FROM t;
            CREATE VIEW cse AS SELECT CASE WHEN x > 0 THEN (x + 1) * (x + 1) ELSE 0 END AS c FROM t;
            CREATE VIEW matched AS SELECT t.id, u.y FROM t
                LEFT ASOF JOIN u MATCH_CONDITION (t.ts >= u.ts) ON t.id = u.id;
            CREATE VIEW labelled AS SELECT s || '-suffix' FROM t;""";

    DBSPCompiler compile(String program, boolean gen2, boolean checkSerialization) {
        return this.compile(program, gen2, checkSerialization, false);
    }

    DBSPCompiler compile(String program, boolean gen2, boolean checkSerialization, boolean incremental) {
        CompilerOptions options = this.testOptions();
        options.ioOptions.gen2 = gen2;
        options.ioOptions.checkSerialization = checkSerialization;
        options.languageOptions.incrementalize = incremental;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(program);
        return compiler;
    }

    String circuitJson(String program, boolean gen2) {
        return this.circuitJson(this.compile(program, gen2, false));
    }

    String circuitJson(DBSPCompiler compiler) {
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        ToJsonOuterVisitor visitor = ToJsonOuterVisitor.create(compiler, 1);
        visitor.apply(circuit);
        return visitor.getJsonString();
    }

    static int occurrences(String text, String pattern) {
        return text.split(java.util.regex.Pattern.quote(pattern), -1).length - 1;
    }

    @Test
    public void jitJsonKeepsTypedBox() {
        String json = this.circuitJson(WINDOW_PROGRAM, false);
        Assert.assertTrue(json.contains("\"TYPEDBOX\""));
        Assert.assertTrue(json.contains("\"DBSPTypeTypedBox\""));
    }

    @Test
    public void gen2JsonDropsTypedBox() {
        String json = this.circuitJson(WINDOW_PROGRAM, true);
        Assert.assertFalse(json.contains("TYPEDBOX"));
        Assert.assertFalse(json.contains("TypedBox"));
        // The window and the bound it boxed (NOW() - INTERVAL 1 HOUR) are still there, unwrapped.
        Assert.assertTrue(json.contains("\"DBSPWindowOperator\""));
        Assert.assertTrue(json.contains("\"DBSPTimeAddSub\""));
    }

    @Test
    public void jitJsonKeepsClone() {
        // ARRAY_AGG(tag) clones the VARCHAR it appends.
        Assert.assertTrue(this.circuitJson(FOLD_PROGRAM, false).contains("\"DBSPCloneExpression\""));
    }

    @Test
    public void gen2JsonDropsClone() {
        String json = this.circuitJson(FOLD_PROGRAM, true);
        Assert.assertFalse(json.contains("DBSPCloneExpression"));
        // The cloned field access is still the argument of the step.
        Assert.assertTrue(json.contains("\"array_aggN\""));
    }

    @Test
    public void jitJsonPacksTheAggregatesIntoOneFold() {
        String json = this.circuitJson(FOLD_PROGRAM, false);
        Assert.assertEquals(1, occurrences(json, "\"DBSPFold\""));
        // The packed step writes the three accumulator fields through a mutable reference.
        Assert.assertTrue(json.contains("\"DBSPAssignmentExpression\""));
        Assert.assertFalse(json.contains("\"DBSPAggregateList\""));
    }

    @Test
    public void gen2JsonKeepsTheAggregateList() {
        String json = this.circuitJson(FOLD_PROGRAM, true);
        Assert.assertEquals(1, occurrences(json, "\"DBSPAggregateList\""));
        // One entry per SQL aggregate, each with its own zero, step, and post-processing.
        Assert.assertEquals(3, occurrences(json, "\"NonLinearAggregate\""));
        Assert.assertFalse(json.contains("\"DBSPFold\""));
        // A step returns its new accumulator; nothing assigns into a packed tuple.
        Assert.assertFalse(json.contains("\"DBSPAssignmentExpression\""));
    }

    /** Every aggregate operator keeps its list, and the circuit decodes from the JSON it writes. */
    @Test
    public void gen2AggregateOperatorsKeepTheList() {
        for (String program : new String[] { INCREMENTAL_PROGRAM, ROLLING_PROGRAM, WATERLINE_PROGRAM }) {
            String json = this.circuitJson(this.compile(program, true, true, true));
            Assert.assertTrue(program, json.contains("\"DBSPAggregateList\""));
            Assert.assertFalse(program, json.contains("\"DBSPFold\""));
            Assert.assertFalse(program, json.contains("DBSPCloneExpression"));
            Assert.assertFalse(program, json.contains("TYPEDBOX"));
        }
    }

    /** A window reads the whole row of a table with a primary key. */
    @Test
    public void gen2WindowReadsTheWholeKeyedRow() {
        String json = this.circuitJson(this.compile("""
                CREATE TABLE t(id VARCHAR NOT NULL PRIMARY KEY, a INT, ts INT, x INT);
                CREATE VIEW v AS SELECT *, SUM(x) OVER (
                    PARTITION BY a ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM t;""",
                true, true, true));
        Assert.assertTrue(json.contains("\"DBSPSourceMapOperator\""));
    }

    /** The persistent ids of the aggregate operators in a Gen-2 circuit. */
    List<String> aggregateHashes(String program) {
        DBSPCompiler compiler = this.compile(program, true, false);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        List<String> hashes = new ArrayList<>();
        new CircuitVisitor(compiler) {
            @Override
            public void postorder(DBSPStreamAggregateOperator operator) {
                hashes.add(Objects.requireNonNull(OperatorHash.getHash(operator, true)).toString());
            }
        }.apply(circuit);
        return hashes;
    }

    /** The persistent id of a Gen-2 aggregate depends only on the aggregate. */
    @Test
    public void gen2AggregateHashIsStable() {
        List<String> hashes = this.aggregateHashes(FOLD_PROGRAM);
        Assert.assertEquals(1, hashes.size());
        Assert.assertEquals(hashes, this.aggregateHashes(FOLD_PROGRAM));
        Assert.assertEquals(hashes, this.aggregateHashes(
                "CREATE TABLE other(x INT);\nCREATE VIEW doubled AS SELECT x * 2, x + 1 FROM other;\n" + FOLD_PROGRAM));
    }

    @Test
    public void jitJsonHoistsConstantsIntoStatics() {
        String json = this.circuitJson(CONSTANT_PROGRAM, false);
        Assert.assertTrue(json.contains("\"DBSPStaticExpression\""));
    }

    @Test
    public void gen2JsonKeepsConstantsInline() {
        String json = this.circuitJson(CONSTANT_PROGRAM, true);
        Assert.assertFalse(json.contains("DBSPStaticItem"));
        Assert.assertFalse(json.contains("DBSPStaticExpression"));
        // The constants are still in the projection, as literals.
        Assert.assertTrue(json.contains("-suffix"));
        Assert.assertTrue(json.contains("\"DBSPDecimalLiteral\""));
    }

    /** The circuit after every pass, the Gen-2 passes included, decodes from the JSON it writes. */
    @Test
    public void gen2CircuitRoundTripsThroughJson() {
        for (String program : new String[] { WINDOW_PROGRAM, FOLD_PROGRAM, CONSTANT_PROGRAM })
            Assert.assertNotNull(this.compile(program, true, true).getFinalCircuit(false));
    }

    /** True if {@code simpleName} names a subclass of {@link DBSPExpression}. */
    static boolean isExpression(String simpleName) {
        for (String pkg : List.of("org.dbsp.sqlCompiler.ir.expression.",
                "org.dbsp.sqlCompiler.ir.expression.literal.", "org.dbsp.sqlCompiler.ir.aggregate.")) {
            try {
                return DBSPExpression.class.isAssignableFrom(Class.forName(pkg + simpleName));
            } catch (ClassNotFoundException ignored) {
            }
        }
        return false;
    }

    /** Add to {@code seen} the class of each expression in {@code node}, and to {@code untyped}
     * the class of each expression that has no type. */
    static void collectExpressions(JsonNode node, Set<String> seen, List<String> untyped) {
        JsonNode cls = node.get("class");
        if (cls != null && isExpression(cls.asText())) {
            seen.add(cls.asText());
            if (!node.has("type"))
                untyped.add(cls.asText());
        }
        for (JsonNode child : node)
            collectExpressions(child, seen, untyped);
    }

    /** Every expression in the JSON has a type, also an expression whose type follows from its
     * children, such as a field projection.  The Rust circuit keeps the clones and the statics
     * that the Gen-2 circuit drops, and an incremental circuit keeps its aggregates. */
    @Test
    public void jsonGivesEveryExpressionAType() throws JsonProcessingException {
        Set<String> seen = new HashSet<>();
        for (String program : List.of(WINDOW_PROGRAM, FOLD_PROGRAM, INCREMENTAL_PROGRAM,
                ROLLING_PROGRAM, WATERLINE_PROGRAM, CONSTANT_PROGRAM, COVERAGE_PROGRAM)) {
            for (boolean gen2 : new boolean[] { false, true }) {
                for (boolean incremental : new boolean[] { false, true }) {
                    CompilerOptions options = this.testOptions();
                    options.ioOptions.gen2 = gen2;
                    options.languageOptions.incrementalize = incremental;
                    // An ORDER BY that is kept compiles to a sort expression
                    options.languageOptions.ignoreOrderBy = false;
                    DBSPCompiler compiler = new DBSPCompiler(options);
                    compiler.submitStatementsForCompilation(program);
                    String text = this.circuitJson(compiler);
                    JsonNode json = Utilities.deterministicObjectMapper().readTree(text);
                    List<String> untyped = new ArrayList<>();
                    collectExpressions(json, seen, untyped);
                    Assert.assertEquals(List.of(), untyped);
                }
            }
        }
        // The expressions whose type follows from their children
        List<String> derived = List.of("DBSPFieldExpression", "DBSPDerefExpression",
                "DBSPBorrowExpression", "DBSPCloneExpression", "DBSPHandleErrorExpression",
                "DBSPSomeExpression", "DBSPBlockExpression", "DBSPLetExpression", "DBSPLazyExpression",
                "DBSPStaticExpression", "DBSPIsNullExpression", "DBSPSortExpression",
                "DBSPUnwrapCustomOrdExpression", "DBSPFieldComparatorExpression",
                "DBSPDirectComparatorExpression", "DBSPNoComparatorExpression", "DBSPZSetExpression",
                "DBSPMinMax", "DBSPFold", "NonLinearAggregate");
        List<String> missing = new ArrayList<>(derived);
        missing.removeAll(seen);
        Assert.assertEquals(List.of(), missing);
    }

    /** RemoveTypedBox refuses the waterline of an indexed input, whose boxed type it cannot rewrite. */
    @Test
    public void removeTypedBoxRejectsIndexedInputWaterline() {
        CompilerOptions options = this.testOptions();
        options.languageOptions.incrementalize = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t(id INT NOT NULL PRIMARY KEY, ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
                CREATE VIEW v AS SELECT * FROM t;""");
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        InternalCompilerError error = Assert.assertThrows(InternalCompilerError.class,
                () -> new RemoveTypedBox(compiler).apply(circuit));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("Cannot unbox the waterline output"));
    }
}
