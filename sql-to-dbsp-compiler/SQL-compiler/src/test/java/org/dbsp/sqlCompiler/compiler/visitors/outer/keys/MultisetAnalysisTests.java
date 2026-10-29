package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveDeindexOperators;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Tests for {@link MultisetAnalysis} on optimized circuits. */
public class MultisetAnalysisTests extends SqlIoTest {
    /** The map_index operators of the circuit that read a table with a primary key,
     * directly or through a differentiator */
    static List<DBSPMapIndexOperator> indexesOfKeyedTables(CompilerCircuit cc) {
        List<DBSPMapIndexOperator> result = new ArrayList<>();
        for (DBSPOperator operator : cc.getCircuit().allOperators) {
            DBSPMapIndexOperator index = operator.as(DBSPMapIndexOperator.class);
            if (index == null)
                continue;
            DBSPOperator source = index.input().node();
            if (source.is(DBSPDifferentiateOperator.class))
                source = source.inputs.get(0).node();
            if (source.is(DBSPSourceMapOperator.class))
                result.add(index);
        }
        return result;
    }

    /** Issue 7706: an index that drops the primary key of a table can hold a row twice. */
    @Test
    public void indexDroppingPrimaryKey() {
        CompilerCircuit cc = this.getCC("""
                CREATE TABLE t (id INT NOT NULL PRIMARY KEY, k INT, a INT);
                CREATE VIEW v AS SELECT k, SUM(a) AS s FROM t GROUP BY k;""");
        List<DBSPMapIndexOperator> indexes = indexesOfKeyedTables(cc);
        Assert.assertEquals(1, indexes.size());
        MultisetAnalysis multisets = new MultisetAnalysis(cc.compiler);
        multisets.apply(cc.getCircuit());
        Assert.assertTrue(multisets.isMultiset(indexes.get(0).outputPort()));
    }

    /** A pass that keeps the circuit it receives */
    static class Capture implements CircuitTransform {
        @Nullable
        DBSPCircuit circuit = null;

        @Override
        public DBSPCircuit apply(DBSPCircuit circuit) {
            this.circuit = circuit;
            return circuit;
        }

        @Override
        public String getName() {
            return "Capture";
        }
    }

    /** A deindex whose key has no columns drops nothing: the NOW() stream is a set */
    @Test
    public void deindexEmptyKey() {
        DBSPCompiler compiler = this.testCompiler();
        Capture capture = new Capture();
        compiler.optimizerHook = optimizer -> optimizer.insertBefore(RemoveDeindexOperators.class, capture);
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t (x TIMESTAMP);
                CREATE VIEW v AS SELECT * FROM t WHERE x >= NOW() - INTERVAL 1 DAY;""");
        Assert.assertNotNull(compiler.getFinalCircuit(false));
        DBSPCircuit circuit = Objects.requireNonNull(capture.circuit);
        MultisetAnalysis multisets = new MultisetAnalysis(compiler);
        multisets.apply(circuit);
        int found = 0;
        for (DBSPOperator operator : circuit.allOperators) {
            DBSPDeindexOperator deindex = operator.as(DBSPDeindexOperator.class);
            if (deindex == null || !deindex.input().getOutputIndexedZSetType().keyType.sameType(DBSPTypeTuple.EMPTY))
                continue;
            Assert.assertFalse(multisets.isMultiset(deindex.input()));
            Assert.assertFalse(multisets.isMultiset(deindex.outputPort()));
            found++;
        }
        Assert.assertEquals(1, found);
    }

    /** SAFE_CAST maps every value that does not fit in TINYINT to NULL, so the projection
     * of a set is a multiset, and the DISTINCT must stay. */
    @Test
    public void safeCastNarrowing() {
        var ccs = this.getCCS("""
                CREATE TABLE t (x INT);
                CREATE VIEW v AS SELECT DISTINCT SAFE_CAST(x AS TINYINT) AS k
                FROM (SELECT DISTINCT x FROM t);""");
        ccs.stepWeightOne("INSERT INTO t VALUES (300), (400), (1);", """
                 k
                ---
                NULL
                 1""");
    }
    /** The runtime applies distinct to the streams that a recursive circuit returns:
     * the output of the recursive view is a set, although the stream inside is a UNION ALL. */
    @Test
    public void recursiveOutputIsSet() {
        CompilerCircuit cc = this.getCC("""
                CREATE TABLE edges(a INT NOT NULL, b INT NOT NULL);
                DECLARE RECURSIVE VIEW closure(a INT NOT NULL, b INT NOT NULL);
                CREATE VIEW closure AS
                  (SELECT a, b FROM edges)
                  UNION ALL
                  (SELECT e.a, c.b FROM edges e JOIN closure c ON e.b = c.a);""");
        MultisetAnalysis multisets = new MultisetAnalysis(cc.compiler);
        multisets.apply(cc.getCircuit());
        int outputs = 0;
        for (DBSPOperator operator : cc.getCircuit().allOperators) {
            DBSPNestedOperator nested = operator.as(DBSPNestedOperator.class);
            if (nested == null)
                continue;
            for (int i = 0; i < nested.outputCount(); i++) {
                OutputPort internal = nested.internalOutputs.get(i);
                if (internal == null)
                    continue;
                Assert.assertTrue(multisets.isMultiset(internal));
                Assert.assertFalse(multisets.isMultiset(nested.getOutput(i)));
                outputs++;
            }
        }
        Assert.assertEquals(1, outputs);
    }

    static final String LENGTHENING_PROGRAM = """
            CREATE TABLE t (s VARCHAR(3));
            CREATE VIEW v AS SELECT DISTINCT CAST(u AS VARCHAR(4)) AS k
            FROM (SELECT DISTINCT UPPER(s) AS u FROM t);""";

    /** UPPER('ß') is 'SS': UPPER keeps the type VARCHAR(3) of its argument but can return a
     * longer string.  The cast to VARCHAR(4) truncates, so two different values of u can give
     * the same k: the last distinct before the view must come after the cast. */
    @Test
    public void castAfterLengtheningFunction() {
        var cc = this.getCC(LENGTHENING_PROGRAM);
        DBSPOperator operator = Objects.requireNonNull(cc.getCircuit().getSink(new ProgramIdentifier("v")));
        while (!operator.is(DBSPDistinctOperator.class) && !operator.is(DBSPStreamDistinctOperator.class)) {
            Assert.assertFalse("A map after the last distinct", operator.is(DBSPMapOperator.class));
            Assert.assertFalse("No distinct before the view", operator.inputs.isEmpty());
            operator = operator.inputs.get(0).node();
        }
    }

    /** The same program with data: 'SSSSSS' and 'SSSSS' both truncate to 'SSSS' */
    @Test
    public void castAfterLengtheningFunctionData() {
        var ccs = this.getCCS(LENGTHENING_PROGRAM);
        ccs.stepWeightOne("INSERT INTO t VALUES ('ßßß'), ('ßßs');", """
                 k
                ------
                 SSSS""");
    }

    /** True if the analysis finds injective the closure that casts the only field of a row,
     * of type {@code nullableType}, to the same type without NULL */
    boolean nonNullCastIsInjective(DBSPType nullableType) {
        DBSPVariablePath row = new DBSPTypeTuple(nullableType).ref().var();
        DBSPExpression cast = row.deref().field(0).cast(
                CalciteObject.EMPTY, nullableType.withMayBeNull(false), DBSPCastExpression.CastType.SqlUnsafe);
        DBSPClosureExpression closure = new DBSPTupleExpression(cast).closure(row);
        return new MultisetAnalysis(this.testCompiler()).isCertainlyInjective(closure);
    }

    /** The cast from a nullable VARIANT to a non-null VARIANT maps SQL NULL to the VARIANT null
     * value.  For other types the same cast fails on NULL, so it is injective. */
    @Test
    public void nonNullVariantCast() {
        Assert.assertFalse(this.nonNullCastIsInjective(DBSPTypeVariant.create(true)));
        Assert.assertTrue(this.nonNullCastIsInjective(
                DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, true)));
    }
}
