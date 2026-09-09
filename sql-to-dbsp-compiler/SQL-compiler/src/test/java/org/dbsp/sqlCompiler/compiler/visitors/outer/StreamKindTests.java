package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;

/** Every operator rejects inputs of a stream kind it cannot consume.  ValidateStreamKinds
 * checks every compiled circuit after every pass, so only the rejection itself needs a test. */
public class StreamKindTests extends SqlIoTest {
    static final String PROGRAM = """
            CREATE TABLE T(id INT NOT NULL, v INT);
            CREATE TABLE S(id INT NOT NULL, w INT);
            CREATE VIEW J AS SELECT T.id, T.v, S.w FROM T JOIN S ON T.id = S.id;
            CREATE VIEW A AS SELECT id, SUM(v) AS total FROM T GROUP BY id;
            CREATE VIEW D AS SELECT DISTINCT v FROM T;""";

    /** Compiles the program with the requested mode and returns the final circuit. */
    DBSPCircuit compile(String sql, boolean incremental) {
        CompilerOptions options = this.testOptions();
        options.languageOptions.incrementalize = incremental;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(sql);
        return BaseSQLTests.getCircuit(compiler);
    }

    /** All operators of the given class in the circuit, including nested ones */
    static <T extends DBSPOperator> List<T> operators(DBSPCircuit circuit, Class<T> clazz) {
        List<T> result = new ArrayList<>();
        for (DBSPOperator operator: circuit.allOperators) {
            if (operator.is(clazz))
                result.add(operator.to(clazz));
            if (operator.is(DBSPNestedOperator.class))
                for (DBSPOperator inner: operator.to(DBSPNestedOperator.class).getAllOperators())
                    if (inner.is(clazz))
                        result.add(inner.to(clazz));
        }
        return result;
    }

    static void expectRejected(DBSPOperator operator) {
        try {
            operator.outputKind(0);
            Assert.fail("Operator " + operator + " should reject the kinds of its inputs");
        } catch (InternalCompilerError expected) {
            Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("requires input"));
        }
    }

    /** Operators built with inputs of the wrong kind are rejected as soon as their kind is computed. */
    @Test
    public void testIllegalInputs() {
        DBSPCircuit circuit = this.compile(PROGRAM, true);
        DBSPSourceBaseOperator source = operators(circuit, DBSPSourceBaseOperator.class).get(0);
        Assert.assertEquals(StreamKind.DELTA, source.outputKind(0));
        DBSPIntegrateOperator integral = new DBSPIntegrateOperator(CalciteEmptyRel.INSTANCE, source.outputPort());
        Assert.assertEquals(StreamKind.COLLECTION, integral.outputKind(0));

        // Integrating a collection, or differentiating a delta
        expectRejected(new DBSPIntegrateOperator(CalciteEmptyRel.INSTANCE, integral.outputPort()));
        expectRejected(new DBSPDifferentiateOperator(CalciteEmptyRel.INSTANCE, source.outputPort()));
        // A linear operator with inputs of two kinds
        expectRejected(new DBSPSumOperator(CalciteEmptyRel.INSTANCE, source.outputPort(), integral.outputPort()));
        // A non-incremental operator over a delta, and an incremental one over a collection
        expectRejected(new DBSPStreamDistinctOperator(CalciteEmptyRel.INSTANCE, source.outputPort()));
        expectRejected(new DBSPDistinctOperator(CalciteEmptyRel.INSTANCE, integral.outputPort()));
    }
}
