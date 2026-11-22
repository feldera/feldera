package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;

/** Every operator rejects inputs of a stream kind it cannot consume.  ValidateStreamKinds
 * checks every compiled circuit after every pass, so only the rejection itself needs a test. */
public class StreamKindTests extends StreamingTestBase {
    static final String PROGRAM = """
            CREATE TABLE T(id INT NOT NULL, v INT);
            CREATE TABLE S(id INT NOT NULL, w INT);
            CREATE VIEW J AS SELECT T.id, T.v, S.w FROM T JOIN S ON T.id = S.id;
            CREATE VIEW A AS SELECT id, SUM(v) AS total FROM T GROUP BY id;
            CREATE VIEW D AS SELECT DISTINCT v FROM T;""";

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

    @Test
    public void validateEachPassResultIncludingChangesToTheSameCircuit() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var compiled = this.getCC(PROGRAM).getCircuit();
        var source = operators(compiled, DBSPSourceBaseOperator.class).get(0);
        var validator = new RustCircuitPass.StreamValidator();
        var circuit = new DBSPCircuit(new ProgramMetadata(), true);
        circuit.addOperator(source);
        new ValidateStreamKinds(compiler).apply(circuit);
        boolean compareRust = Boolean.getBoolean("feldera.optimizer.compareRustStreamKinds");
        if (compareRust)
            validator.validate(circuit);

        var integrated = new DBSPIntegrateOperator(CalciteEmptyRel.INSTANCE, source.outputPort());
        circuit.addOperator(integrated);
        circuit.addOperator(new DBSPSumOperator(CalciteEmptyRel.INSTANCE, source.outputPort(), integrated.outputPort()));
        var wrongMode = new DBSPCircuit(new ProgramMetadata(), false);
        wrongMode.addOperator(source);
        for (var invalid : List.of(circuit, wrongMode)) {
            var expected = Assert.assertThrows(InternalCompilerError.class,
                    () -> new ValidateStreamKinds(compiler).apply(invalid));
            if (compareRust) {
                var actual = Assert.assertThrows(InternalCompilerError.class, () -> validator.validate(invalid));
                Assert.assertEquals(expected.getMessage().lines().findFirst(), actual.getMessage().lines().findFirst());
            }
        }
    }

    /** Operators built with inputs of the wrong kind are rejected as soon as their kind is computed. */
    @Test
    public void testIllegalInputs() {
        DBSPCircuit circuit = this.getCC(PROGRAM).getCircuit();
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
