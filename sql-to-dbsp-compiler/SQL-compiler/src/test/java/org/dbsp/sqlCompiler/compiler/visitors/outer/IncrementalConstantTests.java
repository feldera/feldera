package org.dbsp.sqlCompiler.compiler.visitors.outer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/** Tests for the passes that rewrite a {@link DBSPIncrementalConstantOperator}. */
public class IncrementalConstantTests {
    static final DBSPType ROW = new DBSPTypeTuple(DBSPTypeBool.INSTANCE);

    /** The rows (true) and (false). */
    static DBSPExpression rows() {
        return new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral(true)),
                new DBSPTupleExpression(new DBSPBoolLiteral(false)));
    }

    static <T> List<T> all(DBSPCircuit circuit, Class<T> clazz) {
        return Linq.map(Linq.where(circuit.allOperators, o -> o.is(clazz)), o -> o.to(clazz));
    }

    static <T> T single(DBSPCircuit circuit, Class<T> clazz) {
        List<T> found = all(circuit, clazz);
        Assert.assertEquals(1, found.size());
        return found.get(0);
    }

    /** An incremental constant with {@link #rows()}, a filter of it, and a negation of the filter.
     * If 'byColumn' the filter keeps the rows whose column is true, else it keeps no row. */
    static DBSPCircuit filterCircuit(boolean byColumn) {
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), true);
        var changes = new DBSPIncrementalConstantOperator(CalciteEmptyRel.INSTANCE, rows());
        circuit.addOperator(changes);
        var row = new DBSPVariablePath(ROW.ref());
        DBSPExpression condition = byColumn ? row.deref().field(0) : new DBSPBoolLiteral(false);
        var filter = new DBSPFilterOperator(CalciteEmptyRel.INSTANCE, condition.closure(row), changes.outputPort());
        circuit.addOperator(filter);
        circuit.addOperator(new DBSPNegateOperator(CalciteEmptyRel.INSTANCE, filter.outputPort()));
        return circuit;
    }

    /** CSE merges equal incremental constants, but never a constant with an incremental constant. */
    @Test
    public void cseKeepsConstantsOfDifferentKindsApart() {
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), true);
        circuit.addOperator(new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, rows()));
        circuit.addOperator(new DBSPIncrementalConstantOperator(CalciteEmptyRel.INSTANCE, rows()));
        circuit.addOperator(new DBSPIncrementalConstantOperator(CalciteEmptyRel.INSTANCE, rows()));
        DBSPCircuit result = new CSE(new DBSPCompiler(new CompilerOptions())).apply(circuit);
        single(result, DBSPConstantOperator.class);
        single(result, DBSPIncrementalConstantOperator.class);
    }

    @Test
    public void jsonKeepsIncrementalConstant() throws JsonProcessingException {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), true);
        circuit.addOperator(new DBSPIncrementalConstantOperator(CalciteEmptyRel.INSTANCE, rows()));
        ToJsonOuterVisitor visitor = ToJsonOuterVisitor.create(compiler, 1);
        visitor.apply(circuit);
        JsonNode parsed = Utilities.deterministicObjectMapper().readTree(visitor.getJsonString());
        JsonDecoder decoder = new JsonDecoder(compiler.sqlToRelCompiler.typeFactory);
        DBSPCircuit decoded = decoder.decodeOuter(parsed, DBSPCircuit.class);
        DBSPIncrementalConstantOperator constant = single(decoded, DBSPIncrementalConstantOperator.class);
        Assert.assertTrue(EquivalenceContext.equiv(rows(), constant.getFunction()));
    }

    /** The integral of an incremental constant is the constant. */
    @Test
    public void integralOfIncrementalConstantIsConstant() {
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), true);
        var changes = new DBSPIncrementalConstantOperator(CalciteEmptyRel.INSTANCE, rows());
        circuit.addOperator(changes);
        var integral = new DBSPIntegrateOperator(CalciteEmptyRel.INSTANCE, changes.outputPort());
        circuit.addOperator(integral);
        circuit.addOperator(new DBSPNegateOperator(CalciteEmptyRel.INSTANCE, integral.outputPort()));
        DBSPCircuit result = new RemoveIAfterD(new DBSPCompiler(new CompilerOptions())).apply(circuit);
        Assert.assertTrue(all(result, DBSPIntegrateOperator.class).isEmpty());
        DBSPConstantOperator constant = single(result, DBSPNegateOperator.class)
                .input().node().to(DBSPConstantOperator.class);
        Assert.assertTrue(EquivalenceContext.equiv(rows(), constant.getFunction()));
    }

    /** A filter of an incremental constant becomes an incremental constant with the rows that pass. */
    @Test
    public void filterFoldsIntoIncrementalConstant() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPCircuit circuit = filterCircuit(true);
        // Not OptimizeWithGraph: its dead-code pass would remove this circuit, which has no sink
        Graph graph = new Graph(compiler);
        graph.apply(circuit);
        DBSPCircuit result = new PullFilterVisitor(compiler, graph.getGraphs()).apply(circuit);
        Assert.assertTrue(all(result, DBSPFilterOperator.class).isEmpty());
        DBSPIncrementalConstantOperator constant = single(result, DBSPNegateOperator.class)
                .input().node().to(DBSPIncrementalConstantOperator.class);
        DBSPExpression kept = new DBSPZSetExpression(new DBSPTupleExpression(new DBSPBoolLiteral(true)));
        Assert.assertTrue(EquivalenceContext.equiv(kept, constant.getFunction()));
    }

    /** A filter that keeps no row of a stream of changes becomes an empty incremental constant. */
    @Test
    public void falseFilterBecomesEmptyIncrementalConstant() {
        DBSPCircuit result = new RemoveConstantFilters(new DBSPCompiler(new CompilerOptions()))
                .apply(filterCircuit(false));
        DBSPIncrementalConstantOperator constant = single(result, DBSPNegateOperator.class)
                .input().node().to(DBSPIncrementalConstantOperator.class);
        Assert.assertTrue(PropagateEmptySources.isEmpty(constant.getFunction()));
    }
}
