package org.dbsp.sqlCompiler.compiler.visitors.outer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPAccumulatorOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPChainAggregateKernelOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPDelayInputOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPDelayOutputOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPIntegralInputOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPJoinKernelOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPIntegralOutputOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.ViewMetadata;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWeight;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.HashString;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** Tests for the operators in the primitives package. */
public class PrimitivesTests {
    static final ProgramIdentifier TABLE = new ProgramIdentifier("T");
    static final DBSPTypeStruct STRUCT = new DBSPTypeStruct(CalciteObject.EMPTY, TABLE, Linq.list(
            new DBSPTypeStruct.Field(CalciteObject.EMPTY, new ProgramIdentifier("x"), 0, DBSPTypeBool.INSTANCE)),
            false);
    static final DBSPTypeZSet ZSET = new DBSPTypeZSet(STRUCT.toTuple());

    /** Table T with one boolean column x. */
    static DBSPSourceMultisetOperator source(StreamKind kind) {
        InputColumnMetadata column = new InputColumnMetadata(CalciteObject.EMPTY, new ProgramIdentifier("x"),
                DBSPTypeBool.INSTANCE, false, null, null, null);
        return new DBSPSourceMultisetOperator(
                CalciteEmptyRel.INSTANCE, CalciteObject.EMPTY, ZSET, STRUCT,
                new TableMetadata(TABLE, Linq.list(column), new ArrayList<>(), null, false, false, null),
                TABLE, kind, null);
    }

    /** The integral of table T as a cycle: sum = T + z^-1(sum), and a view V reads sum. */
    static DBSPCircuit delayCircuit() {
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var source = source(StreamKind.COLLECTION);
        circuit.addOperator(source);
        var delayOutput = new DBSPDelayOutputOperator(CalciteEmptyRel.INSTANCE, null, ZSET, StreamKind.COLLECTION);
        circuit.addOperator(delayOutput);
        var sum = new DBSPSumOperator(CalciteEmptyRel.INSTANCE, source.outputPort(), delayOutput.outputPort());
        circuit.addOperator(sum);
        circuit.addOperator(new DBSPDelayInputOperator(CalciteEmptyRel.INSTANCE, sum.outputPort(), delayOutput));
        ProgramIdentifier view = new ProgramIdentifier("V");
        ViewMetadata viewMeta = new ViewMetadata(view, new ArrayList<>(), SqlCreateView.ViewKind.STANDARD,
                -1, false, false, null);
        circuit.addOperator(new DBSPSinkOperator(
                CalciteEmptyRel.INSTANCE, view, "V", STRUCT, viewMeta, sum.outputPort()));
        return circuit;
    }

    /** The changes of table T accumulated; output 'port' of the accumulator feeds a split integral. */
    static DBSPCircuit accumulatorCircuit(int port) {
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), true);
        var source = source(StreamKind.DELTA);
        circuit.addOperator(source);
        var integralOutput = new DBSPIntegralOutputOperator(CalciteEmptyRel.INSTANCE, ZSET);
        circuit.addOperator(integralOutput);
        var accumulator = new DBSPAccumulatorOperator(CalciteEmptyRel.INSTANCE, source.outputPort());
        circuit.addOperator(accumulator);
        circuit.addOperator(new DBSPIntegralInputOperator(
                CalciteEmptyRel.INSTANCE, accumulator.getOutput(port), integralOutput));
        return circuit;
    }

    /** A chain aggregate kernel over rows of one boolean.  The step returns the stored value if
     * 'keep', else the new row. */
    static DBSPChainAggregateKernelOperator kernel(
            boolean keep, DBSPTypeIndexedZSet type, OutputPort delta, OutputPort previous) {
        var row = new DBSPVariablePath(type.elementType.ref());
        var weight = new DBSPVariablePath(DBSPTypeWeight.INSTANCE);
        DBSPClosureExpression init = new DBSPTupleExpression(row.deref().field(0)).closure(row, weight);
        var stored = new DBSPVariablePath(type.elementType);
        var next = new DBSPVariablePath(type.elementType.ref());
        var nextWeight = new DBSPVariablePath(DBSPTypeWeight.INSTANCE);
        DBSPExpression result = keep ? stored.field(0) : next.deref().field(0);
        DBSPClosureExpression step = new DBSPTupleExpression(result).closure(stored, next, nextWeight);
        return new DBSPChainAggregateKernelOperator(CalciteEmptyRel.INSTANCE, init, step, type, delta, previous);
    }

    /** Adds the changes of table T indexed by its column to 'circuit'. */
    static DBSPMapIndexOperator indexedChanges(DBSPCircuit circuit) {
        var source = source(StreamKind.DELTA);
        circuit.addOperator(source);
        var row = new DBSPVariablePath(ZSET.elementType.ref());
        DBSPClosureExpression byColumn = new DBSPRawTupleExpression(
                new DBSPTupleExpression(row.deref().field(0)),
                new DBSPTupleExpression(row.deref().field(0))).closure(row);
        var indexed = new DBSPMapIndexOperator(CalciteEmptyRel.INSTANCE, byColumn, source.outputPort());
        circuit.addOperator(indexed);
        return indexed;
    }

    /** Two expansions that read table T indexed by its column.  Each one accumulates the changes,
     * integrates them with a split integral, and runs a kernel with its own step function on
     * the changes and the integral.  If 'mirrored', the output half of the integral of the
     * second expansion precedes the one of the first expansion. */
    static DBSPCircuit twoExpansions(boolean mirrored) {
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), true);
        var indexed = indexedChanges(circuit);
        DBSPTypeIndexedZSet type = indexed.getOutputIndexedZSetType();
        List<DBSPIntegralOutputOperator> integrals = Linq.list(
                new DBSPIntegralOutputOperator(CalciteEmptyRel.INSTANCE, type),
                new DBSPIntegralOutputOperator(CalciteEmptyRel.INSTANCE, type));
        circuit.addOperator(integrals.get(mirrored ? 1 : 0));
        circuit.addOperator(integrals.get(mirrored ? 0 : 1));
        for (int i = 0; i < 2; i++) {
            DBSPIntegralOutputOperator integral = integrals.get(i);
            var accumulator = new DBSPAccumulatorOperator(CalciteEmptyRel.INSTANCE, indexed.outputPort());
            circuit.addOperator(accumulator);
            circuit.addOperator(new DBSPIntegralInputOperator(
                    CalciteEmptyRel.INSTANCE, accumulator.getOutput(0), integral));
            circuit.addOperator(kernel(i == 0, type, accumulator.getOutput(1), integral.outputPort()));
        }
        return circuit;
    }

    static <T> List<T> all(DBSPCircuit circuit, Class<T> clazz) {
        return Linq.map(Linq.where(circuit.allOperators, o -> o.is(clazz)), o -> o.to(clazz));
    }

    static <T> T single(DBSPCircuit circuit, Class<T> clazz) {
        List<T> found = all(circuit, clazz);
        Assert.assertEquals(1, found.size());
        return found.get(0);
    }

    static DBSPCircuit jsonRoundTrip(DBSPCompiler compiler, DBSPCircuit circuit) throws JsonProcessingException {
        ToJsonOuterVisitor visitor = ToJsonOuterVisitor.create(compiler, 1);
        visitor.apply(circuit);
        JsonNode parsed = Utilities.deterministicObjectMapper().readTree(visitor.getJsonString());
        JsonDecoder decoder = new JsonDecoder(compiler.sqlToRelCompiler.typeFactory);
        return decoder.decodeOuter(parsed, DBSPCircuit.class);
    }

    @Test
    public void cloneRelinksDelayInput() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPCircuit circuit = delayCircuit();
        DBSPDelayOutputOperator original = single(circuit, DBSPDelayOutputOperator.class);
        // Force a copy of every operator
        DBSPCircuit result = new CircuitCloneVisitor(compiler, true).apply(circuit);
        DBSPDelayOutputOperator output = single(result, DBSPDelayOutputOperator.class);
        Assert.assertNotSame(original, output);
        Assert.assertSame(output, single(result, DBSPDelayInputOperator.class).output);
    }

    @Test
    public void jsonKeepsDelayLink() throws JsonProcessingException {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPCircuit decoded = jsonRoundTrip(compiler, delayCircuit());
        DBSPOperator output = single(decoded, DBSPDelayOutputOperator.class);
        Assert.assertSame(output, single(decoded, DBSPDelayInputOperator.class).output);
    }

    @Test
    public void delayCarriesWaterline() {
        DBSPType scalar = new DBSPTypeTuple(DBSPTypeBool.INSTANCE);
        var output = new DBSPDelayOutputOperator(CalciteEmptyRel.INSTANCE, null, scalar, StreamKind.WATERLINE);
        var input = new DBSPDelayInputOperator(CalciteEmptyRel.INSTANCE, output.outputPort(), output);
        Assert.assertEquals(StreamKind.WATERLINE, input.outputKind(0));
    }

    /** Table T joined with itself by a join kernel.  If 'deltaOnRight', the right input is the
     * snapshot of the changes and the left input the integral before the step; else the left
     * input is the snapshot of the changes and the right input the integral after the step.
     * If 'twoDeltas', both inputs are the snapshot of the changes. */
    static DBSPCircuit joinKernelCircuit(boolean deltaOnRight, boolean twoDeltas) {
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), true);
        var indexed = indexedChanges(circuit);
        DBSPTypeIndexedZSet type = indexed.getOutputIndexedZSetType();
        var accumulator = new DBSPAccumulatorOperator(CalciteEmptyRel.INSTANCE, indexed.outputPort());
        circuit.addOperator(accumulator);
        var integral = new DBSPIntegralOperator(CalciteEmptyRel.INSTANCE, accumulator.getOutput(0));
        circuit.addOperator(integral);
        OutputPort changes = accumulator.getOutput(1);
        OutputPort collection = twoDeltas ? changes : integral.getOutput(deltaOnRight ? 0 : 1);
        var key = new DBSPVariablePath(type.keyType.ref());
        var left = new DBSPVariablePath(type.elementType.ref());
        var right = new DBSPVariablePath(type.elementType.ref());
        DBSPClosureExpression function = new DBSPTupleExpression(left.deref().field(0)).closure(key, left, right);
        circuit.addOperator(new DBSPJoinKernelOperator(CalciteEmptyRel.INSTANCE, function, ZSET,
                deltaOnRight ? collection : changes, deltaOnRight ? changes : collection));
        return circuit;
    }

    @Test
    public void joinKernelReadsChangesOnEitherSide() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        new ValidateStreamKinds(compiler).apply(joinKernelCircuit(false, false));
        new ValidateStreamKinds(compiler).apply(joinKernelCircuit(true, false));
        Assert.assertThrows(InternalCompilerError.class,
                () -> new ValidateStreamKinds(compiler).apply(joinKernelCircuit(false, true)));
    }

    /** An accumulator and an integral without consumers are dead. */
    @Test
    public void deadCodeRemovesUnusedPrimitives() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), true);
        var source = source(StreamKind.DELTA);
        circuit.addOperator(source);
        var accumulator = new DBSPAccumulatorOperator(CalciteEmptyRel.INSTANCE, source.outputPort());
        circuit.addOperator(accumulator);
        circuit.addOperator(new DBSPIntegralOperator(CalciteEmptyRel.INSTANCE, accumulator.getOutput(0)));
        DBSPCircuit result = new DeadCode(compiler, true).apply(circuit);
        Assert.assertEquals(1, result.allOperators.size());
        single(result, DBSPSourceMultisetOperator.class);
    }

    @Test
    public void integralInputRejectsSnapshot() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPCircuit circuit = accumulatorCircuit(1);
        Assert.assertThrows(InternalCompilerError.class, () -> new ValidateStreamKinds(compiler).apply(circuit));
    }

    @Test
    public void cseSharesSplitIntegrals() {
        for (boolean mirrored : new boolean[] { false, true }) {
            DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
            DBSPCircuit result = new CSE(compiler).apply(twoExpansions(mirrored));
            new ValidateStreamKinds(compiler).apply(result);
            single(result, DBSPAccumulatorOperator.class);
            DBSPIntegralOutputOperator integral = single(result, DBSPIntegralOutputOperator.class);
            Assert.assertSame(integral, single(result, DBSPIntegralInputOperator.class).output);
            List<DBSPChainAggregateKernelOperator> kernels = all(result, DBSPChainAggregateKernelOperator.class);
            Assert.assertEquals(2, kernels.size());
            for (DBSPChainAggregateKernelOperator kernel : kernels) {
                Assert.assertSame(integral, kernel.right().node());
                Assert.assertTrue(result.allOperators.indexOf(integral) < result.allOperators.indexOf(kernel));
            }
        }
    }

    @Test
    public void dataflowJsonWithAccumulator() throws JsonProcessingException {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPCircuit circuit = new CompactNames(compiler).apply(accumulatorCircuit(0));
        // The dataflow lists the persistent id of every operator
        for (DBSPOperator operator : circuit.allOperators)
            operator.addAnnotation(new OperatorHash(new HashString("h" + operator.id), true), DBSPOperator.class);
        StringBuilder builder = new StringBuilder();
        compiler.getDataflow(builder, circuit);
        JsonNode parsed = Utilities.deterministicObjectMapper().readTree(builder.toString());
        String name = single(circuit, DBSPAccumulatorOperator.class).getCompactName();
        Assert.assertEquals(DBSPAccumulatorOperator.OPERATION,
                parsed.get("mir").get(name).get("operation").asText());
    }
}
