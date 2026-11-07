package org.dbsp.sqlCompiler.compiler.visitors.outer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.CompactName;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.annotation.RegionAnnotation;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
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
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPIntegralOutputOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPJoinKernelOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.ViewMetadata;
import org.dbsp.sqlCompiler.compiler.backend.Gen2JsonVisitor;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Tests for the operators in the primitives package, and for {@link Gen2Lowering}, which
 * lowers operators to them. */
public class PrimitivesTests extends SqlIoTest {
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

    /** The init function of a chain aggregate over rows of one boolean: the value of the row. */
    static DBSPClosureExpression chainInit(DBSPTypeIndexedZSet type) {
        var row = new DBSPVariablePath(type.elementType.ref());
        var weight = new DBSPVariablePath(DBSPTypeWeight.INSTANCE);
        return new DBSPTupleExpression(row.deref().field(0)).closure(row, weight);
    }

    /** The step function of a chain aggregate over rows of one boolean: the stored value if
     * 'keep', else the new row. */
    static DBSPClosureExpression chainStep(boolean keep, DBSPTypeIndexedZSet type) {
        var stored = new DBSPVariablePath(type.elementType);
        var next = new DBSPVariablePath(type.elementType.ref());
        var nextWeight = new DBSPVariablePath(DBSPTypeWeight.INSTANCE);
        DBSPExpression result = keep ? stored.field(0) : next.deref().field(0);
        return new DBSPTupleExpression(result).closure(stored, next, nextWeight);
    }

    /** A chain aggregate kernel over rows of one boolean, with the functions above. */
    static DBSPChainAggregateKernelOperator kernel(
            boolean keep, DBSPTypeIndexedZSet type, OutputPort delta, OutputPort previous) {
        return new DBSPChainAggregateKernelOperator(
                CalciteEmptyRel.INSTANCE, chainInit(type), chainStep(keep, type), type, delta, previous);
    }

    /** Adds the changes of table T indexed by its column to 'circuit'. */
    static DBSPMapIndexOperator indexedChanges(DBSPCircuit circuit) {
        return indexedTable(circuit, StreamKind.DELTA);
    }

    /** Adds table T, whose stream kind is 'kind', indexed by its column to 'circuit'. */
    static DBSPMapIndexOperator indexedTable(DBSPCircuit circuit, StreamKind kind) {
        var source = source(kind);
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

    /** Two split delays of table T, each read by its own view.  If 'mirrored', the output half
     * of the second delay precedes the one of the first delay. */
    static DBSPCircuit twoDelays(boolean mirrored) {
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var source = source(StreamKind.COLLECTION);
        circuit.addOperator(source);
        List<DBSPDelayOutputOperator> outputs = Linq.list(
                new DBSPDelayOutputOperator(CalciteEmptyRel.INSTANCE, null, ZSET, StreamKind.COLLECTION),
                new DBSPDelayOutputOperator(CalciteEmptyRel.INSTANCE, null, ZSET, StreamKind.COLLECTION));
        circuit.addOperator(outputs.get(mirrored ? 1 : 0));
        circuit.addOperator(outputs.get(mirrored ? 0 : 1));
        for (int i = 0; i < 2; i++) {
            DBSPDelayOutputOperator output = outputs.get(i);
            circuit.addOperator(new DBSPDelayInputOperator(CalciteEmptyRel.INSTANCE, source.outputPort(), output));
            ProgramIdentifier view = new ProgramIdentifier("V" + i);
            ViewMetadata viewMeta = new ViewMetadata(view, new ArrayList<>(), SqlCreateView.ViewKind.STANDARD,
                    -1, false, false, null);
            circuit.addOperator(new DBSPSinkOperator(
                    CalciteEmptyRel.INSTANCE, view, "V" + i, STRUCT, viewMeta, output.outputPort()));
        }
        return circuit;
    }

    /** A chain aggregate of table T indexed by its column; the stream kind of T is 'kind'. */
    static DBSPCircuit chainAggregateCircuit(StreamKind kind) {
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), kind == StreamKind.DELTA);
        var indexed = indexedTable(circuit, kind);
        DBSPTypeIndexedZSet type = indexed.getOutputIndexedZSetType();
        circuit.addOperator(new DBSPChainAggregateOperator(CalciteEmptyRel.INSTANCE,
                chainInit(type), chainStep(false, type), type, indexed.outputPort()));
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

    /** CSE merges two delays of the same stream: the input halves, then their output halves,
     * which both views then read. */
    @Test
    public void cseSharesSplitDelays() {
        for (boolean mirrored : new boolean[] { false, true }) {
            DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
            DBSPCircuit result = new CSE(compiler).apply(twoDelays(mirrored));
            new ValidateStreamKinds(compiler).apply(result);
            DBSPDelayOutputOperator output = single(result, DBSPDelayOutputOperator.class);
            Assert.assertSame(output, single(result, DBSPDelayInputOperator.class).output);
            List<DBSPSinkOperator> sinks = all(result, DBSPSinkOperator.class);
            Assert.assertEquals(2, sinks.size());
            for (DBSPSinkOperator sink : sinks)
                Assert.assertSame(output, sink.input().node());
        }
    }

    /** The lowering replaces a chain aggregate of a stream of changes, and keeps one of a
     * collection. */
    @Test
    public void chainAggregateOfCollectionIsNotLowered() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPCircuit lowered = new LowerToPrimitives(compiler).apply(chainAggregateCircuit(StreamKind.DELTA));
        Assert.assertTrue(all(lowered, DBSPChainAggregateOperator.class).isEmpty());
        single(lowered, DBSPChainAggregateKernelOperator.class);
        DBSPCircuit kept = new LowerToPrimitives(compiler).apply(chainAggregateCircuit(StreamKind.COLLECTION));
        single(kept, DBSPChainAggregateOperator.class);
        Assert.assertTrue(all(kept, DBSPChainAggregateKernelOperator.class).isEmpty());
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

    DBSPCompiler gen2Compiler(String sql) {
        CompilerOptions options = this.testOptions();
        options.ioOptions.gen2 = true;
        options.languageOptions.incrementalize = true;
        options.ioOptions.checkSerialization = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(sql);
        return compiler;
    }

    static DBSPCircuit finalCircuit(DBSPCompiler compiler) {
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        return circuit;
    }

    DBSPCircuit gen2Circuit(String sql) {
        return finalCircuit(this.gen2Compiler(sql));
    }

    static String hash(DBSPOperator operator) {
        return Objects.requireNonNull(OperatorHash.getHash(operator, true)).toString();
    }

    /** The JSON for the Gen-2 engine writes each split integral as one integral node, in the
     * place of its output half: it reads the spine of the output accumulator, which comes later,
     * and the kernel reads the integral before the step. */
    @Test
    public void gen2JsonFusesSplitIntegrals() throws JsonProcessingException {
        DBSPCompiler compiler = this.gen2Compiler("""
                CREATE TABLE t(k INT, v INT) WITH ('append_only' = 'true');
                CREATE VIEW mn AS SELECT k, MIN(v) FROM t GROUP BY k;
                CREATE VIEW mx AS SELECT k, MAX(v) FROM t GROUP BY k;""");
        DBSPCircuit circuit = finalCircuit(compiler);
        Gen2JsonVisitor visitor = Gen2JsonVisitor.create(compiler, 1);
        visitor.apply(circuit);
        JsonNode json = Utilities.deterministicObjectMapper().readTree(visitor.getJsonString());
        Map<Long, String> classes = new HashMap<>();
        Map<Long, Integer> positions = new HashMap<>();
        int position = 0;
        for (JsonNode operator: json.get("allOperators")) {
            classes.put(operator.get("id").asLong(), operator.get("class").asText());
            positions.put(operator.get("id").asLong(), position++);
        }
        Assert.assertFalse(classes.containsValue(DBSPIntegralInputOperator.class.getSimpleName()));
        Assert.assertFalse(classes.containsValue(DBSPIntegralOutputOperator.class.getSimpleName()));
        Set<Long> integrals = new HashSet<>();
        for (JsonNode operator: json.get("allOperators")) {
            if (!operator.get("class").asText().equals(DBSPIntegralOperator.class.getSimpleName()))
                continue;
            long id = operator.get("id").asLong();
            integrals.add(id);
            JsonNode input = operator.get("inputs").get(0);
            long spine = input.get("operator").get("node").asLong();
            Assert.assertEquals(DBSPAccumulatorOperator.class.getSimpleName(), classes.get(spine));
            Assert.assertEquals(0, input.get("outputNumber").asInt());
            // The back edge: the accumulator comes after the integral
            Assert.assertTrue(positions.get(spine) > positions.get(id));
        }
        // The output halves keep their ids
        Assert.assertEquals(Set.copyOf(Linq.map(all(circuit, DBSPIntegralOutputOperator.class),
                DBSPOperator::getId)), integrals);
        for (JsonNode operator: json.get("allOperators")) {
            if (!operator.get("class").asText().equals(DBSPChainAggregateKernelOperator.class.getSimpleName()))
                continue;
            JsonNode previous = operator.get("inputs").get(1);
            Assert.assertTrue(integrals.contains(previous.get("operator").get("node").asLong()));
            Assert.assertEquals(0, previous.get("outputNumber").asInt());
        }
    }

    /** Two chain aggregates of one append-only table share the accumulator of their input. */
    @Test
    public void chainAggregatesShareInput() {
        DBSPCircuit circuit = this.gen2Circuit("""
                CREATE TABLE t(k INT, v INT) WITH ('append_only' = 'true');
                CREATE VIEW mn AS SELECT k, MIN(v) FROM t GROUP BY k;
                CREATE VIEW mx AS SELECT k, MAX(v) FROM t GROUP BY k;""");
        Assert.assertTrue(all(circuit, DBSPChainAggregateOperator.class).isEmpty());
        List<DBSPChainAggregateKernelOperator> kernels = all(circuit, DBSPChainAggregateKernelOperator.class);
        Assert.assertEquals(2, kernels.size());
        // One accumulator of the input and one of each output
        Assert.assertEquals(3, all(circuit, DBSPAccumulatorOperator.class).size());
        Assert.assertSame(kernels.get(0).left().node(), kernels.get(1).left().node());
        Assert.assertEquals(2, all(circuit, DBSPIntegralOutputOperator.class).size());
        for (DBSPIntegralInputOperator input : all(circuit, DBSPIntegralInputOperator.class))
            Assert.assertEquals(hash(input.output), hash(input));
        // As in dbsp, the integral of the output is named after the chain aggregate
        for (DBSPChainAggregateKernelOperator kernel : kernels)
            Assert.assertEquals(hash(kernel) + ".accintegral", hash(kernel.right().node()));
        Set<String> names = new HashSet<>();
        for (DBSPOperator operator : circuit.allOperators) {
            Assert.assertEquals(1, operator.annotations.get(CompactName.class).size());
            Assert.assertTrue(names.add(Objects.requireNonNull(CompactName.getCompactName(operator))));
        }
    }

    static void assertReads(OutputPort input, Class<? extends DBSPOperator> clazz, int port) {
        Assert.assertTrue(input.node().is(clazz));
        Assert.assertEquals(port, input.port());
    }

    /** Two joins that read the index of table a. */
    static final String JOINS = """
                CREATE TABLE a(k INT, x INT);
                CREATE TABLE b(k INT, y INT);
                CREATE TABLE c(k INT, z INT);
                CREATE VIEW ab AS SELECT a.k, x, y FROM a JOIN b ON a.k = b.k;
                CREATE VIEW ac AS SELECT a.k, x, z FROM a JOIN c ON a.k = c.k;""";

    /** Both joins read the index of table a, so CSE shares the accumulator and the integral of
     * that index: each table has one accumulator and one integral. */
    @Test
    public void joinsShareIntegrals() {
        DBSPCircuit circuit = this.gen2Circuit(JOINS);
        Assert.assertTrue(all(circuit, DBSPJoinOperator.class).isEmpty());
        Assert.assertEquals(3, all(circuit, DBSPAccumulatorOperator.class).size());
        Assert.assertEquals(3, all(circuit, DBSPIntegralOperator.class).size());
        List<DBSPJoinKernelOperator> kernels = all(circuit, DBSPJoinKernelOperator.class);
        Assert.assertEquals(4, kernels.size());
        for (DBSPJoinKernelOperator kernel : kernels) {
            if (hash(kernel).endsWith(".dl-r")) {
                // The changes of the left side, the right side after the step
                assertReads(kernel.left(), DBSPAccumulatorOperator.class, 1);
                assertReads(kernel.right(), DBSPIntegralOperator.class, 1);
            } else {
                // The left side before the step, the changes of the right side
                Assert.assertTrue(hash(kernel).endsWith(".l-dr"));
                assertReads(kernel.left(), DBSPIntegralOperator.class, 0);
                assertReads(kernel.right(), DBSPAccumulatorOperator.class, 1);
            }
        }
    }

    /** A self-join reads one stream on both sides.  The two accumulators and the two integrals
     * have the same persistent ids, and CSE merges them. */
    @Test
    public void selfJoinSharesAccumulatorAndIntegral() {
        DBSPCircuit circuit = this.gen2Circuit("""
                CREATE TABLE a(k INT, x INT);
                CREATE VIEW aa AS SELECT a1.k, a1.x, a2.x AS y FROM a AS a1 JOIN a AS a2 ON a1.k = a2.k;""");
        DBSPAccumulatorOperator accumulator = single(circuit, DBSPAccumulatorOperator.class);
        DBSPIntegralOperator integral = single(circuit, DBSPIntegralOperator.class);
        List<DBSPJoinKernelOperator> kernels = all(circuit, DBSPJoinKernelOperator.class);
        Assert.assertEquals(2, kernels.size());
        for (DBSPJoinKernelOperator kernel : kernels)
            for (OutputPort input : kernel.inputs)
                Assert.assertTrue(input.node() == accumulator || input.node() == integral);
    }

    static RegionAnnotation region(DBSPOperator operator) {
        List<RegionAnnotation> regions = operator.annotations.get(RegionAnnotation.class);
        Assert.assertEquals(1, regions.size());
        return regions.get(0);
    }

    /** The primitives that replace a join form a region tagged with the operation of the join.
     * An accumulator or an integral that CSE shares keeps the region of one of the joins. */
    @Test
    public void joinPrimitivesFormRegions() {
        DBSPCircuit circuit = this.gen2Circuit(JOINS);
        List<DBSPSumOperator> sums = all(circuit, DBSPSumOperator.class);
        Assert.assertEquals(2, sums.size());
        Set<RegionAnnotation> regions = new HashSet<>();
        for (DBSPSumOperator sum : sums) {
            RegionAnnotation region = region(sum);
            Assert.assertTrue(region.getTag().startsWith("join"));
            Assert.assertTrue(regions.add(region));
            for (OutputPort input : sum.inputs)
                Assert.assertEquals(region, region(input.node()));
        }
        for (DBSPOperator operator : circuit.allOperators)
            if (operator.is(DBSPAccumulatorOperator.class) || operator.is(DBSPIntegralOperator.class))
                Assert.assertTrue(regions.contains(region(operator)));
    }

    /** The persistent ids of all operators of {@code circuit}, the nested ones included. */
    static Set<String> persistentIds(DBSPCircuit circuit) {
        Set<String> result = new HashSet<>();
        for (DBSPOperator operator : circuit.allOperators) {
            result.add(hash(operator));
            if (operator.is(DBSPNestedOperator.class))
                for (DBSPOperator inner : operator.to(DBSPNestedOperator.class).getAllOperators())
                    result.add(hash(inner));
        }
        return result;
    }

    /** Records the persistent ids of the circuit that it receives. */
    static class RecordIds implements CircuitTransform {
        final Set<String> ids = new HashSet<>();

        @Override
        public DBSPCircuit apply(DBSPCircuit circuit) {
            this.ids.addAll(persistentIds(circuit));
            return circuit;
        }

        @Override
        public String getName() {
            return "RecordIds";
        }
    }

    /** The lowering keeps every persistent id of the circuit: as in dbsp, the sum of a join and
     * the kernel of a chain aggregate have the id of the operator that they replace. */
    @Test
    public void loweringKeepsPersistentIds() {
        DBSPCompiler compiler = this.gen2Compiler(JOINS + """
                CREATE TABLE t(k INT, v INT) WITH ('append_only' = 'true');
                CREATE VIEW mn AS SELECT k, MIN(v) FROM t GROUP BY k;""");
        RecordIds beforeLowering = new RecordIds();
        compiler.optimizerHook = optimizer -> optimizer.insertAfter(CircuitStatistics.class, beforeLowering);
        Set<String> afterLowering = persistentIds(finalCircuit(compiler));
        Assert.assertFalse(beforeLowering.ids.isEmpty());
        Assert.assertTrue(afterLowering.containsAll(beforeLowering.ids));
    }

    static Set<String> integralIds(DBSPCircuit circuit) {
        return new HashSet<>(Linq.map(all(circuit, DBSPIntegralOperator.class), PrimitivesTests::hash));
    }

    /** As in dbsp, the id of the integral of a stream does not depend on the joins that read
     * the stream: removing view ab keeps the ids of the integrals of view ac. */
    @Test
    public void sharedIntegralIdStableWhenViewRemoved() {
        String tables = """
                CREATE TABLE a(k INT, x INT);
                CREATE TABLE b(k INT, y INT);
                CREATE TABLE c(k INT, z INT);""";
        String ab = "CREATE VIEW ab AS SELECT a.k, x, y FROM a JOIN b ON a.k = b.k;";
        String ac = "CREATE VIEW ac AS SELECT a.k, x, z FROM a JOIN c ON a.k = c.k;";
        Set<String> both = integralIds(this.gen2Circuit(tables + ab + ac));
        Set<String> acOnly = integralIds(this.gen2Circuit(tables + ac));
        Assert.assertEquals(2, acOnly.size());
        Assert.assertTrue(both.containsAll(acOnly));
    }

    /** The join of a recursive view becomes primitives inside the nested circuit. */
    @Test
    public void lowerJoinInRecursiveCircuit() {
        DBSPCircuit circuit = this.gen2Circuit("""
                CREATE TABLE edges(src INT, dst INT);
                DECLARE RECURSIVE VIEW reach(src INT, dst INT);
                CREATE VIEW reach AS SELECT * FROM edges UNION
                SELECT reach.src, edges.dst FROM reach JOIN edges ON reach.dst = edges.src;""");
        List<DBSPNestedOperator> nested = all(circuit, DBSPNestedOperator.class);
        Assert.assertEquals(1, nested.size());
        List<DBSPOperator> inside = Linq.list(nested.get(0).getAllOperators());
        Assert.assertTrue(Linq.where(inside, o -> o.is(DBSPJoinBaseOperator.class)).isEmpty());
        Assert.assertEquals(2, Linq.where(inside, o -> o.is(DBSPJoinKernelOperator.class)).size());
        Assert.assertEquals(2, Linq.where(inside, o -> o.is(DBSPAccumulatorOperator.class)).size());
        Assert.assertEquals(2, Linq.where(inside, o -> o.is(DBSPIntegralOperator.class)).size());
    }

    /** Dead-code elimination keeps the input half of the output integral of a chain aggregate,
     * which has no consumers, and the accumulator that feeds it. */
    @Test
    public void deadCodeKeepsInputHalves() {
        DBSPCircuit circuit = this.gen2Circuit("""
                CREATE TABLE t(k INT, v INT) WITH ('append_only' = 'true');
                CREATE VIEW mn AS SELECT k, MIN(v) FROM t GROUP BY k;""");
        DBSPCircuit result = new DeadCode(new DBSPCompiler(this.testOptions()), false).apply(circuit);
        Assert.assertEquals(1, all(result, DBSPIntegralInputOperator.class).size());
        Assert.assertEquals(2, all(result, DBSPAccumulatorOperator.class).size());
    }
}
