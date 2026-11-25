package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.AlwaysMonotone;
import org.dbsp.sqlCompiler.circuit.annotation.IsProjection;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.DetectShuffle;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeProjectionVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeProjections;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.util.ExplicitShuffle;
import org.dbsp.util.Maybe;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ProjectionCompositionTests {
    private final ExpressionBuilder b = new ExpressionBuilder();
    private final DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());

    @Test public void pushProjectionPreservesRepeatedPortsAndRebuildsIndexedNoop() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT, b VARCHAR); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var input = source.getOutput(0);
        var row = input.getOutputRowType().ref().var();
        var site = CalciteEmptyRel.INSTANCE;
        var decisions = new ArrayList<Boolean>();
        compiler.stateObserver = new DBSPCompiler.StateObserver() {
            @Override public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit input) {}
            @Override public void after(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit output, boolean completed) {}
            @Override public void nativeDecision(String pass, boolean answered) {
                if (pass.equals("OptimizeProjections/PushThrough"))
                    decisions.add(answered);
            }
        };
        for (boolean indexed : List.of(false, true)) {
            DBSPSimpleOperator producer = indexed ? new DBSPNoopOperator(site, input) : new DBSPSumOperator(site, input, input);
            DBSPUnaryOperator reader = indexed
                    ? new DBSPMapIndexOperator(site, new DBSPRawTupleExpression(
                            new DBSPTupleExpression(row.deref().field(0)), row.deepCopy().deref()).closure(row), producer.outputPort())
                    : new DBSPMapOperator(site, new DBSPTupleExpression(row.deref().field(1)).closure(row), producer.outputPort());
            producer.addAnnotation(AlwaysMonotone.INSTANCE, DBSPSimpleOperator.class);
            reader.addAnnotation(new IsProjection(2), DBSPSimpleOperator.class);
            var circuit = new DBSPCircuit(compiler.metadata, false);
            circuit.addOperator(source);
            circuit.addOperator(producer);
            circuit.addOperator(reader);
            var graph = new Graph(compiler);
            graph.apply(circuit);
            DBSPExpression expected = null;
            for (boolean hybrid : List.of(false, true)) {
                compiler.options.ioOptions.rustOptimizerPasses = hybrid;
                long start = DBSPVariablePath.nextGeneratedName();
                var pass = new OptimizeProjections(compiler, false, graph.getGraphs(), new AnalyzedSet<>());
                var last = pass.apply(circuit).allOperators.getLast().to(DBSPSimpleOperator.class);
                Assert.assertEquals(indexed ? 1 : 0, DBSPVariablePath.nextGeneratedName() - start);
                Assert.assertEquals(producer.getClass(), last.getClass());
                Assert.assertSame(AlwaysMonotone.INSTANCE, last.annotations.first(AlwaysMonotone.class));
                Assert.assertEquals(producer.inputs.size(), last.inputs.size());
                for (var port : last.inputs) {
                    var copy = port.node().to(DBSPUnaryOperator.class);
                    Assert.assertSame(source, copy.input().node());
                    Assert.assertSame(reader.getFunction(), copy.getFunction());
                    Assert.assertSame(reader.outputType, copy.outputType);
                    Assert.assertEquals(2, copy.annotations.first(IsProjection.class).outputSize);
                }
                if (indexed && hybrid)
                    Assert.assertTrue(EquivalenceContext.equiv(expected, last.getFunction()));
                else if (indexed)
                    expected = last.getFunction();
                if (!indexed)
                    Assert.assertNotSame(last.inputs.get(0).node(), last.inputs.get(1).node());
            }
        }
        Assert.assertEquals(List.of(true, true), decisions);
    }

    @Test public void pushThroughPreservesANamedReader() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var input = source.getOutput(0);
        var producer = new DBSPNoopOperator(CalciteEmptyRel.INSTANCE, input);
        var row = input.getOutputRowType();
        var function = new DBSPVariablePath("read_row", new DBSPTypeFunction(row, row.ref()));
        var reader = new DBSPMapOperator(CalciteEmptyRel.INSTANCE, function,
                producer.getOutputZSetType(), producer.outputPort());
        var circuit = new DBSPCircuit(compiler.metadata, false);
        circuit.addOperator(source);
        circuit.addOperator(producer);
        circuit.addOperator(reader);
        var graph = new Graph(compiler);
        graph.apply(circuit);
        for (boolean hybrid : List.of(false, true)) {
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var pass = new OptimizeProjections(compiler, false, graph.getGraphs(), new AnalyzedSet<>());
            var noop = pass.apply(circuit).allOperators.getLast().to(DBSPNoopOperator.class);
            var pushed = noop.input().node().to(DBSPMapOperator.class);
            Assert.assertSame(source, pushed.input().node());
            Assert.assertSame(function, pushed.getFunction());
        }
    }

    @Test public void pushingThroughDelayPreservesInitializerAndTypeDiagnostic() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT, b INT); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var input = source.getOutput(0);
        var initial = DBSPZSetExpression.emptyWithElementType(input.getOutputRowType());
        var delay = new DBSPDelayOperator(CalciteEmptyRel.INSTANCE, initial, input);
        for (boolean narrow : List.of(false, true)) {
            var parameter = input.getOutputRowType().ref().var();
            var body = narrow ? new DBSPTupleExpression(parameter.deref().field(1)) : parameter.deref();
            var reader = new DBSPMapOperator(CalciteEmptyRel.INSTANCE, body.closure(parameter), delay.outputPort());
            var circuit = new DBSPCircuit(compiler.metadata, false);
            circuit.addOperator(source);
            circuit.addOperator(delay);
            circuit.addOperator(reader);
            var graph = new Graph(compiler);
            graph.apply(circuit);
            String expected = null;
            for (boolean hybrid : List.of(false, true)) {
                compiler.options.ioOptions.rustOptimizerPasses = hybrid;
                var pass = new OptimizeProjections(compiler, false, graph.getGraphs(), new AnalyzedSet<>());
                if (narrow) {
                    var error = Assert.assertThrows(InternalCompilerError.class, () -> pass.apply(circuit));
                    Assert.assertTrue(error.getMessage().startsWith("Delay input has type "));
                    if (hybrid)
                        Assert.assertEquals(expected, error.getMessage());
                    expected = error.getMessage();
                } else {
                    var rewritten = pass.apply(circuit).allOperators.getLast().to(DBSPDelayOperator.class);
                    Assert.assertSame(initial, rewritten.getFunction());
                }
            }
        }
    }

    @Test public void validateIndexedReaderOfAnUnchangedProducer() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var input = source.getOutput(0);
        var row = input.getOutputRowType().ref().var();
        var unbound = new DBSPVariablePath("missing", row.getType());
        var function = new DBSPRawTupleExpression(unbound.deref(), row.deref()).closure(row);
        var reader = new DBSPMapIndexOperator(CalciteEmptyRel.INSTANCE, function, input);
        var circuit = new DBSPCircuit(compiler.metadata, false);
        circuit.addOperator(source);
        circuit.addOperator(reader);
        var graph = new Graph(compiler);
        graph.apply(circuit);
        String expected = null;
        for (boolean hybrid : List.of(false, true)) {
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var general = new OptimizeProjections(compiler, false, graph.getGraphs(), new AnalyzedSet<>());
            Assert.assertSame(reader, general.apply(circuit).allOperators.getLast());

            var strict = new OptimizeProjections(compiler, true, graph.getGraphs(), new AnalyzedSet<>());
            var error = Assert.assertThrows(InternalCompilerError.class, () -> strict.apply(circuit));
            Assert.assertTrue(error.getMessage().startsWith("Could not resolve " + unbound));
            if (hybrid)
                Assert.assertEquals(expected, error.getMessage());
            expected = error.getMessage();
        }
    }

    @Test public void outputProjectionValidatesReadersOfAnUnchangedProducer() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var input = source.getOutput(0);
        var row = input.getOutputRowType().ref().var();
        var unbound = new DBSPVariablePath("missing", row.getType());
        var site = CalciteEmptyRel.INSTANCE;
        var rowReader = new DBSPMapOperator(site, unbound.deref().closure(row), input);
        var indexReader = new DBSPMapIndexOperator(site,
                new DBSPRawTupleExpression(unbound.deref(), row.deref()).closure(row), input);
        var wholeRow = new DBSPMapOperator(site, row.deref().closure(row), input);
        for (var reader : List.<DBSPSimpleOperator>of(rowReader, indexReader, wholeRow)) {
            var circuit = new DBSPCircuit(compiler.metadata, false);
            circuit.addOperator(source);
            circuit.addOperator(reader);
            var graph = new Graph(compiler);
            graph.apply(circuit);
            var pass = new OptimizeProjectionVisitor(compiler, graph.getGraphs());
            var expected = Assert.assertThrows(RuntimeException.class, () -> pass.apply(circuit));
            String message = expected.getMessage().lines().findFirst().orElseThrow();
            Assert.assertTrue(message, message.contains(reader == wholeRow ? "IOMap has 0 fields" : "Could not resolve"));
            var actual = Assert.assertThrows(expected.getClass(), () -> pass.applyNative(circuit));
            Assert.assertEquals(message, actual.getMessage().lines().findFirst().orElseThrow());
        }
    }

    @Test public void pushProjectionThroughAggregateMapsTheEmptyResult() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT NOT NULL, b INT NOT NULL); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var input = source.getOutput(0);
        var site = CalciteEmptyRel.INSTANCE;
        var empty = b.tuple(b.lit(17), b.lit(19));
        var zero = new DBSPAggregateZeroOperator(site, empty, input);
        zero.addAnnotation(AlwaysMonotone.INSTANCE, DBSPSimpleOperator.class);
        var row = empty.type.ref().var();
        var reader = new DBSPMapOperator(site, b.tuple(row.deref().field(1)).closure(row), zero.outputPort());
        reader.addAnnotation(new IsProjection(2), DBSPSimpleOperator.class);
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var operators = RustCircuitPass.pushProjection(compiler, reader, zero);
        Assert.assertEquals(2, operators.size());
        var mapped = operators.getFirst().to(DBSPMapOperator.class);
        Assert.assertSame(source, mapped.input().node());
        Assert.assertSame(reader.getFunction(), mapped.getFunction());
        Assert.assertEquals(2, mapped.annotations.first(IsProjection.class).outputSize);
        var result = operators.getLast().to(DBSPAggregateZeroOperator.class);
        Assert.assertSame(mapped, result.input().node());
        Assert.assertTrue(EquivalenceContext.equiv(b.tuple(b.lit(19)), result.getFunction()));
        Assert.assertNull(result.annotations.first(AlwaysMonotone.class));

        var circuit = new DBSPCircuit(compiler.metadata, false);
        circuit.addOperator(source);
        circuit.addOperator(zero);
        circuit.addOperator(reader);
        var graph = new Graph(compiler);
        graph.apply(circuit);
        for (boolean hybrid : List.of(false, true)) {
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var pass = new OptimizeProjections(compiler, true, graph.getGraphs(), new AnalyzedSet<>());
            var rewritten = pass.apply(circuit).allOperators.getLast().to(DBSPAggregateZeroOperator.class);
            Assert.assertTrue(EquivalenceContext.equiv(result.getFunction(), rewritten.getFunction()));
            Assert.assertSame(source, rewritten.input().node().to(DBSPMapOperator.class).input().node());
        }
    }

    private void checkProjection(DBSPExpression function, boolean strict, boolean nested) {
        for (boolean allowNestedAndCasts : new boolean[]{false, true}) {
            var oracle = new Projection(this.compiler, allowNestedAndCasts, allowNestedAndCasts);
            oracle.apply(function);
            Assert.assertEquals(allowNestedAndCasts ? nested : strict, oracle.isProjection);
            if (Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"))
                Assert.assertEquals(oracle.isProjection,
                        RustCircuitPass.isMapProjection(this.compiler, function, allowNestedAndCasts));
        }
    }

    @Test public void classifyProjectionsForJoinsAndSums() {
        var rowType = b.tup(b.i32());
        this.checkProjection(b.closure(rowType, row -> b.tuple(b.field(row, 0))), true, true);
        this.checkProjection(b.closure(rowType, row -> b.tuple(b.tuple(b.field(row, 0)))), false, true);
        this.checkProjection(b.closure(rowType, row -> b.tuple(b.field(row, 0)
                .nullabilityCast(b.i32n(), DBSPCastExpression.CastType.SqlUnsafe))), false, true);
        this.checkProjection(b.closure(rowType, row -> b.tuple(b.add(b.field(row, 0), b.lit(1)))), false, false);
        var indexed = new DBSPTypeRawTuple(rowType.ref(), rowType.ref());
        this.checkProjection(b.lambda(indexed, pair -> b.tuple(pair.field(1).deref().field(0))), true, true);
    }

    @Test public void classifyProjectionRootsAndResolveBindingsBeforeInspectingShape() {
        var input = b.tup(b.i32()).ref();
        var left = new DBSPVariablePath("left", input);
        var right = new DBSPVariablePath("right", input);
        this.checkProjection(b.lit(1), true, true);
        this.checkProjection(b.add(b.lit(1), b.lit(2)), false, false);
        this.checkProjection(new DBSPClosureExpression(b.tuple(b.lit(1))), false, false);
        this.checkProjection(b.tuple(left.deref().field(0), right.deref().field(0)).closure(left, right), true, true);
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        // The addition already disqualifies the projection, but resolution
        // must first report the unbound read, even in a zero-parameter closure.
        var free = b.add(b.lit(1), right.deref().field(0));
        var shared = new DBSPVariablePath(left.variable, left.getType());
        var conflict = new DBSPLetExpression(shared, shared, shared.deepCopy()).closure(left);
        // A whole row returned by dereference has no individual mapped fields.
        var whole = left.deref().closure(left);
        for (var function : List.of(free, new DBSPClosureExpression(free), conflict, whole)) {
            for (boolean nested : List.of(false, true)) {
                var oracle = new Projection(this.compiler, nested, nested);
                var expected = Assert.assertThrows(RuntimeException.class, () -> oracle.apply(function));
                var actual = Assert.assertThrows(expected.getClass(),
                        () -> RustCircuitPass.isMapProjection(this.compiler, function, nested));
                Assert.assertEquals(expected.getMessage().lines().findFirst(), actual.getMessage().lines().findFirst());
            }
        }
    }

    @Test public void trimMiddleTopKColumnAndRebindTheReader() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var key = b.tup(b.str(), b.i32());
        var row = b.tup(b.i32(), b.str(), b.i32n());
        var producer = b.closure(row, r -> b.tuple(b.field(r, 0), b.field(r, 1), b.field(r, 2)));
        var reader = b.lambda(new DBSPTypeRawTuple(key.ref(), row.ref()), pair ->
                new DBSPRawTupleExpression(b.tuple(pair.field(0).deref().field(1)),
                        b.tuple(pair.field(1).deref().field(2), pair.field(1).deref().field(0))));
        var narrowed = RustCircuitPass.trimTopKProjection(this.compiler, producer, reader);
        Assert.assertNotNull(narrowed);
        Assert.assertSame(producer.parameters[0], narrowed.left.parameters[0]);
        var expectedProducer = b.closure(row, r -> b.tuple(b.field(r, 0), b.field(r, 2)));
        var narrowRow = b.tup(b.i32(), b.i32n());
        var expectedReader = b.lambda(new DBSPTypeRawTuple(key.ref(), narrowRow.ref()), pair ->
                new DBSPRawTupleExpression(b.tuple(pair.field(0).deref().field(1)),
                        b.tuple(pair.field(1).deref().field(1), pair.field(1).deref().field(0))));
        Assert.assertTrue(EquivalenceContext.equiv(expectedProducer, narrowed.left));
        Assert.assertTrue(EquivalenceContext.equiv(expectedReader, narrowed.right));
    }

    @Test public void keepTopKWhenTheReaderUsesTheWholeValue() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var row = b.tup(b.i32(), b.i32());
        var producer = b.closure(row, r -> b.tuple(b.field(r, 0), b.field(r, 1)));
        var reader = b.lambda(new DBSPTypeRawTuple(b.tup(b.i32()).ref(), row.ref()), pair ->
                new DBSPRawTupleExpression(b.tuple(), pair.field(1).deref()));
        Assert.assertNull(RustCircuitPass.trimTopKProjection(this.compiler, producer, reader));
    }

    @Test public void narrowUnnestInputWithRepeatedPassthroughAndOrdinality() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var array = new DBSPTypeArray(b.i32n(), false);
        var input = b.tup(b.str(), b.i32(), array, b.str());
        var row = input.ref().var();
        var shuffle = new ExplicitShuffle(4, List.of(3, 0, 2, 1));
        var flatmap = new DBSPFlatmap(CalciteObject.EMPTY, input, row.deref().field(2).closure(row),
                List.of(3, 3), null, b.i32(), shuffle);
        var reduced = RustCircuitPass.trimFlatmapInput(this.compiler, flatmap);
        Assert.assertNotNull(reduced);
        var narrowed = b.tup(array, b.str());
        Assert.assertTrue(narrowed.sameType(reduced.left.getResultType()));
        Assert.assertTrue(narrowed.sameType(reduced.right.inputRowType));
        Assert.assertEquals(List.of(1, 1), reduced.right.passthroughIndexes);
        var parameter = narrowed.ref().var();
        Assert.assertTrue(EquivalenceContext.equiv(parameter.deref().field(0).closure(parameter),
                reduced.right.collectionExpression));
        Assert.assertEquals(List.of(3, 0, 2, 1), reduced.right.shuffle.shuffle(List.of(0, 1, 2, 3)));
        Assert.assertSame(flatmap.shuffle, reduced.right.shuffle);
        Assert.assertSame(flatmap.ordinalityIndexType, reduced.right.ordinalityIndexType);
        Assert.assertTrue(flatmap.getType().to(DBSPTypeFunction.class).resultType
                .sameType(reduced.right.getType().to(DBSPTypeFunction.class).resultType));
        var allColumns = new DBSPFlatmap(CalciteObject.EMPTY, input, flatmap.collectionExpression,
                List.of(0, 1, 3), null, null, new ExplicitShuffle(4, List.of(0, 1, 2, 3)));
        long nextName = DBSPVariablePath.nextGeneratedName();
        Assert.assertNull(RustCircuitPass.trimFlatmapInput(this.compiler, allColumns));
        Assert.assertEquals(nextName, DBSPVariablePath.nextGeneratedName());
    }

    @Test public void copyingUnnestKeepsItsShuffleWhileProjectingCreatesOne() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var input = b.tup(new DBSPTypeArray(b.i32(), false));
        var row = input.ref().var();
        var shuffle = new ExplicitShuffle(1, List.of(0));
        var flatmap = new DBSPFlatmap(CalciteObject.EMPTY, input, row.deref().field(0).closure(row),
                List.of(), null, null, shuffle);
        var pair = new DBSPRawTupleExpression(flatmap, flatmap);
        var oracle = pair.ensureTree(this.compiler).to(DBSPRawTupleExpression.class);
        var actual = RustCircuitPass.ensureExpressionTree(this.compiler, pair).to(DBSPRawTupleExpression.class);
        for (var tree : List.of(oracle, actual)) {
            Assert.assertNotSame(tree.fields[0], tree.fields[1]);
            Assert.assertSame(shuffle, tree.fields[0].to(DBSPFlatmap.class).shuffle);
            Assert.assertSame(shuffle, tree.fields[1].to(DBSPFlatmap.class).shuffle);
        }

        var identity = b.closure(b.tup(b.i32()), r -> b.tuple(b.field(r, 0)));
        var expected = flatmap.withShuffle(DetectShuffle.analyze(this.compiler, identity).after(shuffle));
        var projected = RustCircuitPass.projectUnnest(this.compiler, flatmap, identity);
        Assert.assertNotNull(projected);
        for (var result : List.of(expected, projected)) {
            Assert.assertEquals(List.of(0), result.shuffle.shuffle(List.of(0)));
            Assert.assertNotSame(shuffle, result.shuffle);
        }
    }

    @Test public void composeUnnestShufflesAndRejectComputedOutputs() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var input = b.tup(b.i32(), new DBSPTypeArray(b.i32(), false));
        var row = input.ref().var();
        var flatmap = new DBSPFlatmap(CalciteObject.EMPTY, input, row.deref().field(1).closure(row),
                List.of(0), null, null, new ExplicitShuffle(2, List.of(1, 0)));
        var output = b.tup(b.i32(), b.i32());
        var repeat = b.closure(output, r -> b.tuple(b.field(r, 0), b.field(r, 0)));
        var expected = flatmap.withShuffle(DetectShuffle.analyze(this.compiler, repeat).after(flatmap.shuffle));
        var actual = RustCircuitPass.projectUnnest(this.compiler, flatmap, repeat);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.passthroughIndexes, actual.passthroughIndexes);
        Assert.assertEquals(List.of(0, 0), actual.shuffle.shuffle(List.of(0)));
        Assert.assertTrue(expected.type.sameType(actual.type));
        var compute = b.closure(output, r -> b.tuple(b.add(b.field(r, 0), b.lit(1))));
        Assert.assertNull(RustCircuitPass.projectUnnest(this.compiler, flatmap, compute));
    }

    @Test public void reduceTheProjectedEmptyAggregateAndPreserveIdentityNameAllocation() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var zero = b.tuple(b.lit(17), b.lit(19));
        var swap = b.closure(zero.type, r -> b.tuple(b.field(r, 1), b.field(r, 0)));
        var expected = swap.call(zero.borrow()).reduce(this.compiler);
        var actual = RustCircuitPass.projectAggregateZero(this.compiler, zero, swap);
        Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
        var identity = b.closure(zero.type, r -> b.tuple(b.field(r, 0), b.field(r, 1)));
        long next = DBSPVariablePath.nextGeneratedName();
        Assert.assertTrue(RustCircuitPass.isIdentityProjection(this.compiler, identity));
        Assert.assertFalse(RustCircuitPass.isIdentityProjection(this.compiler, swap));
        Assert.assertEquals(next + 2, DBSPVariablePath.nextGeneratedName());
    }

    @Test public void fillDroppedAggregateColumnsWithTheirOriginalEmptySetResults() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var row = b.i32().var();
        var map = row.closure(row);
        var first = new LinearAggregate(CalciteObject.EMPTY, map, map, b.lit(7));
        var second = new LinearAggregate(CalciteObject.EMPTY, map, map, b.lit(11));
        var list = new DBSPAggregateList(CalciteObject.EMPTY, row, List.of(first, second));
        var output = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, b.tup(b.str(), b.i32()), list.getEmptySetResultType());
        var all = b.lambda(output.getKVRefType(), pair -> pair.field(1).deref());
        long next = DBSPVariablePath.nextGeneratedName();
        Assert.assertNull(RustCircuitPass.trimAggregateProjection(this.compiler, list, output, all));
        Assert.assertEquals(next, DBSPVariablePath.nextGeneratedName());

        for (boolean keepSecond : new boolean[]{true, false}) {
            var reader = b.lambda(output.getKVRefType(), pair -> keepSecond ? pair.field(1).deref().field(1) : b.tuple());
            next = DBSPVariablePath.nextGeneratedName();
            var projected = RustCircuitPass.trimAggregateProjection(this.compiler, list, output, reader);
            Assert.assertNotNull(projected);
            Assert.assertEquals(next + 1, DBSPVariablePath.nextGeneratedName());
            Assert.assertEquals(keepSecond ? List.of(second) : List.of(), projected.left.aggregates);
            var filler = projected.right;
            var input = filler.parameters[0].type.to(DBSPTypeRawTuple.class);
            Assert.assertSame(output.keyType, input.tupFields[0].deref());
            Assert.assertSame(projected.left.getEmptySetResultType(), input.tupFields[1].deref());
            var values = filler.body.to(DBSPRawTupleExpression.class).fields[1].to(DBSPTupleExpression.class);
            Assert.assertSame(first.emptySetResult, values.fields[0]);
            if (keepSecond) {
                var expected = filler.parameters[0].asVariable().field(1).deref().field(0);
                Assert.assertTrue(EquivalenceContext.equiv(expected.closure(filler.parameters),
                        values.fields[1].closure(filler.parameters)));
            } else {
                Assert.assertSame(second.emptySetResult, values.fields[1]);
            }
        }
    }

    @Test public void projectTheResultOfABlockAndKeepItsBindings() {
        var before = b.closure(b.tup(b.i32()), row ->
                b.block(b.call("read_value", b.field(row, 0)), v -> b.tuple(v, b.lit(0))));
        var after = b.closure(before.body.getType(), row -> b.field(row, 0));
        var expected = after.applyAfter(this.compiler, before, Maybe.YES);
        Assert.assertEquals(1, expected.body.to(DBSPBlockExpression.class).contents.size());
        if (Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields")) {
            var actual = RustCircuitPass.composeProjection(this.compiler, before, after);
            Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            Assert.assertSame(before.parameters[0], actual.parameters[0]);
        }
    }

    @Test public void keepAProducerWhenTheProjectionDuplicatesAnExpensiveField() {
        var before = b.closure(b.tup(b.i32()), row -> b.tuple(b.call("read_value", b.field(row, 0))));
        var after = b.closure(before.body.getType(), row -> b.tuple(b.field(row, 0), b.field(row, 0)));
        Assert.assertFalse(after.shouldInlineComposition(this.compiler, before));
        if (Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"))
            Assert.assertNull(RustCircuitPass.tryComposeProjection(this.compiler, before, after));
    }

    @Test public void deriveTheFieldTypeAfterRemovingACloneOfANullableRow() {
        var record = b.tup(b.i32(), b.str()).withMayBeNull(true);
        var before = b.closure(b.tup(record), row -> b.tuple(b.field(row, 0).applyCloneIfNeeded().field(0)));
        var after = b.closure(before.body.type, row -> b.field(row, 0));
        var expected = after.applyAfter(this.compiler, before, Maybe.YES);
        Assert.assertTrue(expected.body.type.mayBeNull);
        Assert.assertNotSame(before.body.field(0).type, expected.body.type);
        if (Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields")) {
            var actual = RustCircuitPass.composeProjection(this.compiler, before, after);
            Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            Assert.assertNotSame(before.body.field(0).type, actual.body.type);
        }
    }

    @Test public void reduceAProducerCallBeforeSubstitutingItIntoAProjection() {
        var rowType = b.tup(b.i32(), b.i32());
        var swap = b.closure(rowType, row -> b.tuple(b.field(row, 1), b.field(row, 0)));
        var before = b.closure(rowType, swap::call);
        var after = b.closure(before.body.type, row -> b.tuple(b.field(row, 1), b.field(row, 0)));
        var expected = after.applyAfter(this.compiler, before, Maybe.YES);
        var input = before.parameters[0].asVariable();
        var identity = b.tuple(b.field(input, 0), b.field(input, 1)).closure(before.parameters);
        Assert.assertTrue(EquivalenceContext.equiv(identity, expected));
        if (Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields")) {
            var actual = RustCircuitPass.composeProjection(this.compiler, before, after);
            Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
        }
    }

    @Test public void inlineIndexedTupleWithBorrowedKeyAndValue() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var rowType = b.tup(b.i32());
        var before = b.closure(rowType, row -> new DBSPRawTupleExpression(
                b.tuple(b.call("read_key", b.field(row, 0))), b.tuple(b.field(row, 0))));
        var after = b.lambda(new DBSPTypeRawTuple(rowType.ref(), rowType.ref()), pair ->
                new DBSPRawTupleExpression(pair.field(1).deref(), pair.field(0).deref()));
        var borrowed = new DBSPRawTupleExpression(before.body.field(0).borrow(), before.body.field(1).borrow());
        var expected = after.call(borrowed).reduce(this.compiler).closure(before.parameters);
        long next = DBSPVariablePath.nextGeneratedName();
        for (boolean onlyProjections : new boolean[]{true, false}) {
            var actual = RustCircuitPass.composeIndexedProjection(this.compiler, before, after, onlyProjections);
            Assert.assertNotNull(actual);
            Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            Assert.assertSame(before.parameters[0], actual.parameters[0]);
            Assert.assertEquals(next, DBSPVariablePath.nextGeneratedName());
        }
    }

    @Test public void bindAnExpensiveIndexedProducerOnlyOnce() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var rowType = b.tup(b.i32());
        var pairType = new DBSPTypeRawTuple(rowType, rowType);
        var before = b.closure(rowType, row -> b.call(pairType, "read_pair", b.field(row, 0)));
        var after = b.lambda(new DBSPTypeRawTuple(rowType.ref(), rowType.ref()), pair ->
                new DBSPRawTupleExpression(pair.field(1).deref(), pair.field(0).deref()));
        long next = DBSPVariablePath.nextGeneratedName();
        Assert.assertNull(RustCircuitPass.composeIndexedProjection(this.compiler, before, after, true));
        Assert.assertEquals(next, DBSPVariablePath.nextGeneratedName());
        var actual = RustCircuitPass.composeIndexedProjection(this.compiler, before, after, false);
        Assert.assertNotNull(actual);
        Assert.assertEquals(next + 1, DBSPVariablePath.nextGeneratedName());
        Assert.assertSame(before.parameters[0], actual.parameters[0]);
        var outer = actual.body.to(DBSPLetExpression.class);
        Assert.assertSame(before.body, outer.initializer);
        Assert.assertSame(pairType, outer.variable.type);
        var inner = outer.consumer.to(DBSPLetExpression.class);
        Assert.assertSame(after.body, inner.consumer);
        Assert.assertSame(after.parameters[0].type, inner.variable.type);
        var borrowed = new DBSPRawTupleExpression(outer.variable.field(0).borrow(), outer.variable.field(1).borrow());
        Assert.assertTrue(EquivalenceContext.equiv(borrowed, inner.initializer));
    }

    @Test public void retainCompositeKeysOnBothAntijoinInputs() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var key = b.tup(b.tup(b.strn()), b.i32());
        var input = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, key, b.tup(b.str(), b.i32n()));
        var projection = b.lambda(input.getKVRefType(), pair -> b.tuple(pair.field(1).deref().field(1)));
        var pair = input.getKVRefType().var();
        var borrowedKey = pair.field(0).deref();
        var left = new DBSPRawTupleExpression(ExpressionCompiler.expandTuple(projection.getNode(), borrowedKey),
                projection.call(pair).reduce(this.compiler)).closure(pair);
        var right = new DBSPRawTupleExpression(
                new DBSPTupleExpression(DBSPTypeTupleBase.flatten(borrowedKey), false),
                new DBSPTupleExpression()).closure(pair);
        long next = DBSPVariablePath.nextGeneratedName();
        var actualLeft = RustCircuitPass.antijoinProjection(this.compiler, input, projection);
        var actualRight = RustCircuitPass.antijoinProjection(this.compiler, input, null);
        Assert.assertTrue(EquivalenceContext.equiv(left, actualLeft));
        Assert.assertTrue(EquivalenceContext.equiv(right, actualRight));
        Assert.assertSame(key, actualLeft.body.field(0).type);
        Assert.assertEquals(next + 2, DBSPVariablePath.nextGeneratedName());
    }

    @Test public void splitAnIndexedProjectionAtTheAntijoin() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var key = b.tup(b.i32(), b.str());
        var value = b.tup(b.i32(), b.i32n());
        var input = new DBSPTypeRawTuple(key.ref(), value.ref());
        var projection = b.lambda(input, pair -> new DBSPRawTupleExpression(
                b.tuple(pair.field(0).deref().field(1).applyCloneIfNeeded()),
                b.tuple(pair.field(1).deref().field(0))));
        long next = DBSPVariablePath.nextGeneratedName();
        var split = RustCircuitPass.splitAntijoinProjection(this.compiler, projection);
        Assert.assertEquals(next, DBSPVariablePath.nextGeneratedName());
        Assert.assertSame(projection.parameters[0], split.left.parameters[0]);
        Assert.assertSame(input.tupFields[0], split.right.parameters[0].type.to(DBSPTypeRawTuple.class).tupFields[0]);
        Assert.assertTrue(key.sameType(split.left.body.field(0).type));
        Assert.assertTrue(projection.body.type.sameType(split.right.body.type));
        var argument = new DBSPRawTupleExpression(split.left.body.field(0).borrow(), split.left.body.field(1).borrow());
        var composed = split.right.call(argument).reduce(this.compiler).closure(split.left.parameters);
        Assert.assertTrue(EquivalenceContext.equiv(projection.reduce(this.compiler), composed));

        var invalid = b.lambda(input, pair -> new DBSPRawTupleExpression(
                b.tuple(pair.field(1).deref().field(0)), b.tuple()));
        var error = Assert.assertThrows(RuntimeException.class,
                () -> RustCircuitPass.splitAntijoinProjection(this.compiler, invalid));
        Assert.assertTrue(error.getMessage().contains("key projection reads the input value"));
    }
}
