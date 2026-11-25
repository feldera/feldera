package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEnumValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPEqualityComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPForExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPQualifyTypeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPReturnExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSortExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPComparatorItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.IdShuffle;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class JoinInputProjectionTests {
    @Test
    public void nestedReaderUsesOriginalOffsetsAtEachProjectionDepth() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(false);
        var values = new DBSPTypeTuple(text, text, text);
        var row = new DBSPTypeTuple(text, values, values).ref().var();
        var pair = new DBSPTypeRawTuple(values.ref(), values.ref()).var();
        var body = new DBSPTupleExpression(row.deref().field(1).field(2), row.deepCopy().deref().field(2).field(0),
                pair.field(0).deref().field(1), pair.deepCopy().field(1).deref().field(2));
        var closure = body.closure(row, pair);
        var fields = new FindUsedFields(compiler).findUsedFields(closure);
        var replies = new java.util.ArrayList<String>();
        compiler.stateObserver = new DBSPCompiler.StateObserver() {
            @Override public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit input) {}
            @Override public void after(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit output, boolean completed) {}
            @Override public void nativeDecision(String operation, boolean success) {
                if (success)
                    replies.add(operation);
            }
        };
        for (int depth : List.of(1, 2, 3)) {
            compiler.options.ioOptions.rustOptimizerPasses = false;
            var rewriter = fields.getFieldRewriter(compiler, depth);
            var expected = rewriter.rewriteClosure(closure);
            compiler.options.ioOptions.rustOptimizerPasses = true;
            var actual = rewriter.rewriteClosure(closure);
            Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            Assert.assertArrayEquals(expected.parameters, actual.parameters);
            Assert.assertEquals(List.of(depth == 1 ? "RemoveUnusedFields/Reader" : "RemoveUnusedFields/NestedReader"), replies);
            replies.clear();
        }
    }

    @Test
    public void batchedReadersPreserveErrorsInDiscardedExpressions() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT, b INT); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var row = source.getOutput(0).getOutputRowType().ref().var();
        var first = row.deref().field(0);
        var second = row.deepCopy().deref().field(1);
        var free = new DBSPVariablePath("missing", first.getType());
        for (var discarded : List.of(new DBSPAssignmentExpression(first, second), free.closure())) {
            var body = new DBSPBlockExpression(List.of(discarded.toStatement()),
                    new DBSPTupleExpression(second.deepCopy()));
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            circuit.addOperator(source);
            circuit.addOperator(new DBSPMapOperator(CalciteEmptyRel.INSTANCE, body.closure(row), source.getOutput(0)));
            var graph = new Graph(compiler);
            graph.apply(circuit);
            for (boolean trimSource : List.of(false, true)) {
                String expected = null;
                for (boolean hybrid : List.of(false, true)) {
                    compiler.options.ioOptions.rustOptimizerPasses = hybrid;
                    CircuitTransform pass;
                    if (trimSource) {
                        var analysis = new UnusedFields.FindUnusedInputFields(compiler, graph.getGraphs());
                        pass = hybrid ? new RustCircuitPass(compiler, analysis, new OptimizerSnapshots(compiler)) : analysis;
                    } else {
                        pass = new RemoveUnusedFields(compiler, new AnalyzedSet<>());
                    }
                    var failure = Assert.assertThrows(InternalCompilerError.class, () -> pass.apply(circuit));
                    if (hybrid)
                        Assert.assertEquals(expected, failure.getMessage());
                    else
                        expected = failure.getMessage();
                }
            }
        }
    }

    @Test
    public void sharedReadsResolveInTheScopeOfEachCopiedOccurrence() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(false);
        var type = new DBSPTypeTuple(text, text).ref();
        var outer = new DBSPParameter("row", type);
        var inner = new DBSPParameter("row", type);
        var shared = outer.asVariable();
        var nested = shared.deref().field(0).closure(inner);
        var body = new DBSPBlockExpression(List.of(nested.toStatement()), shared.deref().field(1));
        var reader = body.closure(outer);

        var reference = new FindUsedFields(compiler).findUsedFields(reader);
        var paths = RustCircuitPass.parameterFieldDependencies(compiler, reader);
        Assert.assertEquals(List.of(List.of(List.of(1))), paths);
        Assert.assertEquals(reference.get(outer).toString(), FindUsedFields.fromPaths(reader, paths).get(outer).toString());

        // Both RETURNs retain the same argument, even after copying the closure.
        var returned = new DBSPReturnExpression(CalciteObject.EMPTY, shared.deref().field(0));
        var conflicting = new DBSPBlockExpression(List.of(returned.closure(inner).toStatement(),
                returned.toStatement()), outer.asVariable().deref().field(1)).closure(outer);
        var error = Assert.assertThrows(InternalCompilerError.class,
                () -> new FindUsedFields(compiler).findUsedFields(conflicting));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("Changing declaration"));
        Assert.assertNull(RustCircuitPass.parameterFieldDependencies(compiler, conflicting));
    }

    @Test
    public void narrowingValidatesTheOriginalReaderAfterAnalyzingItsCopy() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT, b INT); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var type = source.getOutput(0).getOutputRowType().ref();
        var outer = new DBSPParameter("row", type);
        var inner = new DBSPParameter("row", type);
        var shared = outer.asVariable();
        var nested = shared.deref().field(0).closure(inner);
        var result = new DBSPTupleExpression(shared.deref().field(1));
        var reader = new DBSPBlockExpression(List.of(nested.toStatement()), result).closure(outer);
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        circuit.addOperator(source);
        circuit.addOperator(new DBSPMapOperator(CalciteEmptyRel.INSTANCE, reader, source.getOutput(0)));

        var direct = new RemoveUnusedFields(compiler, new AnalyzedSet<>());
        long start = DBSPVariablePath.nextGeneratedName();
        var expected = Assert.assertThrows(InternalCompilerError.class, () -> direct.apply(circuit));
        long allocated = DBSPVariablePath.nextGeneratedName() - start;
        Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("Changing declaration"));

        compiler.options.ioOptions.rustOptimizerPasses = true;
        var hybrid = new RemoveUnusedFields(compiler, new AnalyzedSet<>());
        start = DBSPVariablePath.nextGeneratedName();
        var actual = Assert.assertThrows(InternalCompilerError.class, () -> hybrid.apply(circuit));
        Assert.assertEquals(expected.getMessage(), actual.getMessage());
        Assert.assertSame(expected.dbspNode, actual.dbspNode);
        Assert.assertEquals(allocated, DBSPVariablePath.nextGeneratedName() - start);
    }

    @Test
    public void evaluatedLoopsRetainTheirIterationInputAndStatementReads() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(false);
        var row = new DBSPTypeTuple(text, text).ref().var();
        var block = new DBSPBlockExpression(List.of(row.deref().field(0).toStatement()), null);
        var loop = new DBSPForExpression("unused", row.deepCopy().deref(), block);
        var body = new DBSPBlockExpression(List.of(loop.toStatement()), row.deepCopy().deref().field(1));
        var reader = body.closure(row);
        var paths = RustCircuitPass.parameterFieldDependencies(compiler, reader);
        Assert.assertEquals(List.of(List.of(List.of(), List.of(0), List.of(1))), paths);
        var reference = new FindUsedFields(compiler).findUsedFields(reader);
        Assert.assertEquals(reference.getUse().toString(), FindUsedFields.fromPaths(reader, paths).getUse().toString());
        Assert.assertNull(RustCircuitPass.parameterFieldDependencies(compiler, loop.closure(row)));
        Assert.assertNull(RustCircuitPass.parameterFieldDependencies(compiler, block.closure(row)));
    }

    @Test
    public void discardedExpressionsCanReadFieldsWithoutProducingAFieldUseValue() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(true);
        var type = new DBSPTypeTuple(text, text);
        var row = type.ref().var();
        var first = row.deref().field(0);
        var site = CalciteObject.EMPTY;
        var comparator = new DBSPEqualityComparatorExpression(site, new DBSPNoComparatorExpression(site, type));
        var singleton = new DBSPTupleExpression(first);
        var ordered = new DBSPCustomOrdExpression(site, singleton, new DBSPNoComparatorExpression(site, singleton.getType()));
        for (var expression : List.of(first.question(), new DBSPReturnExpression(site, first), comparator,
                ordered, new DBSPUnwrapCustomOrdExpression(ordered), new DBSPQualifyTypeExpression(first, text))) {
            var body = new DBSPBlockExpression(List.of(expression.toStatement()), row.deepCopy().deref().field(1));
            var reader = body.closure(row);
            var paths = RustCircuitPass.parameterFieldDependencies(compiler, reader);
            var expected = expression == comparator ? List.of(List.of(1)) : List.of(List.of(0), List.of(1));
            Assert.assertEquals(List.of(expected), paths);
            var reference = new FindUsedFields(compiler).findUsedFields(reader);
            Assert.assertEquals(reference.getUse().toString(), FindUsedFields.fromPaths(reader, paths).getUse().toString());

            var returned = expression.closure(row);
            Assert.assertNull(RustCircuitPass.parameterFieldDependencies(compiler, returned));
            Assert.assertThrows(RuntimeException.class, () -> new FindUsedFields(compiler).findUsedFields(returned));
        }
    }

    @Test
    public void discardedCollectionOperandsStillReadTheirInputFields() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteObject.EMPTY;
        var text = DBSPTypeString.varchar(false);
        var type = new DBSPTypeTuple(text, text);
        var row = type.ref().var();
        var multiset = new DBSPZSetExpression(row.deref().field(0));
        var indexed = new DBSPIndexedZSetExpression(site, new DBSPTypeIndexedZSet(site, type, type));
        var sort = new DBSPSortExpression(site, type, new DBSPNoComparatorExpression(site, type));
        for (var expression : List.of(multiset, indexed, sort, new DBSPEnumValue("Ordering", "Equal"),
                new NoExpression(text))) {
            var block = new DBSPBlockExpression(List.of(expression.toStatement()), row.deepCopy().deref().field(1));
            var reader = block.closure(row);
            var paths = RustCircuitPass.parameterFieldDependencies(compiler, reader);
            var expected = expression == multiset ? List.of(List.of(0), List.of(1)) : List.of(List.of(1));
            Assert.assertEquals(List.of(expected), paths);
            var reference = new FindUsedFields(compiler).findUsedFields(reader);
            Assert.assertEquals(reference.getUse().toString(), FindUsedFields.fromPaths(reader, paths).getUse().toString());

            // The same expression needs a symbolic value when returned as a tuple member.
            var result = new DBSPTupleExpression(expression).closure(row);
            Assert.assertNull(RustCircuitPass.parameterFieldDependencies(compiler, result));
            var error = Assert.assertThrows(RuntimeException.class, () -> new FindUsedFields(compiler).findUsedFields(result));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("does not exist in map"));
        }
    }

    @Test
    public void fieldTrimmingVisitsBlockItemsInTheirEnclosingStatement() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteObject.EMPTY;
        var literal = new DBSPI32Literal(7);
        var integer = literal.getType();
        var row = new DBSPVariablePath("row", new DBSPTypeTuple(integer, integer, integer).ref());
        var struct = new DBSPTypeStruct(site, new ProgramIdentifier("S"), List.of(), false);
        var function = new DBSPFunction(site, "seven", List.of(), integer, literal, List.of());
        List<DBSPStatement> items = List.of(new DBSPStaticItem(new DBSPStaticExpression(site, row.deref().field(0), "saved")),
                new DBSPComparatorItem(new DBSPNoComparatorExpression(site, integer)),
                new DBSPStructItem(struct, null), new DBSPFunctionItem(function));
        var block = new DBSPBlockExpression(items, row.deepCopy().deref().field(2));
        for (boolean discarded : List.of(false, true)) {
            var body = discarded ? new DBSPBlockExpression(List.of(block.toStatement()), literal) : block;
            var reader = body.closure(row);
            var paths = RustCircuitPass.parameterFieldDependencies(compiler, reader);
            Assert.assertEquals(List.of(discarded ? List.of(List.of(0), List.of(2)) : List.of(List.of(2))), paths);
            var direct = new FindUsedFields(compiler).findUsedFields(reader);
            Assert.assertEquals(direct.getUse().toString(), FindUsedFields.fromPaths(reader, paths).getUse().toString());
        }
    }

    @Test
    public void localFunctionParameterCannotReadTheOuterRowsFields() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPI32Literal(0).getType();
        var row = new DBSPVariablePath("row", new DBSPTypeTuple(integer, integer).ref());
        var shadow = new DBSPParameter("row", row.getType());
        var local = new DBSPFunction(CalciteObject.EMPTY, "local", List.of(shadow),
                integer, shadow.asVariable().deref().field(0), List.of());
        var reader = new DBSPBlockExpression(List.of(new DBSPFunctionItem(local)), new DBSPI32Literal(1)).closure(row);
        Assert.assertNull(RustCircuitPass.parameterFieldDependencies(compiler, reader));
        Assert.assertThrows(InternalCompilerError.class, () -> new FindUsedFields(compiler).findUsedFields(reader));
    }

    @Test
    public void discardedAggregateReadsItsEmptySetResult() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(false);
        var row = new DBSPTypeTuple(text, text, text).ref().var();
        var element = text.var();
        var accumulator = text.var();
        var aggregate = new LinearAggregate(CalciteObject.EMPTY, element.closure(element),
                accumulator.closure(accumulator), row.deref().field(0));
        var body = new DBSPBlockExpression(List.of(aggregate.toStatement()), row.deepCopy().deref().field(2));
        var map = body.closure(row);
        var expected = new FindUsedFields(compiler).findUsedFields(map);
        Assert.assertEquals("Ref([X, _, X])", expected.getUse().toString());
        Assert.assertEquals(List.of(List.of(List.of(0), List.of(2))),
                RustCircuitPass.parameterFieldDependencies(compiler, map));
    }

    @Test
    public void discardedStatementsAreCheckedBeforeTheFinalFieldRead() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(false);
        var row = new DBSPTypeTuple(text, text).ref().var();
        var first = row.deref().field(0);
        var second = row.deref().field(1);
        var sameField = first.deepCopy();
        var computed = new DBSPApplyExpression("identity", text, first);
        for (var rhs : List.of(sameField, second, computed)) {
            var assignment = new DBSPAssignmentExpression(first, rhs);
            var nested = new DBSPBlockExpression(List.of(assignment.toStatement()), first);
            var body = new DBSPBlockExpression(List.of(new DBSPComment("discarded read"), nested.toStatement()), second);
            var reader = body.closure(row);
            var actual = RustCircuitPass.parameterFieldDependencies(compiler, reader);
            var visitor = new FindUsedFields(compiler);
            compiler.options.ioOptions.rustOptimizerPasses = false;
            if (rhs == sameField) {
                Assert.assertEquals(List.of(List.of(List.of(0), List.of(1))), actual);
                var reference = visitor.findUsedFields(reader);
                var param = reader.parameters[0];
                Assert.assertEquals(reference.get(param).toString(),
                        FindUsedFields.fromPaths(reader, actual).get(param).toString());
            } else {
                Assert.assertNull(actual);
                Assert.assertThrows(InternalCompilerError.class, () -> visitor.findUsedFields(reader));
                compiler.options.ioOptions.rustOptimizerPasses = true;
                Assert.assertThrows(InternalCompilerError.class, () -> new FindUsedFields(compiler).findUsedFields(reader));
            }
        }
    }

    @Test
    public void mapSplitChecksOriginalBindingsOnlyWhenRewritingTheReader() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT, b INT); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var type = source.getOutput(0).getOutputRowType().ref();
        var outer = new DBSPParameter("row", type);
        var inner = new DBSPParameter("row", type);
        var shared = outer.asVariable();
        var nested = shared.deref().field(0).closure(inner);
        var answers = new ArrayList<Boolean>();
        compiler.stateObserver = new DBSPCompiler.StateObserver() {
            @Override public void nativeDecision(String pass, boolean answered) {
                if (pass.equals("SplitInspectedMapInput"))
                    answers.add(answered);
            }
            @Override public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit input) {}
            @Override public void after(String parent, int index, int iteration, CircuitTransform pass,
                                        DBSPCircuit output, boolean completed) {}
        };
        for (boolean narrow : List.of(false, true)) {
            var tail = narrow ? new DBSPTupleExpression(shared.deref().field(1)) : shared.deref();
            var reader = new DBSPBlockExpression(List.of(nested.toStatement()), tail).closure(outer);
            var map = new DBSPMapOperator(CalciteEmptyRel.INSTANCE, reader, source.getOutput(0));
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            circuit.addOperator(source);
            circuit.addOperator(map);
            String expected = null;
            long expectedNames = 0;
            for (boolean hybrid : List.of(false, true)) {
                compiler.options.ioOptions.rustOptimizerPasses = hybrid;
                var pass = new RemoveUnusedFields(compiler, new AnalyzedSet<>());
                answers.clear();
                long before = DBSPVariablePath.nextGeneratedName();
                if (narrow) {
                    var failure = Assert.assertThrows(InternalCompilerError.class, () -> pass.apply(circuit));
                    Assert.assertTrue(failure.getMessage(), failure.getMessage().contains("Changing declaration"));
                    if (hybrid)
                        Assert.assertEquals(expected, failure.getMessage());
                    else
                        expected = failure.getMessage();
                } else {
                    Assert.assertSame(map, pass.apply(circuit).allOperators.get(1));
                }
                long allocated = DBSPVariablePath.nextGeneratedName() - before;
                if (hybrid) {
                    Assert.assertEquals(expectedNames, allocated);
                    Assert.assertEquals(List.of(true), answers);
                } else {
                    expectedNames = allocated;
                }
            }
        }
    }

    @Test
    public void linearAggregateNarrowsOnlyTheIndexedValue() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT, b INT, c VARCHAR); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var rows = source.getOutput(0);
        var row = rows.getOutputRowType().ref().var();
        var site = CalciteEmptyRel.INSTANCE;
        var index = new DBSPMapIndexOperator(site,
                new DBSPRawTupleExpression(new DBSPTupleExpression(row.deref().field(0)), row.deepCopy().deref()).closure(row), rows);
        var pair = index.getOutputIndexedZSetType();
        var calls = new ArrayList<String>();
        compiler.stateObserver = new DBSPCompiler.StateObserver() {
            @Override public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit input) {}
            @Override public void after(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit output, boolean completed) {}
            @Override public void nativeDecision(String pass, boolean answered) {
                if (pass.equals("RemoveUnusedFields/LinearAggregateInput") && answered)
                    calls.add(pass);
            }
        };
        for (var body : List.of(row.deref(), new DBSPTupleExpression(row.deref().field(2)), new DBSPTupleExpression())) {
            var reader = body.closure(row);
            var outputRow = body.getType().ref().var();
            var output = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, pair.keyType, body.getType());
            var aggregate = new DBSPAggregateLinearPostprocessOperator(site, output, reader,
                    outputRow.deref().closure(outputRow), index.outputPort());
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            circuit.addOperator(source);
            circuit.addOperator(index);
            circuit.addOperator(aggregate);
            compiler.options.ioOptions.rustOptimizerPasses = false;
            long before = DBSPVariablePath.nextGeneratedName();
            var direct = new RemoveUnusedFields(compiler, new AnalyzedSet<>()).apply(circuit)
                    .allOperators.getLast().to(DBSPAggregateLinearPostprocessOperator.class);
            long names = DBSPVariablePath.nextGeneratedName() - before;
            compiler.options.ioOptions.rustOptimizerPasses = true;
            before = DBSPVariablePath.nextGeneratedName();
            calls.clear();
            var hybrid = new RemoveUnusedFields(compiler, new AnalyzedSet<>()).apply(circuit)
                    .allOperators.getLast().to(DBSPAggregateLinearPostprocessOperator.class);
            Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - before);
            Assert.assertEquals(List.of("RemoveUnusedFields/LinearAggregateInput"), calls);
            Assert.assertEquals(direct == aggregate, hybrid == aggregate);
            Assert.assertSame(aggregate.postProcess, hybrid.postProcess);
            Assert.assertTrue(EquivalenceContext.equiv(direct.getClosureFunction(), hybrid.getClosureFunction()));
            var expected = direct.input().node().to(DBSPMapIndexOperator.class);
            var actual = hybrid.input().node().to(DBSPMapIndexOperator.class);
            Assert.assertTrue(EquivalenceContext.equiv(expected.getClosureFunction(), actual.getClosureFunction()));
            Assert.assertTrue(pair.keyType.sameType(actual.getOutputIndexedZSetType().keyType));
        }
    }

    @Test
    public void streamAggregatesShareTheInputProjectionAndCheckOriginalBindings() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT, b INT, c INT); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var rows = source.getOutput(0);
        var row = rows.getOutputRowType().ref().var();
        var site = CalciteEmptyRel.INSTANCE;
        var index = new DBSPMapIndexOperator(site,
                new DBSPRawTupleExpression(new DBSPTupleExpression(), row.deref()).closure(row), rows);
        var first = row.deref().field(0);
        var acc = first.getType().var();
        var identity = acc.closure(acc);
        var empty = first.getType().none();
        var right = new LinearAggregate(site, row.deref().field(2).closure(row), identity, empty);
        for (boolean shared : List.of(false, true)) {
            var nested = first.closure(row.asParameter());
            var left = new LinearAggregate(site, shared
                    ? new DBSPBlockExpression(List.of(nested.toStatement()), first).closure(row)
                    : first.closure(row), identity, empty);
            var list = new DBSPAggregateList(site, row, List.of(left, right));
            var output = new DBSPTypeIndexedZSet(site, index.getOutputIndexedZSetType().keyType, list.getEmptySetResultType());
            var operator = new DBSPStreamAggregateOperator(site, output, null, list, index.outputPort());
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            circuit.addOperator(source);
            circuit.addOperator(index);
            circuit.addOperator(operator);
            String diagnostic = null;
            DBSPAggregateList expected = null;
            for (boolean hybrid : List.of(false, true)) {
                compiler.options.ioOptions.rustOptimizerPasses = hybrid;
                var pass = new RemoveUnusedFields(compiler, new AnalyzedSet<>());
                if (shared) {
                    var error = Assert.assertThrows(InternalCompilerError.class, () -> pass.apply(circuit));
                    Assert.assertTrue(error.getMessage(), error.getMessage().contains("Changing declaration"));
                    if (hybrid)
                        Assert.assertEquals(diagnostic, error.getMessage());
                    diagnostic = error.getMessage();
                } else {
                    var result = pass.apply(circuit).allOperators.getLast().to(DBSPStreamAggregateOperator.class);
                    Assert.assertEquals(2, result.input().getOutputIndexedZSetType().elementType.to(DBSPTypeTuple.class).size());
                    Assert.assertSame(empty, result.aggregateList.aggregates.get(1).getEmptySetResult());
                    if (hybrid)
                        Assert.assertTrue(expected.equivalent(result.aggregateList));
                    expected = result.aggregateList;
                }
            }
        }
    }

    @Test
    public void unnestTrimmingResolvesTheOriginalCollectionReaderOnlyWhenNarrowing() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT, items INT ARRAY); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var input = source.getOutput(0);
        var type = input.getOutputRowType().to(DBSPTypeTuple.class);
        var row = type.ref().var();
        var shadow = row.asParameter();
        var statement = row.deref().field(0).closure(shadow).toStatement();
        var reader = new DBSPBlockExpression(List.of(statement), row.deref().field(1)).closure(row);
        for (boolean narrow : List.of(false, true)) {
            var passthrough = narrow ? List.<Integer>of() : List.of(0);
            int width = passthrough.size() + 1;
            var flatmap = new DBSPFlatmap(CalciteObject.EMPTY, type, reader, passthrough,
                    null, null, new IdShuffle(width));
            var operator = new DBSPFlatMapOperator(CalciteEmptyRel.INSTANCE, flatmap,
                    new DBSPTypeZSet(flatmap.getType().to(DBSPTypeFunction.class).resultType), input);
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            circuit.addOperator(source);
            circuit.addOperator(operator);
            String diagnostic = null;
            long allocations = 0;
            for (boolean hybrid : List.of(false, true)) {
                compiler.options.ioOptions.rustOptimizerPasses = hybrid;
                var pass = new RemoveUnusedFields(compiler, new AnalyzedSet<>());
                long before = DBSPVariablePath.nextGeneratedName();
                if (narrow) {
                    var failure = Assert.assertThrows(InternalCompilerError.class, () -> pass.apply(circuit));
                    Assert.assertTrue(failure.getMessage(), failure.getMessage().contains("Changing declaration"));
                    if (hybrid)
                        Assert.assertEquals(diagnostic, failure.getMessage());
                    diagnostic = failure.getMessage();
                } else {
                    Assert.assertSame(operator, pass.apply(circuit).allOperators.getLast());
                }
                long consumed = DBSPVariablePath.nextGeneratedName() - before;
                if (hybrid)
                    Assert.assertEquals(allocations, consumed);
                allocations = consumed;
            }
        }
    }

    @Test
    public void joinTrimmingChecksSharedReadersAfterProjectingInputs() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT, b INT); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        var input = source.getOutput(0);
        var row = input.getOutputRowType().ref().var();
        var index = new DBSPMapIndexOperator(CalciteEmptyRel.INSTANCE,
                new DBSPRawTupleExpression(new DBSPTupleExpression(), row.deref()).closure(row), input);
        var indexed = index.outputPort();
        var type = indexed.getOutputIndexedZSetType();
        var key = new DBSPParameter("key", type.keyType.ref());
        var left = new DBSPParameter("left", type.elementType.ref());
        var right = new DBSPParameter("right", type.elementType.ref());
        var nested = new DBSPParameter("left", left.getType());
        var shared = left.asVariable();
        var discarded = shared.deref().field(0).closure(nested).toStatement();
        var comparator = new DBSPDirectComparatorExpression(CalciteObject.EMPTY,
                new DBSPNoComparatorExpression(CalciteObject.EMPTY, row.deref().field(1).getType()), true);
        var decisions = new ArrayList<String>();
        compiler.stateObserver = new DBSPCompiler.StateObserver() {
            @Override public void nativeDecision(String pass, boolean answered) {
                if (pass.startsWith("RemoveUnusedFields/Narrow"))
                    decisions.add(pass + "=" + answered);
            }
            @Override public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit input) {}
            @Override public void after(String parent, int index, int iteration, CircuitTransform pass,
                                        DBSPCircuit output, boolean completed) {}
        };
        for (boolean asof : List.of(false, true)) {
            for (boolean narrow : List.of(false, true)) {
                var leftOutput = narrow ? shared.deref().field(1) : shared.deref();
                var reader = new DBSPTupleExpression(new DBSPBlockExpression(List.of(discarded), leftOutput),
                        right.asVariable().deref()).closure(key, left, right);
                var output = new DBSPTypeZSet(reader.getResultType());
                var join = asof
                        ? new DBSPAsofJoinOperator(CalciteEmptyRel.INSTANCE, output, reader, 1, 1,
                                comparator, false, indexed, indexed)
                        : new DBSPJoinOperator(CalciteEmptyRel.INSTANCE, output, reader, indexed, indexed, false);
                var circuit = new DBSPCircuit(new ProgramMetadata(), false);
                circuit.addOperator(source);
                circuit.addOperator(index);
                circuit.addOperator(join);
                String message = null;
                long allocations = 0;
                for (boolean hybrid : List.of(false, true)) {
                    compiler.options.ioOptions.rustOptimizerPasses = hybrid;
                    var pass = new RemoveUnusedFields(compiler, new AnalyzedSet<>());
                    long before = DBSPVariablePath.nextGeneratedName();
                    decisions.clear();
                    if (narrow) {
                        var failure = Assert.assertThrows(InternalCompilerError.class, () -> pass.apply(circuit));
                        Assert.assertTrue(failure.getMessage(), failure.getMessage().contains("Changing declaration"));
                        if (hybrid)
                            Assert.assertEquals(message, failure.getMessage());
                        message = failure.getMessage();
                    } else {
                        Assert.assertSame(join, pass.apply(circuit).allOperators.getLast());
                    }
                    long names = DBSPVariablePath.nextGeneratedName() - before;
                    if (hybrid) {
                        Assert.assertEquals(allocations, names);
                        Assert.assertEquals(List.of("RemoveUnusedFields/Narrow" + (asof ? "Asof" : "Join") + "=true"), decisions);
                    }
                    allocations = names;
                }
            }
        }
    }

    @Test
    public void fieldUseFailuresComeFromRustInEvaluationOrder() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPI32Literal(1).getType();
        var row = new DBSPVariablePath("row", new DBSPTypeTuple(integer, integer).ref());
        var missing = new NoExpression(integer);
        var assign = new DBSPAssignmentExpression(row.deref().field(0), row.deepCopy().deref().field(1));
        var free = new DBSPVariablePath("unbound", integer);
        var assignFirst = new DBSPBlockExpression(List.of(assign.toStatement()), missing);
        var missingFirst = new DBSPTupleExpression(missing, assign);
        var decisions = new ArrayList<Boolean>();
        compiler.stateObserver = new DBSPCompiler.StateObserver() {
            @Override public void nativeDecision(String pass, boolean answered) {
                if (pass.equals("ParameterFieldDependencies"))
                    decisions.add(answered);
            }
            @Override public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit input) {}
            @Override public void after(String parent, int index, int iteration, CircuitTransform pass,
                                        DBSPCircuit output, boolean completed) {}
        };
        for (var body : List.of(assignFirst, missingFirst, new DBSPTupleExpression(missing),
                new DBSPBlockExpression(List.of(assign.toStatement()), free))) {
            var reader = body.closure(row);
            compiler.options.ioOptions.rustOptimizerPasses = false;
            var direct = Assert.assertThrows(RuntimeException.class, () -> new FindUsedFields(compiler).findUsedFields(reader));
            compiler.options.ioOptions.rustOptimizerPasses = true;
            decisions.clear();
            var hybrid = Assert.assertThrows(direct.getClass(), () -> new FindUsedFields(compiler).findUsedFields(reader));
            Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
            Assert.assertEquals(List.of(true), decisions);
        }
    }

    @Test
    public void fieldUseAssignmentOnlyInitializesItsOwnOccurrence() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPI32Literal(1).getType();
        var row = new DBSPTypeTuple(integer, integer).ref().var();
        var target = new NoExpression(integer);
        var assignment = new DBSPAssignmentExpression(target, row.deref().field(0));
        var valid = new DBSPBlockExpression(List.of(assignment.toStatement()), row.deepCopy().deref().field(1)).closure(row);
        var shared = new DBSPBlockExpression(List.of(assignment.toStatement()), target).closure(row);
        for (boolean hybrid : List.of(false, true)) {
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var finder = new FindUsedFields(compiler);
            Assert.assertEquals("Ref([X, X])", finder.findUsedFields(valid).getUse().toString());
            // ensureTree copies the second occurrence. It has no assigned value.
            var error = Assert.assertThrows(RuntimeException.class, () -> finder.findUsedFields(shared));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("does not exist in map"));
        }
    }

    @Test
    public void fieldUseReportsTheIndexedParameterWidth() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPI32Literal(0).getType();
        var raw = new DBSPTypeRawTuple(integer, integer, integer).var();
        // Indexed parameters expose a key and a value to the field-use interpreter.
        var reader = raw.field(2).closure(raw);
        var direct = Assert.assertThrows(IndexOutOfBoundsException.class,
                () -> new FindUsedFields(compiler).findUsedFields(reader));
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var hybrid = Assert.assertThrows(IndexOutOfBoundsException.class,
                () -> new FindUsedFields(compiler).findUsedFields(reader));
        Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
    }

    @Test
    public void sqlTopKUsesRustToDiscoverAndRemapComparedFields() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var options = new CompilerOptions();
        options.ioOptions.rustOptimizerPasses = true;
        var compiler = new DBSPCompiler(options);
        var answered = new HashSet<String>();
        compiler.stateObserver = new DBSPCompiler.StateObserver() {
            @Override
            public void nativeDecision(String operation, boolean success) {
                if (success)
                    answered.add(operation);
            }

            @Override
            public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit input) {}

            @Override
            public void after(String parent, int index, int iteration, CircuitTransform pass,
                              DBSPCircuit output, boolean completed) {}
        };
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t(id INT NOT NULL, ts INT NOT NULL, a INT, b INT, c INT);
                CREATE VIEW v AS SELECT id, a, FIRST_VALUE(b) OVER (PARTITION BY id ORDER BY ts) AS first_b FROM t;
                """);
        compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        Assert.assertTrue(answered.contains("RemoveUnusedFields/TopKInput"));
        Assert.assertTrue(answered.contains("RemoveUnusedFields/TopKFields"));
        Assert.assertTrue(answered.contains("RemoveUnusedFields/TopKComparators"));
    }

    @Test
    public void rebindsEachJoinInputAndRetainsTheKeyDeclaration() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(false);
        var row = new DBSPTypeTuple(text, text, new DBSPTypeTuple(text));
        var key = new DBSPParameter("key", text.ref());
        var left = new DBSPParameter("left", row.ref());
        var right = new DBSPParameter("right", row.ref());
        var body = new DBSPTupleExpression(left.asVariable().deref().field(2).field(0),
                right.asVariable().deref().field(1), key.asVariable().deref());
        var closure = body.closure(key, left, right);
        var fields = new FindUsedFields(compiler).findUsedFields(closure);
        var rewriter = fields.getFieldRewriter(compiler, 1);
        rewriter.parameterFullyUsed(key);
        var expected = rewriter.rewriteClosure(closure);
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var actual = rewriter.rewriteClosure(closure);
        Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
        Assert.assertSame(key, actual.parameters[0]);
        Assert.assertSame(rewriter.newParam.get(left), actual.parameters[1]);
        Assert.assertSame(rewriter.newParam.get(right), actual.parameters[2]);
    }

    @Test
    public void topKProjectionPreservesTieBreakersAndNullOrdering() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteObject.EMPTY;
        var text = DBSPTypeString.varchar(true);
        var row = new DBSPTypeTuple(text, text, text, text);
        var narrowed = new DBSPTypeTuple(text, text);
        var base = new DBSPNoComparatorExpression(site, row);
        var order = base.field(3, false, true).field(1, true, false);
        var ties = base.field(3, true, false);
        Assert.assertEquals(List.of(1, 3), RustCircuitPass.topKComparedFields(compiler, order, ties));
        Assert.assertEquals(List.of(), RustCircuitPass.topKComparedFields(compiler, base, base));
        var direct = new DBSPDirectComparatorExpression(site, base, true);
        Assert.assertNull(RustCircuitPass.topKComparedFields(compiler, order, direct));

        var fields = new FieldUseMap(row, false);
        fields.setUsed(1);
        fields.setUsed(3);
        var result = RustCircuitPass.projectTopKComparators(compiler, order, ties, narrowed, fields.usageAtDepth(1));
        var expectedOrder = RemoveUnusedFields.remapComparedFields(order, fields, narrowed);
        var expectedTies = RemoveUnusedFields.remapComparedFields(ties, fields, narrowed);
        Assert.assertTrue(EquivalenceContext.equiv(expectedOrder, result.left));
        Assert.assertTrue(EquivalenceContext.equiv(expectedTies, result.right));
        Assert.assertEquals(expectedOrder.getComparatorStructName(), result.left.getComparatorStructName());
        Assert.assertEquals(expectedTies.getComparatorStructName(), result.right.getComparatorStructName());
        Assert.assertSame(narrowed, result.left.comparedValueType());
        Assert.assertSame(narrowed, result.right.comparedValueType());
    }

    @Test
    public void compressesParametersAtTheRequestedDepthAndSharesOriginalLeaves() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(false);
        var nested = new DBSPTypeTuple(text, text).withMayBeNull(true);
        var pair = new DBSPTypeRawTuple(nested.ref(), new DBSPTypeTuple().ref());
        var bits = new FieldUseMap(pair, false);
        bits.field(0).deref().setUsed(1);
        for (int depth : List.of(0, 1, 2)) {
            var expected = bits.compressedType(depth);
            Assert.assertNotNull(expected);
            var actual = RustCircuitPass.compressedParameterTypes(compiler, depth, List.of(pair, text),
                    List.of(bits.usageAtDepth(depth), List.of(true)));
            Assert.assertTrue(expected.sameType(actual.get(0)));
            Assert.assertSame(text, actual.get(1));
            if (depth == 0)
                Assert.assertSame(pair, actual.get(0));
            else {
                var row = actual.get(0).to(DBSPTypeRawTuple.class).tupFields[0].deref().to(DBSPTypeTuple.class);
                Assert.assertEquals(depth == 1 ? 2 : 1, row.size());
                Assert.assertSame(text, row.tupFields[0]);
                Assert.assertTrue(row.mayBeNull);
            }
        }
    }

    @Test
    public void indexedValueProjectionExpandsNestedKeysAndPreservesNullableRows() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var number = new DBSPTypeInteger(CalciteObject.EMPTY, 64, true, false);
        var text = DBSPTypeString.varchar(false);
        var nested = new DBSPTypeTuple(number, text).withMayBeNull(true);
        var key = new DBSPTypeTuple(text, nested);
        for (boolean nullable : List.of(false, true)) {
            var row = new DBSPTypeTuple(number, nested, text).withMayBeNull(nullable);
            var input = new DBSPTypeRawTuple(key.ref(), row.ref());
            for (var columns : List.of(List.<Integer>of(), List.of(1, 2))) {
                var used = new FieldUseMap(row.ref(), false);
                for (int column : columns)
                    used.deref().setUsed(column);
                long start = DBSPVariablePath.nextGeneratedName();
                var projection = used.getProjection(1);
                Assert.assertNotNull(projection);
                var expected = RemoveUnusedFields.indexedValueProjection(compiler, input, projection);
                long end = DBSPVariablePath.nextGeneratedName();
                DBSPVariablePath.restoreGeneratedNames(start);
                var actual = RustCircuitPass.projectUsedFields(compiler, used.getType(), used.usageAtDepth(1), input);
                Assert.assertEquals(end, DBSPVariablePath.nextGeneratedName());
                Assert.assertTrue(expected.getResultType().sameType(actual.getResultType()));
                Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            }
        }
    }

    @Test
    public void fieldProjectionRetainsEmptyTuplesAndRawTuplePositions() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(false);
        var empty = new DBSPTypeTuple();
        var nested = new DBSPTypeTuple(text);
        for (var input : List.of(text, new DBSPTypeTuple(empty, text), new DBSPTypeRawTuple(text.ref(), nested))) {
            for (boolean read : List.of(false, true)) {
                var used = new FieldUseMap(input.ref(), read);
                long start = DBSPVariablePath.nextGeneratedName();
                var expected = used.getProjection(1);
                Assert.assertNotNull(expected);
                long end = DBSPVariablePath.nextGeneratedName();
                DBSPVariablePath.restoreGeneratedNames(start);
                var actual = RustCircuitPass.projectUsedFields(compiler, used.getType(), used.usageAtDepth(1), null);
                Assert.assertEquals(end, DBSPVariablePath.nextGeneratedName());
                Assert.assertTrue(expected.getResultType().sameType(actual.getResultType()));
                Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            }
        }
    }

    @Test
    public void keepsCompositeKeysAndDistinguishesNullFromAnEmptyValue() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteObject.EMPTY;
        var number = new DBSPTypeInteger(site, 64, true, false);
        var text = DBSPTypeString.varchar(false);
        var key = new DBSPTypeTuple(number, text);
        for (boolean nullable : List.of(false, true)) {
            var row = new DBSPTypeTuple(number, number, text).withMayBeNull(nullable);
            var pair = new DBSPTypeRawTuple(key.ref(), row.ref());
            for (var columns : List.of(List.<Integer>of(), List.of(0, 2))) {
                long start = DBSPVariablePath.nextGeneratedName();
                var expected = RemoveUnusedFields.joinInputProjection(site, pair, columns);
                long end = DBSPVariablePath.nextGeneratedName();
                DBSPVariablePath.restoreGeneratedNames(start);
                var actual = RustCircuitPass.projectJoinInput(compiler, site, pair, columns);
                Assert.assertEquals(end, DBSPVariablePath.nextGeneratedName());
                Assert.assertTrue(actual.getResultType().sameType(expected.getResultType()));
                Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            }
        }
    }
}
