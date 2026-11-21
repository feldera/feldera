package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.StarJoinState;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class StarJoinTreeTests {
    private static OutputPort input(DBSPCircuit circuit, DBSPTypeIndexedZSet type) {
        var op = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, new DBSPIndexedZSetExpression(CalciteObject.EMPTY, type));
        circuit.addOperator(op);
        return op.outputPort();
    }

    /** Keep the key and build a pair of the two input values. */
    private static OutputPort indexedJoin(DBSPCircuit circuit, OutputPort left, OutputPort right) {
        var key = left.getOutputIndexedZSetType().keyType.ref().var();
        var l = left.getOutputIndexedZSetType().elementType.ref().var();
        var r = right.getOutputIndexedZSetType().elementType.ref().var();
        var value = new DBSPTupleExpression(l.deref(), r.deref());
        var function = new DBSPRawTupleExpression(key.deref(), value).closure(key, l, r);
        var type = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, left.getOutputIndexedZSetType().keyType, value.getType());
        var op = new DBSPStreamJoinIndexOperator(CalciteEmptyRel.INSTANCE, type, function, left, right, false);
        circuit.addOperator(op);
        return op.outputPort();
    }

    @Test
    public void indexedRootPreservesLeafOrder() throws IOException {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var integer = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, false);
        var type = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, integer, integer);
        var a = input(circuit, type);
        var b = input(circuit, type);
        var c = input(circuit, type);
        var root = indexedJoin(circuit, a, indexedJoin(circuit, b, c));
        circuit.addOperator(new DBSPDifferentiateOperator(CalciteEmptyRel.INSTANCE, root));

        var graph = new Graph(compiler);
        graph.apply(circuit);
        var analysis = new CreateStarJoins.FindJoinTrees(compiler, graph.graphs);
        var snapshots = new OptimizerSnapshots(compiler);
        long initial = DBSPVariablePath.nextGeneratedName();
        var mapper = Utilities.deterministicObjectMapper();
        boolean nativePasses = Boolean.getBoolean("feldera.optimizer.rustPasses");
        byte[] plans;
        if (nativePasses) {
            Assert.assertSame(circuit, new RustCircuitPass(compiler, analysis, snapshots).apply(circuit));
            plans = new CreateStarJoins.ReplaceStarJoins(compiler, analysis.joinTrees).nativeReplacements();
        } else {
            Assert.assertSame(circuit, analysis.apply(circuit));
            var writer = snapshots.writer();
            writer.apply(circuit);
            plans = StarJoinState.encode(writer, analysis.replacements());
        }
        var trees = mapper.readTree(plans);
        Assert.assertEquals(1, trees.size());
        var tree = trees.get(0);
        Assert.assertEquals(root.operator.id, tree.required("root").longValue());
        var inputs = tree.required("inputs");
        var leaves = List.of(a, b, c);
        Assert.assertEquals(leaves.size(), inputs.size());
        for (int i = 0; i < leaves.size(); i++) {
            Assert.assertEquals(leaves.get(i).operator.id, inputs.required(i).required(0).longValue());
            Assert.assertEquals(leaves.get(i).outputNumber, inputs.required(i).required(1).intValue());
        }
        Assert.assertEquals(4, tree.required("function").required("parameters").size());
        // Three child parameters, two variables for the key test and four parent parameters.
        Assert.assertEquals(initial + 9, DBSPVariablePath.nextGeneratedName());

        var replacement = new CreateStarJoins.ReplaceStarJoins(compiler, analysis.joinTrees);
        var rewritten = nativePasses
                ? new RustCircuitPass(compiler, replacement, snapshots).apply(circuit)
                : replacement.apply(circuit);
        var output = rewritten.allOperators.getLast().to(DBSPDifferentiateOperator.class).input();
        Assert.assertEquals(StreamKind.COLLECTION, output.kind());
        Assert.assertTrue(root.outputType().sameType(output.outputType()));
        var star = output.to(DBSPIntegrateOperator.class).input().to(DBSPStarJoinIndexOperator.class);
        Assert.assertEquals(StreamKind.DELTA, star.outputPort().kind());
        var rewrittenLeaves = rewritten.allOperators.stream()
                .filter(DBSPConstantOperator.class::isInstance).map(op -> op.getOutput(0)).toList();
        Assert.assertEquals(3, rewrittenLeaves.size());
        Assert.assertEquals(rewrittenLeaves, star.inputs.stream()
                .map(port -> port.to(DBSPDifferentiateOperator.class).input()).toList());

        var key = new DBSPI32Literal(7);
        var first = new DBSPI32Literal(1);
        var second = new DBSPI32Literal(2);
        var third = new DBSPI32Literal(3);
        var row = star.getClosureFunction()
                .call(key.borrow(), first.borrow(), second.borrow(), third.borrow()).reduce(compiler);
        var expected = new DBSPRawTupleExpression(key,
                new DBSPTupleExpression(first, new DBSPTupleExpression(second, third)));
        Assert.assertTrue(row.toString(), expected.equivalent(row));
    }
}
