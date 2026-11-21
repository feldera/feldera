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
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class StarJoinSnapshotTests {
    private static OutputPort input(DBSPCircuit circuit, DBSPTypeIndexedZSet type) {
        var value = new DBSPIndexedZSetExpression(CalciteObject.EMPTY, type);
        var op = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, value);
        circuit.addOperator(op);
        return op.outputPort();
    }

    /** Keep the key and build a pair of the two input values. */
    private static OutputPort indexedJoin(DBSPCircuit circuit, OutputPort left, OutputPort right) {
        var keyType = left.getOutputIndexedZSetType().keyType;
        var key = keyType.ref().var();
        var l = left.getOutputIndexedZSetType().elementType.ref().var();
        var r = right.getOutputIndexedZSetType().elementType.ref().var();
        var value = new DBSPTupleExpression(l.deref(), r.deref());
        var function = new DBSPRawTupleExpression(key.deref(), value).closure(key, l, r);
        var type = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, keyType, value.getType());
        var op = new DBSPStreamJoinIndexOperator(CalciteEmptyRel.INSTANCE, type, function, left, right, false);
        circuit.addOperator(op);
        return op.outputPort();
    }

    @Test
    public void indexedJoinTreePreservesSnapshots() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var integer = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, false);
        var type = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, integer, integer);
        var a = input(circuit, type);
        var b = input(circuit, type);
        var c = input(circuit, type);
        var root = indexedJoin(circuit, a, indexedJoin(circuit, b, c));
        var consumer = new DBSPDifferentiateOperator(CalciteEmptyRel.INSTANCE, root);
        circuit.addOperator(consumer);

        var rewritten = new CreateStarJoins(compiler).apply(circuit);
        var output = rewritten.allOperators.getLast().to(DBSPDifferentiateOperator.class).input();
        Assert.assertEquals(StreamKind.COLLECTION, output.kind());
        Assert.assertTrue(root.outputType().sameType(output.outputType()));
        var star = output.to(DBSPIntegrateOperator.class).input().to(DBSPStarJoinIndexOperator.class);
        Assert.assertEquals(StreamKind.DELTA, star.outputPort().kind());
        var leaves = List.of(a, b, c);
        Assert.assertEquals(leaves.size(), star.inputs.size());
        for (int i = 0; i < leaves.size(); i++)
            Assert.assertEquals(leaves.get(i), star.inputs.get(i).to(DBSPDifferentiateOperator.class).input());
    }
}
