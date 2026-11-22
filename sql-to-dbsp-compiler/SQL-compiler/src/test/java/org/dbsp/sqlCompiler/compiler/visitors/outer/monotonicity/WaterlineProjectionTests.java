package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class WaterlineProjectionTests {
    @Test
    public void projectNestedNullableRow() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var fixed = new MonotoneType(integer);
        var unknown = new NonMonotoneType(integer);
        var nested = new PartiallyMonotoneTuple(List.of(unknown, fixed), false, true);
        var row = new PartiallyMonotoneTuple(List.of(unknown, fixed, nested), false, false);
        // The closure's SQL types can be equal to distinct upstream types.
        var localInteger = new DBSPTypeInteger(node, 32, true, false);
        var localRow = new DBSPTypeTuple(localInteger, localInteger, nested.getType());
        var original = new DBSPVariablePath("row", localRow.ref());
        var body = new DBSPTupleExpression(original.deepCopy().deref().field(2).field(1),
                original.deepCopy().deref().field(0), original.deepCopy().deref().field(1));
        var closure = body.closure(original);
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(row.getType()));
        var analysis = new MonotoneTransferFunctions(compiler, operator,
                MonotoneTransferFunctions.ArgumentKind.ZSet, row);
        var direct = analysis.applyAnalysis(closure).getReducedExpression().to(DBSPClosureExpression.class);
        var parameter = new DBSPVariablePath("row", row.getProjectedType().ref());
        var expected = new DBSPTupleExpression(parameter.deepCopy().deref().field(1).field(0),
                parameter.deepCopy().deref().field(0)).closure(parameter);
        Assert.assertTrue(direct.equivalent(expected));
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var result = RustCircuitPass.projectRow(compiler, body, parameter, row);
            Assert.assertNotNull(result);
            Assert.assertTrue(result.getReducedExpression().closure(parameter).equivalent(expected));
            var shape = result.getMonotoneType().to(PartiallyMonotoneTuple.class);
            Assert.assertTrue(shape.getFieldType(0).getType().mayBeNull);
            Assert.assertFalse(shape.getFieldType(1).mayBeMonotone());
            Assert.assertSame(fixed, shape.getFieldType(2));
        }
    }

    @Test
    public void missingEmptyFieldsKeepTheirDeclaredType() {
        var options = new CompilerOptions();
        options.ioOptions.rustOptimizerPasses = Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply");
        var compiler = new DBSPCompiler(options);
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var field = new MonotoneType(integer);

        // Only the integer survives input compaction. Both destination fields
        // take their bound from the same declared, non-nullable empty tuple.
        var absent = new PartiallyMonotoneTuple(List.of(), false, true);
        var empty = new PartiallyMonotoneTuple(List.of(), false, false);
        var source = new PartiallyMonotoneTuple(List.of(absent, absent, field), false, false);
        var destination = new PartiallyMonotoneTuple(List.of(empty, empty, field), false, false);
        var projected = source.getProjectedType();
        var initial = projected.defaultValue();
        var left = projected.ref().var();
        var right = projected.ref().var();
        var input = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var waterline = new DBSPWaterlineOperator(node, initial.closure(),
                initial.closure(integer.ref().var(), DBSPTypeRawTuple.EMPTY.ref().var()),
                left.deref().closure(left, right), input.outputPort());
        var outputCircuit = new DBSPCircuit(new ProgramMetadata(), true);
        outputCircuit.addOperator(input);
        outputCircuit.addOperator(waterline);
        var pass = new InsertLimiters(compiler, outputCircuit, new Monotonicity.MonotonicityInformation(), Map.of(), Set.of()) {
            @Override
            protected void addOperator(DBSPOperator operator) {
                outputCircuit.addOperator(operator);
            }
        };

        var result = pass.project(waterline.outputPort(), source, destination);
        var body = result.simpleNode().getClosureFunction().body.to(DBSPTupleExpression.class);
        Assert.assertSame(empty.getType(), body.fields[0].getType());
        Assert.assertSame(empty.getType(), body.fields[1].getType());
    }
}
