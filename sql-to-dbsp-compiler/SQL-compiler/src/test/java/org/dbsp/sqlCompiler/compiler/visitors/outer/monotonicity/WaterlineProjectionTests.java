package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class WaterlineProjectionTests {
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
