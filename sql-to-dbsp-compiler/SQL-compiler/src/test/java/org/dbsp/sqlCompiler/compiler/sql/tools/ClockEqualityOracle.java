package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.DesugarNowEquality;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.util.Utilities;

import java.io.IOException;

/** Check equality guards and parameter identity when two call arguments share a closure. */
public final class ClockEqualityOracle {
    private static JsonNode snapshot(DBSPCompiler compiler, DBSPCircuit circuit) throws IOException {
        var writer = ToJsonOuterVisitor.createForReplay(compiler);
        writer.apply(circuit);
        return Utilities.deterministicObjectMapper().readTree(writer.getJsonString());
    }

    private static void observe(boolean clockOnLeft, boolean clockOnRight) throws IOException {
        DBSPVariablePath.reset();
        var compiler = new DBSPCompiler(new CompilerOptions());
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var timestamp = DBSPTypeTimestamp.INSTANCE;
        var input = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, new DBSPZSetExpression(timestamp));
        circuit.addOperator(input);
        var row = timestamp.ref().var();
        var param = timestamp.var();
        var closure = param.closure(param);
        DBSPExpression leftValue = clockOnLeft ? new DBSPApplyExpression("NoW", timestamp) : row.deref();
        var left = new DBSPApplyExpression("consume_closures", timestamp, closure, closure, leftValue);
        DBSPExpression right = clockOnRight ? new DBSPApplyExpression("now", timestamp) : row.deref();
        var eq = new DBSPBinaryExpression(CalciteObject.EMPTY, DBSPTypeBool.create(false), DBSPOpcode.EQ, left, right);
        circuit.addOperator(new DBSPFilterOperator(CalciteEmptyRel.INSTANCE, eq.closure(row), input.outputPort()));

        var mapper = Utilities.deterministicObjectMapper();
        var record = mapper.createObjectNode().put("pass", "DesugarNowEquality")
                .put("id", "nested-closure/left-clock=" + clockOnLeft + "/right-clock=" + clockOnRight);
        var invocation = record.putArray("invocations").addObject();
        invocation.put("names_before", DBSPVariablePath.nextGeneratedName()).set("before", snapshot(compiler, circuit));
        var result = new DesugarNowEquality(compiler).apply(circuit);
        invocation.put("names_after", DBSPVariablePath.nextGeneratedName()).set("after", snapshot(compiler, result));
        System.out.println(mapper.writeValueAsString(record));
    }

    public static void main(String[] args) throws IOException {
        for (boolean left : new boolean[]{false, true})
            for (boolean right : new boolean[]{false, true})
                observe(left, right);
    }
}
