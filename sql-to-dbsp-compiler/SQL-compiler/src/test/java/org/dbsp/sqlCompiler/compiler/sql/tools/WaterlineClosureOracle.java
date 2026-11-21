package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.Waterline;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SimplifyWaterline;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.util.Utilities;

import java.io.IOException;

/** Exercise both validity branches even when SQL lowering emits only payload tuples. */
public final class WaterlineClosureOracle {
    private static JsonNode snapshot(DBSPCompiler compiler, DBSPCircuit circuit) throws IOException {
        var writer = ToJsonOuterVisitor.createForReplay(compiler);
        writer.apply(circuit);
        return Utilities.deterministicObjectMapper().readTree(writer.getJsonString());
    }

    private static void observe(boolean raw, boolean annotated) throws IOException {
        DBSPVariablePath.reset();
        var compiler = new DBSPCompiler(new CompilerOptions());
        var circuit = new DBSPCircuit(new ProgramMetadata(), true);
        var valid = new DBSPBoolLiteral(true);
        var value = new DBSPI32Literal(7);
        var row = raw ? new DBSPRawTupleExpression(valid, value) : new DBSPTupleExpression(valid, value);
        var source = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, new DBSPZSetExpression(row));
        circuit.addOperator(source);
        var entry = row.getType().ref().var();
        var unit = new DBSPRawTupleExpression().getType().ref().var();
        var other = row.getType().ref().var();
        var waterline = new DBSPWaterlineOperator(CalciteEmptyRel.INSTANCE, row.closure(),
                entry.deref().closure(entry, unit), entry.deref().closure(entry, other), source.outputPort());
        circuit.addOperator(waterline);
        var arg = row.getType().ref().var();
        var choose = new DBSPIfExpression(CalciteObject.EMPTY, arg.deref().field(0),
                arg.deref().field(1), new DBSPI32Literal(-1));
        var apply = new DBSPApplyOperator(CalciteEmptyRel.INSTANCE, choose.closure(arg), waterline.outputPort(), null);
        if (annotated)
            apply.annotations.add(Waterline.INSTANCE);
        circuit.addOperator(apply);
        var mapper = Utilities.deterministicObjectMapper();
        var record = mapper.createObjectNode();
        record.put("id", "waterline/raw=" + raw + "/annotated=" + annotated).put("pass", "SimplifyWaterline");
        var sample = record.putArray("invocations").addObject();
        sample.put("names_before", DBSPVariablePath.nextGeneratedName()).set("before", snapshot(compiler, circuit));
        var result = new SimplifyWaterline(compiler)
                .circuitRewriter(op -> op.hasAnnotation(a -> a.is(Waterline.class))).apply(circuit);
        sample.put("names_after", DBSPVariablePath.nextGeneratedName()).set("after", snapshot(compiler, result));
        System.out.println(mapper.writeValueAsString(record));
    }

    public static void main(String[] args) throws IOException {
        for (boolean raw : new boolean[]{false, true}) {
            observe(raw, true);
            observe(raw, false);
        }
    }
}
