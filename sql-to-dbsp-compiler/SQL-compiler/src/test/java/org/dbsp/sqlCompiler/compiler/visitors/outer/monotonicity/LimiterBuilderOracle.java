package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalConstantOperator;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

/** Capture limiter inputs, ambient name allocation and output from the Java pass. */
public final class LimiterBuilderOracle {
    private final DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());

    private JsonNode graph(DBSPCircuit circuit) throws Exception {
        ToJsonOuterVisitor visitor = ToJsonOuterVisitor.createForReplay(this.compiler);
        circuit.accept(visitor);
        return Utilities.deterministicObjectMapper().readTree(visitor.getJsonString());
    }

    private ObjectNode minimum(int count, boolean independentTypes) throws Exception {
        var empty = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(empty, 32, true, false);
        var type = new DBSPTypeRawTuple(new DBSPTypeTuple(integer, integer), integer);
        var circuit = new DBSPCircuit(new ProgramMetadata(), true);
        List<OutputPort> bounds = new ArrayList<>();
        var input = new DBSPConstantOperator(empty, DBSPZSetExpression.emptyWithElementType(integer));
        circuit.addOperator(input);
        for (int i = 0; i < count; i++) {
            if (independentTypes) {
                var field = new DBSPTypeInteger(empty, 32, true, false);
                type = new DBSPTypeRawTuple(new DBSPTypeTuple(field, field), field);
            }
            var initial = type.defaultValue();
            var left = type.ref().var();
            var right = type.ref().var();
            var value = new DBSPWaterlineOperator(empty, initial.closure(),
                    initial.closure(integer.ref().var(), DBSPTypeRawTuple.EMPTY.ref().var()),
                    left.deref().closure(left, right), input.outputPort());
            circuit.addOperator(value);
            bounds.add(value.outputPort());
        }
        ObjectNode record = Utilities.deterministicObjectMapper().createObjectNode();
        record.set("before", this.graph(circuit));
        record.put("next", DBSPVariablePath.nextGeneratedName());
        var bound = InsertLimiters.createMinBound(this.compiler, bounds);
        circuit.addOperator(bound.node());
        record.set("after", this.graph(circuit));
        record.put("after_next", DBSPVariablePath.nextGeneratedName());
        return record;
    }

    private ObjectNode window(boolean collection) throws Exception {
        var empty = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(empty, 32, true, false);
        var contents = new DBSPIndexedZSetExpression(empty, new DBSPTypeIndexedZSet(empty, integer, integer));
        var data = collection ? new DBSPConstantOperator(empty, contents) : new DBSPIncrementalConstantOperator(empty, contents);
        var pair = new DBSPTypeRawTuple(integer, integer);
        var init = pair.defaultValue();
        var control = new DBSPWaterlineOperator(empty, init.closure(),
                init.closure(integer.ref().var(), integer.ref().var()),
                init.closure(pair.ref().var(), pair.ref().var()), data.outputPort());
        var circuit = new DBSPCircuit(new ProgramMetadata(), true);
        circuit.addOperator(data);
        circuit.addOperator(control);
        var record = Utilities.deterministicObjectMapper().createObjectNode();
        record.set("before", this.graph(circuit));
        var destination = circuit;
        var pass = new InsertLimiters(this.compiler, circuit, new Monotonicity.MonotonicityInformation(), Map.of(), Set.of()) {
            @Override
            protected void addOperator(DBSPOperator op) { destination.addOperator(op); }
        };
        var result = pass.window(empty, collection, !collection, collection, data.outputPort(), control.outputPort());
        record.set("after", this.graph(circuit));
        record.put("input", result.input().node().getId());
        record.put("output", result.output().node().getId());
        return record;
    }

    public static void main(String[] args) throws Exception {
        var oracle = new LimiterBuilderOracle();
        ObjectNode cases = Utilities.deterministicObjectMapper().createObjectNode();
        if (args.length == 1 && args[0].equals("window")) {
            cases.set("collection", oracle.window(true));
            cases.set("delta", oracle.window(false));
            System.out.println(cases);
            return;
        }
        for (int count : List.of(1, 2, 3, 5))
            cases.set(Integer.toString(count), oracle.minimum(count, false));
        cases.set("4-distinct-types", oracle.minimum(4, true));
        System.out.println(cases);
    }
}
