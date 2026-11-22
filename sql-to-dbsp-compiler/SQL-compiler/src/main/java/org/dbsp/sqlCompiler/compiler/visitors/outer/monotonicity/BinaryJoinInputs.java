package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.AlwaysMonotone;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.JoinDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.JoinIndexDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.LeftJoinDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.OperatorDeltaExpansion;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/** The three terms of the incremental join, in visitor order. The sum can read them in a different order. */
record BinaryJoinInputs(DBSPDelayedIntegralOperator left, DBSPDelayedIntegralOperator right,
                        List<DBSPJoinBaseOperator> terms, DBSPSumOperator sum,
                        @Nullable LeftJoinDeltaExpansion outer) {
    static boolean supports(Class<?> operator) {
        return operator == DBSPJoinOperator.class || operator == DBSPJoinIndexOperator.class
                || operator == DBSPLeftJoinOperator.class || operator == DBSPLeftJoinIndexOperator.class;
    }
    @Nullable
    static BinaryJoinInputs from(@Nullable OperatorDeltaExpansion expansion) {
        if (expansion instanceof JoinDeltaExpansion join)
            return new BinaryJoinInputs(join.leftIntegrator, join.rightIntegrator,
                    List.of(join.leftDelta, join.rightDelta, join.both), join.sum, null);
        if (expansion instanceof JoinIndexDeltaExpansion join)
            return new BinaryJoinInputs(join.leftIntegrator, join.rightIntegrator,
                    List.of(join.leftDelta, join.rightDelta, join.both), join.sum, null);
        if (expansion instanceof LeftJoinDeltaExpansion join)
            return new BinaryJoinInputs(join.leftIntegrator, join.rightIntegrator,
                    List.of(join.leftDelta, join.rightDelta, join.join), join.sum, join);
        return null;
    }

    private static void projection(InsertLimiters pass, OutputPort port, ObjectNode record, String name) {
        var value = pass.expansionMonotoneValues.get(port);
        if (value != null && value.mayBeMonotone())
            record.set(name, LimiterInputs.projection(Monotonicity.getBodyType(value)));
    }

    void capture(InsertLimiters pass, ObjectNode join, ObjectNode owner) {
        var inputs = join.putArray("inputs");
        for (var integral : List.of(this.left, this.right)) {
            var entry = inputs.addObject();
            entry.put("integral_monotone", integral.annotations.first(AlwaysMonotone.class) != null);
            projection(pass, integral.input(), entry, "projection");
        }
        var terms = join.putArray("terms");
        for (var term : this.terms) {
            var entry = terms.addObject().put("id", term.id);
            projection(pass, term.left(), entry, "left");
            projection(pass, term.right(), entry, "right");
            projection(pass, term.outputPort(), entry, "output");
        }
        var order = join.putArray("sum_inputs");
        for (var port : this.sum.inputs)
            order.add(port.operator.id);
        projection(pass, this.sum.outputPort(), owner, "projection");
        if (this.outer != null) {
            var unmatched = join.putObject("unmatched");
            projection(pass, this.outer.antiJoin.outputPort(), unmatched, "output");
            unmatched.put("pad_id", this.outer.map.id);
            projection(pass, this.outer.map.outputPort(), unmatched, "padded");
        }
    }

    void recordTransfers(InsertLimiters pass, Map<Long, DBSPClosureExpression> functions) {
        for (var term : this.terms) {
            var value = pass.expansionMonotoneValues.get(term);
            if (value != null && value.mayBeMonotone())
                functions.put(term.id, value.getReducedExpression().to(DBSPClosureExpression.class));
        }
        if (this.outer != null) {
            var value = pass.expansionMonotoneValues.get(this.outer.map);
            if (value != null)
                functions.put(this.outer.map.id, value.getReducedExpression().to(DBSPClosureExpression.class));
        }
    }
}
