package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;

import java.util.List;
import java.util.function.Predicate;

/** Instrument a plan by inserting code to write dump the contents after
 * chosen plan stages. */
public class InstrumentDump extends CircuitCloneVisitor {
    /** When this predicate returns true, the corresponding operator
     * is instrumented with a dump. */
    public final Predicate<DBSPSimpleOperator> instrument;

    public InstrumentDump(DBSPCompiler compiler, Predicate<DBSPSimpleOperator> instrument) {
        super(compiler, false);
        this.instrument = instrument;
    }

    public void instrument(DBSPSimpleOperator operator) {
        if (operator.is(DBSPSinkOperator.class)) {
            super.replace(operator);
            return;
        }
        DBSPType type = operator.getType();
        if (!type.is(DBSPTypeZSet.class)) {
            super.replace(operator);
            return;
        }
        if (!this.instrument.test(operator)) {
            super.replace(operator);
            return;
        }

        List<OutputPort> inputs = Linq.map(operator.inputs, this::mapped);
        DBSPSimpleOperator input = operator.withInputs(inputs, false);
        this.addOperator(input);
        DBSPTypeZSet zset = type.to(DBSPTypeZSet.class);
        DBSPVariablePath row = new DBSPVariablePath(zset.elementType.ref());
        DBSPExpression dump = new DBSPApplyExpression(operator.getNode(), "dump", zset.elementType,
                new DBSPStringLiteral(Long.toString(operator.id)), row);
        DBSPExpression function = dump.closure(row.asParameter());
        DBSPSimpleOperator map = new DBSPMapOperator(operator.getRelNode(), function, zset, input.outputPort());
        this.map(operator, map);
    }

    @Override
    public void postorder(DBSPMapOperator map) {
        this.instrument(map);
    }

    @Override
    public void postorder(DBSPSumOperator sum) { this.instrument(sum); }

    @Override
    public void postorder(DBSPStreamDistinctOperator distinct) { this.instrument(distinct); }

    @Override
    public void postorder(DBSPSubtractOperator sub) { this.instrument(sub); }

    @Override
    public void postorder(DBSPStreamJoinOperator join) { this.instrument(join); }
}
