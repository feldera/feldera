package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import java.util.List;

/** A Join operator that has N inputs; each input is indexed and all keys must be of the same type;
 * this operator is incremental-only. */
public abstract class DBSPStarJoinBaseOperator extends DBSPSimpleOperator implements IJoin, IIncremental {
    protected DBSPStarJoinBaseOperator(
            CalciteRelNode node, String operation,
            DBSPType outputType, DBSPClosureExpression function,
            boolean isMultiset, List<OutputPort> inputs) {
        super(node, operation, function, outputType, isMultiset);
        Utilities.enforce(inputs.size() > 1);
        Utilities.enforce(inputs.size() == function.parameters.length - 1);
        DBSPType keyType = inputs.get(0).outputType().to(DBSPTypeIndexedZSet.class).keyType;
        for (OutputPort input: inputs) {
            this.addInput(input);
            Utilities.enforce(input.outputType().is(DBSPTypeIndexedZSet.class));
            Utilities.enforce(input.getOutputIndexedZSetType().keyType.sameType(keyType));
        }
    }

    /** Replace inputs and function, preserve output type */
    public final DBSPStarJoinBaseOperator withFunctionAndInputs(
            DBSPExpression function, List<OutputPort> inputs) {
        return this.with(function, this.outputType, inputs, false)
                .to(DBSPStarJoinBaseOperator.class);
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!other.is(DBSPStarJoinBaseOperator.class))
            return false;
        DBSPStarJoinBaseOperator o = other.to(DBSPStarJoinBaseOperator.class);
        return super.equivalent(o);
    }
}
