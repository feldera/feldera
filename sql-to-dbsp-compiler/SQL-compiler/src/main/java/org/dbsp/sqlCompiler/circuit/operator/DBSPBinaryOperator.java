package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

/** Base class for all DBSP query operators that have two inputs. */
public abstract class DBSPBinaryOperator extends DBSPSimpleOperator {
    protected DBSPBinaryOperator(CalciteRelNode node, String operation,
                                 @Nullable DBSPExpression function, DBSPType outputType,
                                 boolean isMultiset, OutputPort left, OutputPort right,
                                 boolean containsIntegrator) {
        super(node, operation, function, outputType, isMultiset, containsIntegrator);
        this.addInput(left);
        this.addInput(right);
    }

    /** The first input of this operator. */
    public OutputPort left() {
        return this.inputs.get(0);
    }

    /** The second input of this operator. */
    public OutputPort right() {
        return this.inputs.get(1);
    }
}
