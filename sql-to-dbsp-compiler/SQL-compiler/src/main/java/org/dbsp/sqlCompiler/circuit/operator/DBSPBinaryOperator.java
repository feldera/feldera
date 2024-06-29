package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

/** Base class for all DBSP query operators that have two inputs. */
public abstract class DBSPBinaryOperator extends DBSPOperator {
    protected DBSPBinaryOperator(CalciteObject node, String operation,
                                 @Nullable DBSPExpression function, DBSPType outputType,
                                 boolean isMultiset, DBSPOperator left, DBSPOperator right) {
        super(node, operation, function, outputType, isMultiset);
        this.addInput(left);
        this.addInput(right);
    }

    /** The first input of this operator. */
    public DBSPOperator left() {
        return this.inputs.get(0);
    }

    /** The second input of this operator. */
    public DBSPOperator right() {
        return this.inputs.get(1);
    }
}
