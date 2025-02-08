package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;

public class RemoveIdentityOperators extends CircuitCloneVisitor {
    public RemoveIdentityOperators(DBSPCompiler compiler) {
        super(compiler, false);
    }

    /** Check whether a closure is an "identity" function for a Map or MapIndex operator.
     * There are two possible shapes we check:
     * |x| *x
     * and
     * |(&k, &v)| (*k, *v) */
    public static boolean isIdentityFunction(DBSPClosureExpression expression) {
        if (expression.parameters.length != 1)
            return false;
        // After some fast negative checks we compare equivalence with
        // an identity function of the appropriate type.
        DBSPType paramType = expression.parameters[0].getType();
        if (paramType.is(DBSPTypeTuple.class)) {
            if (!paramType.is(DBSPTypeRef.class) ||
                    !paramType.deref().sameType(expression.getResultType()))
                return false;
            DBSPVariablePath var = paramType.var();
            DBSPTypeTupleBase tuple = paramType.deref().as(DBSPTypeTupleBase.class);
            DBSPClosureExpression id;
            if (tuple != null) {
                id = new DBSPTupleExpression(DBSPTypeTupleBase.flatten(var.deref()), tuple.mayBeNull).closure(var);
            } else {
                id = var.deref().applyCloneIfNeeded().closure(var);
            }
            return EquivalenceContext.equiv(expression, id);
        } else if (paramType.is(DBSPTypeRawTuple.class)) {
            DBSPTypeRawTuple raw = paramType.to(DBSPTypeRawTuple.class);
            if (raw.size() != 2) {
                return false;
            }
            if (!raw.tupFields[0].is(DBSPTypeRef.class) || !raw.tupFields[1].is(DBSPTypeRef.class))
                return false;
            if (!expression.getResultType().is(DBSPTypeRawTuple.class))
                return false;
            DBSPVariablePath var = paramType.var();
            DBSPClosureExpression id = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(DBSPTypeTupleBase.flatten(var.field(0).deref()), false),
                    new DBSPTupleExpression(DBSPTypeTupleBase.flatten(var.field(1).deref()), false)
            ).closure(var);
            return EquivalenceContext.equiv(expression, id);
        } else {
            return false;
        }
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        if (operator.function == null || !operator.function.is(DBSPClosureExpression.class)) {
            super.postorder(operator);
            return;
        }

        DBSPClosureExpression function = operator.getClosureFunction();
        if (isIdentityFunction(function)) {
            OutputPort input = this.mapped(operator.input());
            this.map(operator.outputPort(), input, false);
            return;
        }

        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        if (operator.function == null || !operator.function.is(DBSPClosureExpression.class)) {
            super.postorder(operator);
            return;
        }

        DBSPClosureExpression function = operator.getClosureFunction();
        if (isIdentityFunction(function)) {
            OutputPort input = this.mapped(operator.input());
            this.map(operator.outputPort(), input, false);
            return;
        }

        super.postorder(operator);
    }
}
