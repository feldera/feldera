package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;

/** Visitor tailored for optimizing the functions in Apply nodes produced by
 * the InsertLimiters pass.  These nodes have the closures of the shape:
 * clo = |v: (bool, T) -> expression.  These expressions are optimized using the following
 * rewrite:
 *
 * <p>
 * |v: (bool, T) -> if v.0 { clo((true, v.1)) } else { clo((false, v.1)) }
 */
public class SimplifyWaterline extends Simplify {
    public SimplifyWaterline(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public void postorder(DBSPClosureExpression expression) {
        DBSPExpression body = this.getE(expression.body);
        DBSPClosureExpression transformed = body.closure(expression.parameters);

        DBSPExpression result = transformed;
        if (transformed.parameters.length != 1) {
            this.map(expression, result);
            return;
        }
        DBSPType type = transformed.parameters[0].getType();
        if (!type.is(DBSPTypeRef.class)) {
            this.map(expression, result);
            return;
        }
        DBSPTypeTupleBase tuple = type.deref().as(DBSPTypeTupleBase.class);
        if (tuple == null || tuple.size() != 2) {
            this.map(expression, result);
            return;
        }

        if (!tuple.tupFields[0].is(DBSPTypeBool.class) || tuple.tupFields[0].mayBeNull) {
            this.map(expression, result);
            return;
        }

        DBSPVariablePath var = type.var();
        DBSPExpression ifTrue = transformed.call(
                tuple.makeTuple(new DBSPBoolLiteral(true), var.deref().field(1)).borrow())
                .reduce(this.compiler);
        DBSPExpression ifFalse = transformed.call(
                tuple.makeTuple(new DBSPBoolLiteral(false), var.deref().field(1)).borrow())
                .reduce(this.compiler);
        result = new DBSPIfExpression(expression.getNode(),
                var.deref().field(0), ifTrue, ifFalse).closure(var);
        this.map(expression, result);
    }
}
