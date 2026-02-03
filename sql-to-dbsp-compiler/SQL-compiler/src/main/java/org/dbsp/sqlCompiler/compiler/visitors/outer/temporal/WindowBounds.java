package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsBoundedType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeTypedBox;

import javax.annotation.Nullable;

record WindowBounds(
        DBSPCompiler compiler,
        @Nullable WindowBound lower,
        @Nullable WindowBound upper,
        DBSPExpression common) {
    DBSPClosureExpression makeWindow() {
        DBSPType type = ContainsNow.timestampType();
        // The input has type Tup1<Timestamp>
        DBSPVariablePath var = new DBSPTypeTuple(type).ref().var();
        RewriteNow.RewriteNowExpression rn = new RewriteNow.RewriteNowExpression(this.compiler(), var.deref().field(0));
        final DBSPExpression lowerBound, upperBound;
        CalciteObject node = CalciteObject.EMPTY;
        if (this.lower != null) {
            lowerBound = rn.apply(this.lower().expression()).to(DBSPExpression.class);
            node = this.lower.expression().getNode();
        } else {
            lowerBound = type.to(IsBoundedType.class).getMinValue();
        }
        if (this.upper != null) {
            upperBound = rn.apply(this.upper.expression()).to(DBSPExpression.class);
            if (!node.getPositionRange().isValid())
                node = this.upper.expression().getNode();
        } else {
            upperBound = type.to(IsBoundedType.class).getMaxValue();
        }
        return new DBSPRawTupleExpression(node,
                DBSPTypeTypedBox.wrapTypedBox(lowerBound, false),
                DBSPTypeTypedBox.wrapTypedBox(upperBound, false))
                .closure(node, var);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.common).append(" in ");
        if (this.lower == null)
            builder.append("(*");
        else
            builder.append(this.lower.inclusive() ? "[" : "(")
                    .append(this.lower);
        builder.append(", ");
        if (this.upper == null)
            builder.append("*)");
        else
            builder.append(this.upper)
                    .append(this.upper.inclusive() ? "]" : ")");
        return builder.toString();
    }
}
