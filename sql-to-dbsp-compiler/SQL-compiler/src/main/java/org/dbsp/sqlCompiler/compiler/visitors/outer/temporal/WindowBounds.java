package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsBoundedType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeTypedBox;

import javax.annotation.Nullable;

record WindowBounds(
        DBSPCompiler compiler,
        @Nullable WindowBound lower,
        @Nullable WindowBound upper,
        DBSPExpression common) {
    DBSPClosureExpression makeWindow(DBSPType windowKeyType) {
        // The input has type Tup1<Timestamp>, since it is produced by scalarNow().
        DBSPType timestampType = ContainsNow.timestampType();
        DBSPVariablePath var = new DBSPTypeTuple(timestampType).ref().var();
        RewriteNow.RewriteNowExpression rn = new RewriteNow.RewriteNowExpression(
                this.compiler(), var.deref().field(0));
        // Before the first NOW value the input is the minimum timestamp.  The bounds grow
        // with NOW, so they are then the minimum, and their expressions are not evaluated.
        DBSPExpression nowKnown = ExpressionCompiler.makeBinaryExpression(CalciteObject.EMPTY,
                DBSPTypeBool.create(false), DBSPOpcode.NEQ, var.deref().field(0),
                timestampType.to(IsBoundedType.class).getMinValue());
        final DBSPExpression lowerBound, upperBound;
        CalciteObject node = CalciteObject.EMPTY;
        if (this.lower != null) {
            DBSPExpression bound = rn.apply(this.lower().expression()).to(DBSPExpression.class)
                    .nullabilityCast(windowKeyType, DBSPCastExpression.CastType.SqlUnsafe);
            node = this.lower.expression().getNode();
            lowerBound = new DBSPIfExpression(node, nowKnown, bound,
                    windowKeyType.to(IsBoundedType.class).getMinValue());
        } else {
            lowerBound = windowKeyType.to(IsBoundedType.class).getMinValue();
        }
        if (this.upper != null) {
            DBSPExpression bound = rn.apply(this.upper.expression()).to(DBSPExpression.class)
                    .nullabilityCast(windowKeyType, DBSPCastExpression.CastType.SqlUnsafe);
            if (!node.getPositionRange().isValid())
                node = this.upper.expression().getNode();
            upperBound = new DBSPIfExpression(node, nowKnown.deepCopy(), bound,
                    windowKeyType.to(IsBoundedType.class).getMinValue());
        } else {
            upperBound = windowKeyType.to(IsBoundedType.class).getMaxValue();
        }
        return new DBSPRawTupleExpression(node,
                DBSPTypeTypedBox.wrapTypedBox(lowerBound, false),
                DBSPTypeTypedBox.wrapTypedBox(upperBound, false))
                .closure(node, var);
    }

    /** If {@code common} has the shape {@code (*row).field}, returns {@code field}; otherwise returns -1. */
    int keyColumn(DBSPParameter row) {
        DBSPFieldExpression field = this.common.as(DBSPFieldExpression.class);
        if (field == null)
            return -1;
        DBSPDerefExpression deref = field.expression.as(DBSPDerefExpression.class);
        if (deref == null)
            return -1;
        DBSPVariablePath variable = deref.expression.as(DBSPVariablePath.class);
        if (variable == null || !variable.variable.equals(row.name))
            return -1;
        return field.fieldNo;
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
