package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Lineage;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.IHasPrecision;

/** A lineage interpreter in which a lossless cast preserves the lineage of its source, in
 * addition to the casts that {@link Lineage.InnerLineage} accepts.  A cast is lossless when it
 * changes neither the value nor how it compares: a non-fixed string cast to an equal, larger, or
 * unlimited precision, or any cast between integer types, which raises an error for a value the
 * target cannot hold rather than changing it.  Two values with the same lineage are therefore
 * equal, but need not have the same type: a column with the lineage of another column may be a
 * wider copy of it.  Every other cast stays opaque, for the reasons {@link Lineage.InnerLineage}
 * gives. */
class LosslessCastLineage extends Lineage.InnerLineage {
    LosslessCastLineage(DBSPCompiler compiler) {
        super(compiler, null);
    }

    /** True if casting {@code from} to {@code to} changes neither the value nor its comparison. */
    static boolean isLosslessCast(DBSPType from, DBSPType to) {
        if (from.is(DBSPTypeString.class) && to.is(DBSPTypeString.class)) {
            DBSPTypeString source = from.to(DBSPTypeString.class);
            DBSPTypeString target = to.to(DBSPTypeString.class);
            if (source.fixed || target.fixed)
                return false;
            if (target.precision == IHasPrecision.UNLIMITED_PRECISION)
                return true;
            return source.precision != IHasPrecision.UNLIMITED_PRECISION
                    && target.precision >= source.precision;
        }
        // An integer cast fails on a value the target cannot hold instead of altering it
        return from.is(DBSPTypeInteger.class) && to.is(DBSPTypeInteger.class);
    }

    /** A lossless cast carries the lineage of its source; the strict rule decides the rest. */
    @Override
    public void postorder(DBSPCastExpression expression) {
        if (isLosslessCast(expression.source.getType(), expression.getType())) {
            this.set(expression, this.get(expression.source));
            return;
        }
        super.postorder(expression);
    }
}
