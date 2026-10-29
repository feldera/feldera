package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Lineage;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPReturnExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.IHasPrecision;

/** A lineage interpreter in which a lossless cast preserves the lineage of its source, in
 * addition to the casts that {@link Lineage.InnerLineage} accepts.  Two values with the same
 * lineage are therefore equal, but need not have the same type: a column with the lineage of
 * another column may be a wider copy of it. */
class LosslessCastLineage extends Lineage.InnerLineage {
    /** If true, a return or an assignment in the closure is an internal error.  Otherwise the
     * interpreter sets {@link #foundUnhandled} and gives the expression an unknown value. */
    final boolean throwOnUnhandled;
    /** True if the closure contains a return or an assignment */
    boolean foundUnhandled = false;

    LosslessCastLineage(DBSPCompiler compiler, boolean throwOnUnhandled) {
        super(compiler, null);
        this.throwOnUnhandled = throwOnUnhandled;
    }

    void unhandled(DBSPExpression expression) {
        this.foundUnhandled = true;
        if (this.getN(expression) == null)
            this.set(expression, Lineage.Unknown.INSTANCE);
    }

    @Override
    public void postorder(DBSPReturnExpression expression) {
        if (this.throwOnUnhandled)
            super.postorder(expression);
        else
            this.unhandled(expression);
    }

    @Override
    public void postorder(DBSPAssignmentExpression expression) {
        if (this.throwOnUnhandled)
            super.postorder(expression);
        else
            this.unhandled(expression);
    }

    /** True if {@code to} holds every value of {@code from}. */
    static boolean holdsAllValues(DBSPTypeInteger from, DBSPTypeInteger to) {
        if (from.signed == to.signed)
            return to.getWidth() >= from.getWidth();
        // An unsigned type holds no negative value
        return to.signed && to.getWidth() > from.getWidth();
    }

    /** True if {@code cast} changes neither the value nor how it compares: a non-fixed string
     * cast to a VARCHAR with unlimited precision, or a cast between integer types.
     * A cast to a VARCHAR with a precision truncates, and a value can be longer than the
     * precision of its type: UPPER('ß') is 'SS', but UPPER keeps the type of its argument.
     * An unsafe integer cast raises an error for a value the target cannot hold rather than
     * changing it.  A safe cast returns NULL for such a value, so a safe integer cast is
     * lossless only if the target holds every value of the source. */
    static boolean isLosslessCast(DBSPCastExpression cast) {
        DBSPType from = cast.source.getType();
        DBSPType to = cast.getType();
        if (from.is(DBSPTypeString.class) && to.is(DBSPTypeString.class)) {
            DBSPTypeString source = from.to(DBSPTypeString.class);
            DBSPTypeString target = to.to(DBSPTypeString.class);
            return !source.fixed && !target.fixed
                    && target.precision == IHasPrecision.UNLIMITED_PRECISION;
        }
        if (!from.is(DBSPTypeInteger.class) || !to.is(DBSPTypeInteger.class))
            return false;
        return !cast.safe.isSafe()
                || holdsAllValues(from.to(DBSPTypeInteger.class), to.to(DBSPTypeInteger.class));
    }

    /** A lossless cast carries the lineage of its source.  Every other cast follows the strict
     * rule of {@link Lineage.InnerLineage}: it keeps the lineage only if the cast changes
     * nothing but nullability. */
    @Override
    public void postorder(DBSPCastExpression expression) {
        if (isLosslessCast(expression)) {
            this.set(expression, this.get(expression.source));
            return;
        }
        super.postorder(expression);
    }
}
