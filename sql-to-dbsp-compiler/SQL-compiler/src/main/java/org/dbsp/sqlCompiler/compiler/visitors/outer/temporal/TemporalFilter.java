package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.util.Linq;

/**
 * A Boolean expression that is a comparison involving now() that can be implemented as a temporal filter.
 *
 * @param parameter Parameter of the original filter comparison function
 * @param noNow     left expression, which does not involve now.
 * @param withNow   right expression, which must be a monotone expression of now(),
 *                  and which does not involve other fields
 * @param opcode    comparison
 */
record TemporalFilter(DBSPParameter parameter, DBSPExpression noNow,
                      DBSPExpression withNow, DBSPOpcode opcode)
        implements BooleanExpression {
    @Override
    public boolean compatible(BooleanExpression other) {
        TemporalFilter o = other.as(TemporalFilter.class);
        if (o == null)
            return false;

        // To be compatible the operations must be on different sides, or have the same inclusivity
        boolean thisGe = RewriteNow.isGreater(this.opcode);
        boolean otherGe = RewriteNow.isGreater(o.opcode);
        if (thisGe == otherGe) {
            boolean thisInclusive = RewriteNow.isInclusive(this.opcode);
            boolean otherInclusive = RewriteNow.isInclusive(o.opcode);
            if (thisInclusive != otherInclusive)
                return false;
        }

        EquivalenceContext context = new EquivalenceContext();
        context.leftDeclaration.newContext();
        context.rightDeclaration.newContext();
        context.leftDeclaration.substitute(this.parameter.name, this.parameter);
        context.rightDeclaration.substitute(this.parameter.name, this.parameter);
        context.leftToRight.put(this.parameter, this.parameter);
        return context.equivalent(this.noNow, o.noNow);
    }

    @Override
    public BooleanExpression combine(BooleanExpression other) {
        return new TemporalFilterList(Linq.list(this, other.to(TemporalFilter.class)));
    }

    public BooleanExpression seal() {
        // return a singleton list
        return new TemporalFilterList(Linq.list(this));
    }

    @Override
    public boolean isNullable() {
        return this.noNow.getType().mayBeNull || this.withNow.getType().mayBeNull;
    }
}
