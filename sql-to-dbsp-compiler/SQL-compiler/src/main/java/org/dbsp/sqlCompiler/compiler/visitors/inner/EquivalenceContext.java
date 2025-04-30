package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

import javax.annotation.Nullable;
import java.util.List;

/** Context used to compare two expressions with free variables.
 * The context contains for each variable a declaration in the Scopes data structures.
 * The context also contains a leftToRight Substitution for variable renames.
 * E.g., |x| x and |y| y are equivalent.  When checking these
 * two expressions, the substitution would end up mapping x to y. */
public class EquivalenceContext {
    public final Scopes<String, IDBSPDeclaration> leftDeclaration;
    public final Scopes<String, IDBSPDeclaration> rightDeclaration;
    public final Substitution<IDBSPDeclaration, IDBSPDeclaration> leftToRight;

    public EquivalenceContext(Scopes<String, IDBSPDeclaration> leftDeclaration,
                              Scopes<String, IDBSPDeclaration> rightDeclaration,
                              Substitution<IDBSPDeclaration, IDBSPDeclaration> leftToRight) {
        this.leftDeclaration = leftDeclaration;
        this.rightDeclaration = rightDeclaration;
        this.leftToRight = leftToRight;
    }

    public EquivalenceContext() {
        this.leftDeclaration = new Scopes<>();
        this.rightDeclaration = new Scopes<>();
        this.leftToRight = new Substitution<>();
    }

    public static boolean equiv(@Nullable DBSPExpression left, @Nullable DBSPExpression right) {
        return new EquivalenceContext().equivalent(left, right);
    }

    public static boolean equiv(@Nullable DBSPAggregateList left, @Nullable DBSPAggregateList right) {
        if (left == null)
            return right == null;
        if (right == null)
            return false;
        return left.equivalent(right);
    }

    public boolean equivalent(@Nullable DBSPExpression left, @Nullable DBSPExpression right) {
        if (left == null)
            return right == null;
        if (right == null)
            return false;
        return left.equivalent(this, right);
    }

    public boolean equivalent(@Nullable DBSPExpression[] left, @Nullable DBSPExpression[] right) {
        if (left == null)
            return right == null;
        if (right == null)
            return false;
        if (left.length != right.length)
            return false;
        for (int i = 0; i < left.length; i++)
            if (!this.equivalent(left[i], right[i]))
                return false;
        return true;
    }

    public <T extends DBSPExpression> boolean equivalent(@Nullable List<T> left, @Nullable List<T> right) {
        if (left == null)
            return right == null;
        if (right == null)
            return false;
        if (left.size() != right.size())
            return false;
        for (int i = 0; i < left.size(); i++)
            if (!this.equivalent(left.get(i), right.get(i)))
                return false;
        return true;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    public EquivalenceContext clone() {
        return new EquivalenceContext(
                this.leftDeclaration.clone(),
                this.rightDeclaration.clone(),
                this.leftToRight.clone());
    }

    @Override
    public String toString() {
        return "EquivalenceContext{" +
                "leftDeclaration=" + leftDeclaration +
                ",\nrightDeclaration=" + rightDeclaration +
                ",\nleftToRight=" + leftToRight +
                "\n}";
    }
}
