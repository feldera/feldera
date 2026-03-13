package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a set of fields of a parameter that are "used" for computing an expression.
 */
public final class FieldSet extends IUsedFields {
    /**
     * The empty set of fields: e.g., constant expressions
     */
    static final FieldSet EMPTY = new FieldSet(new HashSet<>());
    private final Set<IUsedFields> used;

    public FieldSet(Set<IUsedFields> used) {
        this.used = used;
    }

    /**
     * Given a set of field uses, returns usually a {@link FieldSet} containing all.
     * However, if `fields` is empty, it returns the EMPTY set.
     *
     * @param fields Set of fields that may all be referenced to compute the current expression.
     */
    public static IUsedFields union(Set<IUsedFields> fields) {
        if (fields.isEmpty())
            return EMPTY;
        Set<IUsedFields> u = new HashSet<>();
        for (IUsedFields f : fields) {
            if (f.is(FieldSet.class)) {
                u.addAll(f.to(FieldSet.class).used);
            } else {
                u.add(f);
            }
        }
        if (u.isEmpty())
            return EMPTY;
        return new FieldSet(u);
    }

    boolean isEmpty() {
        return this.used.isEmpty();
    }

    @Override
    public ParameterFieldUse getParameterUse() {
        if (this.isEmpty())
            return new ParameterFieldUse();
        ParameterFieldUse result = null;
        for (var u : this.used) {
            ParameterFieldUse next = u.getParameterUse();
            if (result == null)
                result = next;
            else
                result.union(next);
        }
        if (result == null)
            return new ParameterFieldUse();
        return result;
    }

    public Set<IUsedFields> used() {
        return used;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (FieldSet) obj;
        return Objects.equals(this.used, that.used);
    }

    @Override
    public int hashCode() {
        return Objects.hash(used);
    }

    @Override
    public String toString() {
        return "FieldSet[" +
                "used=" + used + ']';
    }

}
