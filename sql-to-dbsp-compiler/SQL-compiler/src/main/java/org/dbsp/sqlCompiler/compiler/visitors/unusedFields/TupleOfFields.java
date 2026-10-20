package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import java.util.List;
import java.util.Objects;

/**
 * Represents a tuple of field uses: a separate use for each of the fields of a tuple.
 */
public final class TupleOfFields extends IUsedFields {
    private final List<IUsedFields> fields;

    public TupleOfFields(List<IUsedFields> fields) {
        this.fields = fields;
    }

    @Override
    void markParameterUse(ParameterFieldUse use) {
        for (var u : this.fields)
            u.markParameterUse(use);
    }

    @Override
    public String toString() {
        return this.fields.toString();
    }

    public List<IUsedFields> fields() {
        return fields;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (TupleOfFields) obj;
        return Objects.equals(this.fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

}
