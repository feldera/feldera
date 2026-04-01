package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import java.util.Objects;

/**
 * A field of the form &param.i0.i1....in
 */
public final class BorrowedField extends IUsedFields {
    private final ParameterField field;

    public BorrowedField(ParameterField field) {
        this.field = field;
    }

    @Override
    public ParameterFieldUse getParameterUse() {
        return this.field.getParameterUse();
    }

    @Override
    public String toString() {
        return "&" + this.field;
    }

    public ParameterField field() {
        return field;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (BorrowedField) obj;
        return Objects.equals(this.field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field);
    }
}
