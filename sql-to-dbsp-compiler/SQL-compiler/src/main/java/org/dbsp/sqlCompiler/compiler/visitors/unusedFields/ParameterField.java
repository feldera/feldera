package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents field accesses in a parameter: param.i0.i1. ... .in.
 * 'param' is the parameter, while 'indexes' is the list of indexes.
 * If the list of indexes is empty, this represents the whole parameter.
 */
public final class ParameterField extends IUsedFields {
    private final DBSPParameter param;
    private final List<Integer> indexes;

    public ParameterField(DBSPParameter param, List<Integer> indexes) {
        this.param = param;
        this.indexes = indexes;
    }

    public ParameterField field(int index) {
        List<Integer> indexes = new ArrayList<>(this.indexes);
        indexes.add(index);
        return new ParameterField(this.param, indexes);
    }

    @Override
    public ParameterFieldUse getParameterUse() {
        ParameterFieldUse result = new ParameterFieldUse();
        DBSPType paramType = param.getType();
        DBSPTypeRawTuple raw = paramType.as(DBSPTypeRawTuple.class);
        boolean isRaw = raw != null && raw.size() == 2 &&
                raw.tupFields[0].is(DBSPTypeRef.class) &&
                raw.tupFields[1].is(DBSPTypeRef.class);
        boolean isRef = param.getType().is(DBSPTypeRef.class);
        FieldUseMap whole;
        FieldUseMap first = null;
        FieldUseMap second = null;
        // This is actually a heuristic recognizing various type patterns for parameters
        // and adjusting the field use.
        // - &T
        // - (&left, &right) (a raw tuple)
        // - anything else
        if (isRef) {
            whole = new FieldUseMap(param.getType(), false).deref();
        } else if (isRaw) {
            var tuple = new FieldUseMap(param.getType(), false);
            first = tuple.field(0).deref();
            second = tuple.field(1).deref();
            whole = FieldUseMap.list(param.getType().to(DBSPTypeRawTuple.class),
                    List.of(first, second));
        } else {
            whole = new FieldUseMap(param.getType(), false);
        }
        FieldUseMap fu = whole;
        for (int i : this.indexes) {
            if (fu.getType().is(DBSPTypeNull.class))
                // This can happen e.g., for a parameter with type Tup2<i64, null>
                break;
            fu = fu.field(i);
        }
        // Mutate field of whole
        fu.setUsed();
        if (isRef) {
            result.set(this.param, whole.borrow());
        } else if (isRaw) {
            result.set(this.param, FieldUseMap.list(param.getType().to(DBSPTypeRawTuple.class),
                    List.of(first.borrow(), second.borrow())));
        } else {
            result.set(this.param, whole);
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.param.name);
        for (int i : this.indexes)
            builder.append(".").append(i);
        return builder.toString();
    }

    public DBSPParameter param() {
        return param;
    }

    public List<Integer> indexes() {
        return indexes;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ParameterField) obj;
        return Objects.equals(this.param, that.param) &&
                Objects.equals(this.indexes, that.indexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(param, indexes);
    }
}
