package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;

import javax.annotation.Nullable;
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

    /** The parameter type if it is a raw tuple of two references, null otherwise */
    @Nullable
    static DBSPTypeRawTuple rawPairOfRefs(DBSPType type) {
        DBSPTypeRawTuple raw = type.as(DBSPTypeRawTuple.class);
        if (raw != null && raw.size() == 2 &&
                raw.tupFields[0].is(DBSPTypeRef.class) &&
                raw.tupFields[1].is(DBSPTypeRef.class))
            return raw;
        return null;
    }

    /** A use map of the parameter with all fields unused.  The shape depends on the parameter type:
     * &T is a reference to the map of T,
     * (&left, &right) is a raw tuple of two references,
     * any other type is mapped directly. */
    static FieldUseMap allUnused(DBSPParameter param) {
        DBSPType paramType = param.getType();
        if (paramType.is(DBSPTypeRef.class))
            return new FieldUseMap(paramType, false).deref().borrow();
        DBSPTypeRawTuple raw = rawPairOfRefs(paramType);
        if (raw != null) {
            FieldUseMap tuple = new FieldUseMap(paramType, false);
            return FieldUseMap.list(raw, List.of(
                    tuple.field(0).deref().borrow(),
                    tuple.field(1).deref().borrow()));
        }
        return new FieldUseMap(paramType, false);
    }

    @Override
    void markParameterUse(ParameterFieldUse use) {
        FieldUseMap stored = use.getOrAdd(this.param, ParameterField::allUnused);
        DBSPType paramType = this.param.getType();
        DBSPTypeRawTuple raw = rawPairOfRefs(paramType);
        FieldUseMap fu;
        if (paramType.is(DBSPTypeRef.class)) {
            fu = stored.deref();
        } else if (raw != null) {
            fu = FieldUseMap.list(raw, List.of(stored.field(0).deref(), stored.field(1).deref()));
        } else {
            fu = stored;
        }
        for (int i : this.indexes) {
            if (fu.getType().is(DBSPTypeNull.class))
                // This can happen e.g., for a parameter with type Tup2<i64, null>
                break;
            fu = fu.field(i);
        }
        fu.setUsed();
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
