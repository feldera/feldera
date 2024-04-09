package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** A tuple type where the monotonicity is tracked per field */
public class PartiallyMonotoneTuple implements IMaybeMonotoneType {
    final List<IMaybeMonotoneType> fields;
    final boolean raw;
    final DBSPTypeTupleBase type;
    final boolean anyMonotone;

    public PartiallyMonotoneTuple(List<IMaybeMonotoneType> fields, boolean raw) {
        this.fields = fields;
        this.raw = raw;
        List<DBSPType> fieldTypes = Linq.map(fields, IMaybeMonotoneType::getType);
        this.anyMonotone = Linq.any(fields, IMaybeMonotoneType::mayBeMonotone);
        if (raw)
            this.type = new DBSPTypeRawTuple(CalciteObject.EMPTY, fieldTypes);
        else
            this.type = new DBSPTypeTuple(CalciteObject.EMPTY, fieldTypes);
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }

    @Override
    @Nullable
    public DBSPType getProjectedType() {
        if (!this.mayBeMonotone())
            return null;
        List<IMaybeMonotoneType> monotone = Linq.where(this.fields, IMaybeMonotoneType::mayBeMonotone);
        List<DBSPType> fields = Linq.map(monotone, IMaybeMonotoneType::getProjectedType);
        if (this.raw)
            return new DBSPTypeRawTuple(CalciteObject.EMPTY, fields);
        else
            return new DBSPTypeTuple(CalciteObject.EMPTY, fields);
    }

    @Override
    public boolean mayBeMonotone() {
        return this.anyMonotone;
    }

    @Override
    public DBSPExpression projectExpression(DBSPExpression source) {
        List<DBSPExpression> results = new ArrayList<>();
        int index = 0;
        for (IMaybeMonotoneType type: this.fields) {
            if (type.mayBeMonotone()) {
                results.add(type.projectExpression(source.field(index)));
            }
            index++;
        }
        if (this.raw)
            return new DBSPRawTupleExpression(results);
        else
            return new DBSPTupleExpression(source.getNode(), results);
    }

    public IMaybeMonotoneType getField(int index) {
        return this.fields.get(index);
    }

    /** Given an index in the original type, return an index into a type
     * that contains only the components that may be monotone. */
    public int compressedIndex(int fieldNo) {
        int result = 0;
        for (int i = 0; i < fieldNo; i++) {
            IMaybeMonotoneType field = this.getField(i);
            if (field.mayBeMonotone())
                result++;
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("(");
        for (IMaybeMonotoneType field: this.fields) {
            result.append(field.toString());
            result.append(", ");
        }
        result.append(")");
        return result.toString();
    }
}
