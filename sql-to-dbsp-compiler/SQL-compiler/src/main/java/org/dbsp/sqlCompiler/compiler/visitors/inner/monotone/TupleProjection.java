package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Represents a list of fields from a tuple type. */
public class TupleProjection extends ValueProjection {
    public final LinkedHashMap<Integer, ValueProjection> columns;

    public TupleProjection(DBSPTypeTupleBase type, LinkedHashMap<Integer, ValueProjection> columns) {
        super(type);
        this.columns = columns;
        for (int i: columns.keySet()) {
            assert i < type.size(): "Index " + i + " out of bounds for type " + type;
        }
    }

    @Nullable
    public ValueProjection field(int i) {
        return this.columns.get(i);
    }

    public static TupleProjection empty(DBSPTypeTuple type) {
        return new TupleProjection(type, new LinkedHashMap<>());
    }

    @Override
    public DBSPType getProjectedType() {
        List<DBSPType> fields = new ArrayList<>();
        for (Map.Entry<Integer, ValueProjection> column: this.columns.entrySet()) {
            DBSPType type = column.getValue().getProjectedType();
            fields.add(type);
        }
        return this.type.to(DBSPTypeTupleBase.class).makeType(fields);
    }

    @Override
    public MonotoneValue createInput(DBSPExpression expression) {
        assert expression.getType().sameType(this.getProjectedType()):
            "Types differ " + expression.getType() + " and " + this.getProjectedType();
        LinkedHashMap<Integer, MonotoneValue> fields = new LinkedHashMap<>();
        int index = 0;
        for (Map.Entry<Integer, ValueProjection> entry: this.columns.entrySet()) {
            MonotoneValue monotoneValue = entry.getValue().createInput(expression.field(index));
            Utilities.putNew(fields, entry.getKey(), monotoneValue);
            index++;
        }
        return new MonotoneTuple(this.type.to(DBSPTypeTupleBase.class), fields);
    }

    public DBSPExpression project(DBSPExpression expression) {
        DBSPExpression[] fields = new DBSPExpression[this.columns.size()];
        int index = 0;
        for (Map.Entry<Integer, ValueProjection> entry: this.columns.entrySet()) {
            int i = entry.getKey();
            DBSPExpression subExpression = entry.getValue().project(expression.field(i));
            fields[index++] = subExpression;
        }
        if (this.isRaw())
            return new DBSPRawTupleExpression(fields);
        return new DBSPTupleExpression(fields);
    }

    public boolean isRaw() {
        return this.type.is(DBSPTypeRawTuple.class);
    }

    @Override
    public boolean isEmpty() {
        return this.columns.isEmpty();
    }

    @Override
    public String toString() {
        return this.id + ": " + this.type + "/" +
                String.join(", ", Linq.map(this.columns.keySet(), Object::toString));
    }
}
