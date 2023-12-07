package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MonotoneTuple extends MonotoneValue {
    /**
     * Only some fields of a tuple may be monotone.
     * This map is indexed by the original field position and gives
     * the corresponding monotone expression.
     */
    final LinkedHashMap<Integer, MonotoneValue> fields;

    public MonotoneTuple(DBSPTypeTupleBase originalType) {
        super(originalType);
        this.fields = new LinkedHashMap<>();
    }

    public MonotoneTuple(DBSPTypeTupleBase originalType, LinkedHashMap<Integer, MonotoneValue> fields) {
        super(originalType);
        this.fields = fields;
        for (Map.Entry<Integer, MonotoneValue> entry: fields.entrySet()) {
            int index = entry.getKey();
            MonotoneValue value = entry.getValue();
            DBSPType valueOriginalType = value.getOriginalType();
            assert originalType.tupFields[index].sameType(valueOriginalType) :
                "Field type mismatch " + originalType.tupFields[index] + " vs " + valueOriginalType;
        }
    }

    public void addField(Integer index, MonotoneValue value) {
        // We expect that fields are added in increasing order
        Utilities.putNew(this.fields, index, value);
    }

    public List<MonotoneValue> getFields() {
        return new ArrayList<>(this.fields.values());
    }

    @Override
    public DBSPExpression getExpression() {
        List<DBSPExpression> fields = Linq.map(this.getFields(), MonotoneValue::getExpression);
        if (this.originalType.is(DBSPTypeRawTuple.class))
            return new DBSPRawTupleExpression(fields);
        else
            return new DBSPTupleExpression(fields, false);
    }

    public boolean isEmpty() {
        return this.fields.isEmpty();
    }

    @Nullable
    public MonotoneValue field(int index) {
        return this.fields.get(index);
    }

    public ValueProjection getProjection() {
        LinkedHashMap<Integer, ValueProjection> fields =
                Utilities.mapValues(this.fields, MonotoneValue::getProjection);
        return new TupleProjection(this.originalType.to(DBSPTypeTupleBase.class), fields);
    }
}
