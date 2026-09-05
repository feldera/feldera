package org.dbsp.sqlCompiler.compiler.frontend;

import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FieldUseMap;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Keeps track of the fields of a tuple that are used as key fields.
 * Note that key fields may be duplicated.
 * E.g., a key may contain fields 1, 3, and 1 of a tuple of size 5, in this order.
 * [_, 0 and 2, _, 1, _]. */
public class KeyFields {
    /** Indexes of fields that belong to the key, in the order they appear in the key: [1, 3, 1] */
    private final List<Integer> keyFields;
    private final Set<Integer> keyFieldSet;

    public KeyFields() {
        this.keyFieldSet = new HashSet<>();
        this.keyFields = new ArrayList<>();
    }

    public KeyFields(KeyFields other) {
        this.keyFieldSet = new HashSet<>(other.keyFieldSet);
        this.keyFields = new ArrayList<>(other.keyFields);
    }

    public boolean isKeyField(int i) {
        return this.keyFieldSet.contains(i);
    }

    /** Indexes of the key fields, in the order they appear in the key. */
    public List<Integer> keyFieldIndexes() {
        return Collections.unmodifiableList(this.keyFields);
    }

    /** Call this function to add key fields in the order they appear in the key */
    public void add(int index) {
        this.keyFieldSet.add(index);
        this.keyFields.add(index);
    }

    public void addAll(List<Integer> indexes) {
        for (int i: indexes)
            this.add(i);
    }

    public void addAllUsed(@Nullable FieldUseMap map) {
        if (map == null)
            return;
        if (map.isRef())
            map = map.deref();
        this.addAll(map.getUsedFields());
    }

    /** Extract the non-key fields from a value into a tuple, in the order they appear in the tuple.
     * For our example, the result will be [e.0, e.2, e.4]
     *
     * @param e Expression that is a reference to a tuple.
     * @return  A tuple containing just the non-key fields, in order.
     */
    public DBSPTupleExpression nonKeyFields(DBSPExpression e) {
        // Almost like nonKeyFields with 3 arguments, except we don't insert casts ever
        // return this.nonKeyFields(var, e.getType().to(DBSPTypeTupleBase.class), 0);
        List<DBSPExpression> fields = new ArrayList<>();
        DBSPTypeTupleBase tuple = e.getType().to(DBSPTypeTupleBase.class);
        for (int i = 0; i < tuple.size(); i++) {
            if (this.isKeyField(i))
                continue;
            fields.add(e.field(i).applyCloneIfNeeded());
        }
        return new DBSPTupleExpression(fields, false);
    }

    /** Extract the non-key fields from a value into a tuple, and insert casts if needed.
     * For our example, the result will be [(desiredType[0])e.0, (desiredType[1])e.2, (desiredType[2])e.4]
     *
     * @param e        Expression that is a reference to a tuple.
     * @param desiredType Type to cast resulting tuple to.
     * @return          A tuple containing just the non-key fields, in order.
     */
    public DBSPTupleExpression nonKeyFields(DBSPExpression e, DBSPTypeTupleBase desiredType) {
        List<DBSPExpression> fields = new ArrayList<>();
        Utilities.enforce(e.getType().getToplevelFieldCount() >= desiredType.size());
        for (int i = 0; i < desiredType.size(); i++) {
            if (this.isKeyField(i))
                continue;
            fields.add(e.field(i)
                    .applyCloneIfNeeded()
                    .nullabilityCast(desiredType.getFieldType(i), DBSPCastExpression.CastType.SqlUnsafe));
        }
        return new DBSPTupleExpression(fields, false);
    }

    /** Extract the key fields from a value (e), in the order they have to appear in the key.
     * For our example the result will be [(keyType[0])e.1, (keyType[1])e.3, (keyType[2])e.1]
     *
     * @param e       Expression that is a reference to a tuple.
     * @param keyType Type for the key fields in the result; may introduce nullability casts.
     * @return A tuple containing just the key fields, in order.
     */
    public DBSPTupleExpression keyFields(DBSPExpression e, DBSPTypeTupleBase keyType) {
        List<DBSPExpression> fields = new ArrayList<>();
        Utilities.enforce(keyType.size() == this.size());
        int index = 0;
        for (int kf: this.keyFields) {
            fields.add(e.field(kf)
                    .applyCloneIfNeeded()
                    .nullabilityCast(keyType.getFieldType(index), DBSPCastExpression.CastType.SqlUnsafe));
            index++;
        }
        return new DBSPTupleExpression(fields, false);
    }

    /** Given a type for the row, return the type for the key */
    public DBSPTypeTuple keyType(DBSPTypeTupleBase row) {
        List<DBSPType> fields = new ArrayList<>();
        for (int kf: this.keyFields) {
            fields.add(row.getFieldType(kf));
        }
        return new DBSPTypeTuple(fields);
    }

    /**
     * Produce a list with original fields from two values: the key value, and the data value.
     *
     * @param key       Expression containing the key fields, in order.
     * @param data      Expression containing the data fields, in order.
     * @param result    A list of the unshuffled fields.  The result is
     *                  [data.0, key.0, data.1, key.1, data.2]
     *                  (In this case we expect key.0 == key.2 always)
     */
    public void unshuffleKeyAndDataFields(DBSPExpression key, DBSPExpression data, List<DBSPExpression> result) {
        Utilities.enforce(key.getType().getToplevelFieldCount() == this.size());
        int size = this.keyFieldSet.size() + data.getType().getToplevelFieldCount();
        int skipped = 0;
        boolean nullableData = data.getType().deref().mayBeNull;
        for (int i = 0; i < size; i++) {
            @Nullable DBSPExpression checkNull = nullableData ? data.deref().is_null() : null;
            if (this.isKeyField(i)) {
                int index = 0;
                for (int ki: this.keyFields) {
                    // There could be multiple positions, stop at the first one
                    if (ki == i)
                        break;
                    index++;
                }
                DBSPExpression keyField = key.deref().field(index).applyCloneIfNeeded();
                if (nullableData) {
                    if (!keyField.getType().mayBeNull)
                        keyField = keyField.some();
                    keyField = new DBSPIfExpression(key.getNode(),
                            checkNull, keyField.getType().none(), keyField);
                }
                result.add(keyField);
                skipped++;
            } else {
                DBSPExpression dataField;
                dataField = data.deref().field(i - skipped).applyCloneIfNeeded();
                if (nullableData) {
                    if (!dataField.getType().mayBeNull)
                        dataField = dataField.some();
                    dataField = new DBSPIfExpression(key.getNode(),
                            checkNull, dataField.getType().none(), dataField);
                }
                result.add(dataField);
            }
        }
    }

    private int size() {
        return this.keyFields.size();
    }

    @Override
    public String toString() {
        return "KeyFields: " + this.keyFields;
    }
}
