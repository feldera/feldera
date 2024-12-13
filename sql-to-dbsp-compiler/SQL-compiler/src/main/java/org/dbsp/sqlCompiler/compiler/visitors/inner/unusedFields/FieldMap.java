package org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields;

import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Maps each tuple field index to a new tuple field index.  */
public class FieldMap {
    public final DBSPType parameterType;
    /** Total number of fields in the represented type */
    final int size;
    /** Map is indexed with a field in the represented type, and gives a field in the compressed type. */
    private final LinkedHashMap<Integer, Integer> fieldMap;

    public List<Integer> getUsedFields() {
        return Linq.list(this.fieldMap.keySet());
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean hasUnusedFields() {
        return this.size > this.fieldMap.size();
    }

    public boolean isEmpty() {
        return this.fieldMap.isEmpty();
    }

    public FieldMap(DBSPType parameterType, BitSet bits) {
        this.parameterType = parameterType;
        this.fieldMap = new LinkedHashMap<>();
        assert this.parameterType.is(DBSPTypeRef.class);
        assert this.parameterType.deref().is(DBSPTypeTupleBase.class);
        this.size = this.parameterType.deref().to(DBSPTypeTupleBase.class).size();

        int compressed = 0;
        for (int i = 0; i < bits.length(); i++) {
            if (bits.get(i)) {
                Utilities.putNew(this.fieldMap, i, compressed);
                compressed++;
            }
        }
    }

    static FieldMap identity(DBSPType parameterType) {
        DBSPTypeTupleBase type = parameterType.deref().to(DBSPTypeTupleBase.class);
        BitSet all = new BitSet();
        all.set(0, type.size());
        return new FieldMap(parameterType, all);
    }

    public int getNewIndex(int originalIndex) {
        return Utilities.getExists(this.fieldMap, originalIndex);
    }

    public DBSPTypeTupleBase compressedType() {
        DBSPTypeTupleBase tuple = this.parameterType.deref().to(DBSPTypeTupleBase.class);
        if (this.fieldMap.size() == tuple.size())
            return tuple;
        DBSPType[] fields = new DBSPType[this.fieldMap.size()];
        for (int i = 0; i < tuple.size(); i++) {
            if (this.fieldMap.containsKey(i)) {
                int target = this.fieldMap.get(i);
                fields[target] = tuple.getFieldType(i);
            }
        }
        return tuple.makeType(Linq.list(fields));
    }

    public DBSPClosureExpression getProjection() {
        DBSPVariablePath var = this.parameterType.var();
        DBSPTypeTupleBase tuple = this.parameterType.deref().to(DBSPTypeTupleBase.class);
        DBSPExpression[] fields = new DBSPExpression[this.fieldMap.size()];
        for (int i = 0; i < tuple.size(); i++) {
            if (this.fieldMap.containsKey(i)) {
                int target = this.fieldMap.get(i);
                fields[target] = var.deref().field(i).applyCloneIfNeeded();
            }
        }
        return tuple.makeTuple(fields).closure(var.asParameter());
    }

    @Override
    public String toString() {
        return this.fieldMap.keySet().toString();
    }
}
