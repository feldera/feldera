package org.dbsp.sqlCompiler.circuit;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeOption;

import java.util.ArrayList;
import java.util.List;

/** Interface for a source operator that has primary keys */
public interface IInputMapOperator {
    TableMetadata getMetadata();
    List<Integer> getKeyFields();
    DBSPTypeIndexedZSet getOutputIndexedZSetType();
    DBSPTypeStruct getOriginalRowType();
    DBSPOperator asOperator();
    int getDataOutputIndex();

    default ProgramIdentifier getTableName() {
        return this.getMetadata().tableName;
    }

    /** Return a closure that describes the key function. */
    default DBSPExpression getKeyFunc() {
        DBSPVariablePath var = new DBSPVariablePath(this.getOutputIndexedZSetType().elementType.ref());
        DBSPExpression[] fields = new DBSPExpression[this.getKeyFields().size()];
        int insertAt = 0;
        for (int index: this.getKeyFields()) {
            fields[insertAt++] = var.deref().field(index).applyCloneIfNeeded();
        }
        DBSPExpression tuple = new DBSPTupleExpression(fields);
        return tuple.closure(var);
    }

    /** Return a closure that describes the key function when applied to upsertStructType.toTuple(). */
    default DBSPExpression getUpdateKeyFunc(DBSPTypeStruct upsertStructType) {
        DBSPVariablePath var = new DBSPVariablePath(upsertStructType.toTupleDeep().ref());
        DBSPExpression[] fields = new DBSPExpression[this.getKeyFields().size()];
        int insertAt = 0;
        for (int index: this.getKeyFields()) {
            fields[insertAt++] = var.deref().field(index).applyCloneIfNeeded();
        }
        DBSPExpression tuple = new DBSPTupleExpression(fields);
        return tuple.closure(var);
    }

    /** Return a struct that contains only the key fields from the
     * originalRowType. */
    default DBSPTypeStruct getKeyStructType(ProgramIdentifier name) {
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        int current = 0;
        int keyIndexes = 0;
        for (DBSPTypeStruct.Field field: this.getOriginalRowType().fields.values()) {
            if (current == this.getKeyFields().get(keyIndexes)) {
                fields.add(field);
                keyIndexes++;
                if (keyIndexes == this.getKeyFields().size())
                    break;
            }
            current++;
        }
        return new DBSPTypeStruct(this.getOriginalRowType().getNode(), name, name.name(), fields, false);
    }

    /** Return a struct that is similar with the originalRowType, but where
     * each non-key field is wrapped in an additional Option type. */
    default DBSPTypeStruct getStructUpsertType(ProgramIdentifier name) {
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        int current = 0;
        int keyIndexes = 0;
        for (DBSPTypeStruct.Field field: this.getOriginalRowType().fields.values()) {
            if (keyIndexes < this.getKeyFields().size() && current == this.getKeyFields().get(keyIndexes)) {
                fields.add(field);
                keyIndexes++;
            } else {
                DBSPType fieldType = field.type;
                // We need here an explicit Option type, because
                // fieldType may be nullable.  The resulting Rust type will
                // actually be Option<Option<Type>>.
                DBSPType some = new DBSPTypeOption(fieldType);
                fields.add(new DBSPTypeStruct.Field(field.getNode(), field.name, current, some));
            }
            current++;
        }
        return new DBSPTypeStruct(this.getOriginalRowType().getNode(), name, name.name(), fields, false);
    }
}
