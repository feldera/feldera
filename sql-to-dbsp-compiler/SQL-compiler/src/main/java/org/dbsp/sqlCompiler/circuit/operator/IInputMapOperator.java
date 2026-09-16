package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeOption;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;

/** Interface for a source operator that has primary keys */
public interface IInputMapOperator extends IInputOperator, IStateful {
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
        return new DBSPTypeStruct(this.getOriginalRowType().getNode(), name, fields, false);
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
        return new DBSPTypeStruct(this.getOriginalRowType().getNode(), name, fields, false);
    }

    /** Whether the table is fed through the lazy input map, which resolves a
     * transaction's writes against the table when the transaction commits,
     * rather than the eager map, which resolves each one as it arrives.
     *
     * <p>A table with a primary key and no LATENESS takes the lazy map.  A
     * table with LATENESS keeps the eager map, whose waterline the lazy map
     * has no counterpart for.  Later a table property will let the user
     * choose. */
    default boolean usesLazyInputMap() {
        return this.asOperator().is(DBSPSourceMapOperator.class)
                && !Linq.any(this.getMetadata().getColumns(), column -> column.lateness != null);
    }

    default DBSPTypeUser getHandleType() {
        DBSPTypeIndexedZSet ix = this.getDataOutputType().to(DBSPTypeIndexedZSet.class);
        if (this.usesLazyInputMap())
            // The lazy map takes whole records and deletes only, so its handle has no update type.
            return new DBSPTypeUser(
                    ix.getNode(), DBSPTypeCode.USER, "LazyMapHandle", false,
                    ix.keyType, ix.elementType);
        DBSPTypeStruct upsertStruct = this.getStructUpsertType(
                        new ProgramIdentifier(this.getOriginalRowType().hashName + "_upsert", false));
        return new DBSPTypeUser(
                ix.getNode(), DBSPTypeCode.USER, "MapHandle", false,
                ix.keyType, ix.elementType, upsertStruct.toTupleDeep());
    }
}
