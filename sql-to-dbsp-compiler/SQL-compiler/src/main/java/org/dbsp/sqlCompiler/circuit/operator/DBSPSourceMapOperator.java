package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeOption;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** This operator produces an IndexedZSet as a result, indexed on the table keys. */
public final class DBSPSourceMapOperator extends DBSPSourceTableOperator {
    public final List<Integer> keyFields;

    /**
     * Create a DBSP operator that is a source to the dataflow graph.
     * The table has a primary key, so the data forms a set.
     * The data is represented as an indexed zset, hence the name "MapOperator".
     *
     * @param node       Calcite node for the statement creating the table
     *                   that this node is created from.
     * @param sourceName Calcite node for the identifier naming the table.
     * @param keyFields  Fields of the input row which compose the key.
     * @param outputType Type of output produced.
     * @param name       The name of the table that this operator is created from.
     * @param comment    A comment describing the operator.
     */
    public DBSPSourceMapOperator(
            CalciteRelNode node, CalciteObject sourceName, List<Integer> keyFields,
            DBSPTypeIndexedZSet outputType, DBSPTypeStruct originalRowType,
            TableMetadata metadata, ProgramIdentifier name, @Nullable String comment) {
        super(node, "source_map", sourceName, outputType, originalRowType, false,
                metadata, name, comment);
        this.keyFields = keyFields;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression unused, DBSPType outputType) {
        return new DBSPSourceMapOperator(this.getRelNode(), this.sourceName,
                this.keyFields, outputType.to(DBSPTypeIndexedZSet.class), this.originalRowType,
                this.metadata, this.tableName, this.comment).copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        assert newInputs.isEmpty();
        if (force)
            return new DBSPSourceMapOperator(this.getRelNode(), this.sourceName,
                    this.keyFields, this.getOutputIndexedZSetType(), this.originalRowType,
                    this.metadata, this.tableName, this.comment).copyAnnotations(this);
        return this;
    }

    /** Return a struct that contains only the key fields from the
     * originalRowType. */
    public DBSPTypeStruct getKeyStructType(ProgramIdentifier name) {
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        int current = 0;
        int keyIndexes = 0;
        for (DBSPTypeStruct.Field field: this.originalRowType.fields.values()) {
            if (current == this.keyFields.get(keyIndexes)) {
                fields.add(field);
                keyIndexes++;
                if (keyIndexes == this.keyFields.size())
                    break;
            }
            current++;
        }
        return new DBSPTypeStruct(this.originalRowType.getNode(), name, name.name(), fields, false);
    }

    /** Return a struct that is similar with the originalRowType, but where
     * each non-key field is wrapped in an additional Option type. */
    public DBSPTypeStruct getStructUpsertType(ProgramIdentifier name) {
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        int current = 0;
        int keyIndexes = 0;
        for (DBSPTypeStruct.Field field: this.originalRowType.fields.values()) {
            if (keyIndexes < this.keyFields.size() && current == this.keyFields.get(keyIndexes)) {
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
        return new DBSPTypeStruct(this.originalRowType.getNode(), name, name.name(), fields, false);
    }

    /** Return a closure that describes the key function. */
    public DBSPExpression getKeyFunc() {
        DBSPVariablePath var = new DBSPVariablePath(this.getOutputIndexedZSetType().elementType.ref());
        DBSPExpression[] fields = new DBSPExpression[this.keyFields.size()];
        int insertAt = 0;
        for (int index: this.keyFields) {
            fields[insertAt++] = var.deref().field(index).applyCloneIfNeeded();
        }
        DBSPExpression tuple = new DBSPTupleExpression(fields);
        return tuple.closure(var);
    }

    /** Return a closure that describes the key function when applied to upsertStructType.toTuple(). */
    public DBSPExpression getUpdateKeyFunc(DBSPTypeStruct upsertStructType) {
        DBSPVariablePath var = new DBSPVariablePath(upsertStructType.toTupleDeep().ref());
        DBSPExpression[] fields = new DBSPExpression[this.keyFields.size()];
        int insertAt = 0;
        for (int index: this.keyFields) {
            fields[insertAt++] = var.deref().field(index).applyCloneIfNeeded();
        }
        DBSPExpression tuple = new DBSPTupleExpression(fields);
        return tuple.closure(var);
    }

    // equivalent inherited from base class

    @SuppressWarnings("unused")
    public static DBSPSourceMapOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        ProgramIdentifier name = ProgramIdentifier.fromJson(Utilities.getProperty(node, "tableName"));
        DBSPType originalRowType = DBSPNode.fromJsonInner(node, "originalRowType", decoder, DBSPType.class);
        TableMetadata metadata = TableMetadata.fromJson(Utilities.getProperty(node, "metadata"), decoder);
        List<Integer> keyFields = Linq.list(Linq.map(
                Utilities.getProperty(node, "keyFields").elements(), JsonNode::asInt));
        return new DBSPSourceMapOperator(CalciteEmptyRel.INSTANCE, CalciteObject.EMPTY, keyFields,
                info.getIndexedZsetType(), originalRowType.to(DBSPTypeStruct.class), metadata, name, null)
                .addAnnotations(info.annotations(), DBSPSourceMapOperator.class);
    }
}
