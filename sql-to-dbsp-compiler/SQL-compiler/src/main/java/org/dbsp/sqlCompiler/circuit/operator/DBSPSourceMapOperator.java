package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.InputTableMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * This operator produces an IndexedZSet as a result, indexed on the table keys.
 */
public class DBSPSourceMapOperator extends DBSPSourceTableOperator {
    public final List<Integer> keyFields;

    /**
     * Create a DBSP operator that is a source to the dataflow graph.
     * The table has a primary key, so the data forms a set.
     * The data is represented as an indexed zset, hence the name "MapOperator".
     * @param node        Calcite node for the statement creating the table
     *                    that this node is created from.
     * @param sourceName  Calcite node for the identifier naming the table.
     * @param outputType  Type of output produced.
     * @param keyFields   Fields of the input row which compose the key.
     * @param comment     A comment describing the operator.
     * @param name        The name of the table that this operator is created from.
     */
    public DBSPSourceMapOperator(
            CalciteObject node, CalciteObject sourceName, List<Integer> keyFields,
            DBSPTypeIndexedZSet outputType, DBSPTypeStruct originalRowType, @Nullable String comment,
            InputTableMetadata metadata, String name) {
        super(node, sourceName, outputType, originalRowType, comment, metadata, name);
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
    public DBSPOperator withFunction(@Nullable DBSPExpression unused, DBSPType outputType) {
        return new DBSPSourceMapOperator(this.getNode(), this.sourceName,
                this.keyFields, outputType.to(DBSPTypeIndexedZSet.class), this.originalRowType,
                this.comment, this.metadata, this.outputName);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPSourceMapOperator(this.getNode(), this.sourceName,
                    this.keyFields, this.getOutputIndexedZSetType(), this.originalRowType,
                    this.comment, this.metadata, this.outputName);
        return this;
    }

    /**
     * Return a struct that contains only the key fields from the
     * originalRowType.
     */
    public DBSPTypeStruct getKeyStructType() {
        // TODO: this should be a fresh name.
        String name = this.originalRowType.sanitizedName + "_key";
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
        return new DBSPTypeStruct(this.originalRowType.getNode(), name, name, fields);
    }

    /**
     * Return a closure that describes the key function.
     */
    public DBSPExpression getKeyFunc() {
        DBSPVariablePath var = new DBSPVariablePath("t", this.getOutputIndexedZSetType().elementType.ref());
        DBSPExpression[] fields = new DBSPExpression[this.keyFields.size()];
        int insertAt = 0;
        for (int index: this.keyFields) {
            fields[insertAt++] = var.deref().field(index);
        }
        DBSPExpression tuple = new DBSPTupleExpression(fields);
        return tuple.closure(var.asParameter());
    }
}
