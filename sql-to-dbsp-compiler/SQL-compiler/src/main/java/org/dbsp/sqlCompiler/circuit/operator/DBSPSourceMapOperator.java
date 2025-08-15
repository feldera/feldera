package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.IInputMapOperator;
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
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** This operator produces an IndexedZSet as a result, indexed on the table keys. */
public final class DBSPSourceMapOperator
        extends DBSPSourceTableOperator
        implements IInputMapOperator {
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
    public int getDataOutputIndex() {
        return 0;
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            Utilities.enforce(newInputs.isEmpty());
            return new DBSPSourceMapOperator(this.getRelNode(), this.sourceName,
                    this.keyFields, outputType.to(DBSPTypeIndexedZSet.class), this.originalRowType,
                    this.metadata, this.tableName, this.comment).copyAnnotations(this);
        }
        return this;
    }

    @Override
    public TableMetadata getMetadata() {
        return this.metadata;
    }

    @Override
    public List<Integer> getKeyFields() {
        return this.keyFields;
    }

    @Override
    public DBSPTypeStruct getOriginalRowType() {
        return this.originalRowType;
    }

    @Override
    public DBSPOperator asOperator() {
        return this;
    }

    // equivalent inherited from base class

    @SuppressWarnings("unused")
    public static DBSPSourceMapOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        ProgramIdentifier name = ProgramIdentifier.fromJson(Utilities.getProperty(node, "tableName"));
        DBSPTypeStruct originalRowType = DBSPNode.fromJsonInner(
                node, "originalRowType", decoder, DBSPTypeStruct.class);
        TableMetadata metadata = TableMetadata.fromJson(Utilities.getProperty(node, "metadata"), decoder);
        List<Integer> keyFields = Linq.list(Linq.map(
                Utilities.getProperty(node, "keyFields").elements(), JsonNode::asInt));
        return new DBSPSourceMapOperator(CalciteEmptyRel.INSTANCE, CalciteObject.EMPTY, keyFields,
                info.getIndexedZsetType(), originalRowType, metadata, name, null)
                .addAnnotations(info.annotations(), DBSPSourceMapOperator.class);
    }
}
