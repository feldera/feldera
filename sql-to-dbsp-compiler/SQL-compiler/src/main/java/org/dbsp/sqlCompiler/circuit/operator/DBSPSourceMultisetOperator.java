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
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

public final class DBSPSourceMultisetOperator
        extends DBSPSourceTableOperator {
    /**
     * Create a DBSP operator that is a source to the dataflow graph.
     * The table has *no* primary key, so the data can form a multiset.
     *
     * @param node       Calcite node for the statement creating the table
     *                   that this node is created from.
     * @param sourceName Calcite node for the identifier naming the table.
     * @param outputType Type of table.
     * @param name       The name of the table that this operator is created from.
     * @param comment    A comment describing the operator. */
    public DBSPSourceMultisetOperator(
            CalciteRelNode node, CalciteObject sourceName,
            DBSPTypeZSet outputType, DBSPTypeStruct originalRowType,
            TableMetadata metadata, ProgramIdentifier name, @Nullable String comment) {
        super(node, "source_multiset", sourceName, outputType, originalRowType, true,
                metadata, name, comment);
        Utilities.enforce(metadata.getColumnCount() == originalRowType.fields.size());
        Utilities.enforce(metadata.getColumnCount() == outputType.elementType.to(DBSPTypeTuple.class).size());
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
    public DBSPOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            Utilities.enforce(newInputs.isEmpty());
            return new DBSPSourceMultisetOperator(
                    this.getRelNode(), this.sourceName, outputType.to(DBSPTypeZSet.class), this.originalRowType,
                    this.metadata, this.tableName, this.comment).copyAnnotations(this);
        }
        return this;
    }

    @Override
    public DBSPTypeUser getHandleType() {
        DBSPType elementType = this.getOutputZSetElementType();
        return new DBSPTypeUser(
                this.getNode(), DBSPTypeCode.USER, "ZSetHandle", false,
                elementType);
    }

    @Override
    public String toString() {
        return this.getClass()
                .getSimpleName()
                .replace("DBSP", "")
                .replace("Operator", "")
                + " " + this.tableName
                + " " + this.getIdString();
    }

    @SuppressWarnings("unused")
    public static DBSPSourceMultisetOperator fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPTypeStruct originalRowType = DBSPNode.fromJsonInner(
                node, "originalRowType", decoder, DBSPTypeStruct.class);
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        ProgramIdentifier name = ProgramIdentifier.fromJson(Utilities.getProperty(node, "tableName"));
        TableMetadata metadata = TableMetadata.fromJson(Utilities.getProperty(node, "metadata"), decoder);
        return new DBSPSourceMultisetOperator(CalciteEmptyRel.INSTANCE, CalciteObject.EMPTY,
                info.getZsetType(), originalRowType, metadata, name, null)
                .addAnnotations(info.annotations(), DBSPSourceMultisetOperator.class);
    }
}
