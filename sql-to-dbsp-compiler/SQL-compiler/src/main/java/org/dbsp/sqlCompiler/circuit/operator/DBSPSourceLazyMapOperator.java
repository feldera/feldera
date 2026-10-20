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
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** Implementation of a table with a primary key when {@code FELDERA_LAZY_UPSERT} is on,
 * unless the table has a LATENESS column.  It does not support partial updates. */
public final class DBSPSourceLazyMapOperator extends DBSPSourceMapOperator {
    public DBSPSourceLazyMapOperator(
            CalciteRelNode node, CalciteObject sourceName, List<Integer> keyFields,
            DBSPTypeIndexedZSet outputType, DBSPTypeStruct originalRowType,
            TableMetadata metadata, ProgramIdentifier name, StreamKind kind, @Nullable String comment) {
        super(node, "lazy_source_map", sourceName, keyFields, outputType, originalRowType,
                metadata, name, kind, comment);
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
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            Utilities.enforce(newInputs.isEmpty());
            return new DBSPSourceLazyMapOperator(this.getRelNode(), this.sourceName,
                    this.keyFields, outputType.to(DBSPTypeIndexedZSet.class), this.originalRowType,
                    this.metadata, this.tableName, this.kind, this.comment).copyAnnotations(this);
        }
        return this;
    }

    @Override
    public DBSPSourceBaseOperator withKind(StreamKind kind) {
        return new DBSPSourceLazyMapOperator(this.getRelNode(), this.sourceName, this.keyFields,
                this.getOutputIndexedZSetType(), this.originalRowType, this.metadata, this.tableName, kind, this.comment)
                .copyAnnotations(this).to(DBSPSourceBaseOperator.class);
    }

    @Override
    public DBSPSourceTableOperator withMetadata(TableMetadata metadata) {
        return new DBSPSourceLazyMapOperator(this.getRelNode(), this.sourceName, this.keyFields,
                this.getOutputIndexedZSetType(), this.originalRowType, metadata, this.tableName, this.kind, this.comment);
    }

    @Override
    public DBSPTypeUser getHandleType() {
        DBSPTypeIndexedZSet ix = this.getDataOutputType().to(DBSPTypeIndexedZSet.class);
        return new DBSPTypeUser(
                ix.getNode(), DBSPTypeCode.USER, "LazyMapHandle", false, ix.keyType, ix.elementType);
    }

    @SuppressWarnings("unused")
    public static DBSPSourceLazyMapOperator fromJson(JsonNode node, JsonDecoder decoder) {
        // Read in the order accept(InnerVisitor) writes: the original row type comes first
        DBSPTypeStruct originalRowType = DBSPNode.fromJsonInner(
                node, "originalRowType", decoder, DBSPTypeStruct.class);
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        ProgramIdentifier name = ProgramIdentifier.fromJson(Utilities.getProperty(node, "tableName"));
        TableMetadata metadata = TableMetadata.fromJson(Utilities.getProperty(node, "metadata"), decoder);
        List<Integer> keyFields = Linq.list(Linq.map(
                Utilities.getProperty(node, "keyFields").elements(), JsonNode::asInt));
        return new DBSPSourceLazyMapOperator(CalciteEmptyRel.INSTANCE, CalciteObject.EMPTY, keyFields,
                info.getIndexedZsetType(), originalRowType, metadata, name, decoder.sourceKind(), null)
                .addAnnotations(info.annotations(), DBSPSourceLazyMapOperator.class);
    }
}
