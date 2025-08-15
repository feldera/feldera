package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.IInputMapOperator;
import org.dbsp.sqlCompiler.circuit.IMultiOutput;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeTypedBox;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.List;

/** Corresponds to input_map_with_waterline operator from DBSP.
 * This operator has 3 outputs: the data, the waterline, and the error stream
 * with records that missed the lateness. */
public class DBSPInputMapWithWaterlineOperator
        extends DBSPOperator
        implements IMultiOutput, IInputMapOperator, IInputOperator {
    // Fields that belong normally to SourceTableOperators (which we don't derive from)
    public final ProgramIdentifier tableName;
    public final DBSPTypeStruct originalRowType;
    public final CalciteObject sourceName;
    // Note: the metadata is not transformed after being set.
    // In particular, types are not rewritten.
    public final TableMetadata metadata;
    public final List<Integer> keyFields;

    public final String operation;
    public final DBSPClosureExpression initializer;
    public final DBSPClosureExpression timestamp;
    public final DBSPClosureExpression lub;
    public final DBSPClosureExpression filter;
    public final DBSPClosureExpression error;

    final DBSPTypeIndexedZSet outputType;
    final DBSPType waterlineType;
    final DBSPType errorType;

    public DBSPInputMapWithWaterlineOperator(
            CalciteRelNode node, CalciteObject sourceName, List<Integer> keyFields, DBSPTypeIndexedZSet outputType,
            DBSPTypeStruct originalRowType, TableMetadata metadata, ProgramIdentifier name,
            DBSPClosureExpression initializer, DBSPClosureExpression timestamp, DBSPClosureExpression lub,
            DBSPClosureExpression filter, DBSPClosureExpression error) {
        super(node);
        this.sourceName = sourceName;
        this.outputType = outputType;
        this.tableName = name;
        this.originalRowType = originalRowType;
        this.metadata = metadata;
        this.keyFields = keyFields;

        this.waterlineType = lub.getResultType();
        this.initializer = initializer;
        this.timestamp = timestamp;
        this.filter = filter;
        this.lub = lub;
        this.error = error;
        this.operation = "input_map_with_waterline";

        this.errorType = TypeCompiler.makeZSet(error.getResultType());
        Utilities.enforce(initializer.parameters.length == 0);
    }

    @Override
    public int getDataOutputIndex() {
        return 0;
    }

    @Override
    public boolean hasOutput(int outputNumber) {
        return outputNumber >= 0 && outputNumber <= 2;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        // A source is never equivalent with another operator
        return false;
    }

    @Override
    public DBSPType outputType(int outputNo) {
        return switch (outputNo) {
            case 0 -> this.outputType;
            case 1 -> this.errorType;
            case 2 -> new DBSPTypeTypedBox(this.lub.getResultType(), false);
            default -> throw new InternalCompilerError("Unpexected output " + outputNo);
        };
    }

    @Override
    public boolean isMultiset(int outputNumber) {
        return false;
    }

    @Override
    public int outputCount() {
        return 3;
    }

    @Override
    public TableMetadata getMetadata() {
        return this.metadata;
    }

    @Override
    public DBSPOperator withInputs(List<OutputPort> inputs, boolean force) {
        return this;
    }

    @Override
    public DBSPOperator asOperator() {
        return this;
    }

    @Override
    public ProgramIdentifier getTableName() {
        return IInputMapOperator.super.getTableName();
    }

    @Override
    public DBSPType getDataOutputType() {
        return this.outputType(0);
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
    public void accept(InnerVisitor visitor) {
        visitor.property("originalRowType");
        this.originalRowType.accept(visitor);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("let ")
                .append(this.getNodeName(false))
                .append(" = ")
                .append(this.tableName.toString())
                .append("();");
    }

    @Override
    public List<Integer> getKeyFields() {
        return this.keyFields;
    }

    @Override
    public DBSPTypeIndexedZSet getOutputIndexedZSetType() {
        return this.outputType;
    }

    @Override
    public DBSPTypeStruct getOriginalRowType() {
        return this.originalRowType;
    }

    @SuppressWarnings("unused")
    public static DBSPInputMapWithWaterlineOperator fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPTypeIndexedZSet outputType = DBSPNode.fromJsonInner(node, "outputType", decoder, DBSPTypeIndexedZSet.class);
        ProgramIdentifier name = ProgramIdentifier.fromJson(Utilities.getProperty(node, "tableName"));
        DBSPTypeStruct originalRowType = DBSPNode.fromJsonInner(node, "originalRowType", decoder, DBSPTypeStruct.class);
        TableMetadata metadata = TableMetadata.fromJson(Utilities.getProperty(node, "metadata"), decoder);
        List<Integer> keyFields = Linq.list(Linq.map(
                Utilities.getProperty(node, "keyFields").elements(), JsonNode::asInt));
        DBSPClosureExpression initializer = DBSPNode.fromJsonInner(
                node, "initializer", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression timestamp = DBSPNode.fromJsonInner(
                node, "timestamp", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression lub = DBSPNode.fromJsonInner(
                node, "lub", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression filter = DBSPNode.fromJsonInner(
                node, "filter", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression error = DBSPNode.fromJsonInner(
                node, "error", decoder, DBSPClosureExpression.class);
        return new DBSPInputMapWithWaterlineOperator(CalciteEmptyRel.INSTANCE, CalciteObject.EMPTY, keyFields,
                outputType, originalRowType, metadata, name, initializer, timestamp, lub, filter, error);
    }
}
