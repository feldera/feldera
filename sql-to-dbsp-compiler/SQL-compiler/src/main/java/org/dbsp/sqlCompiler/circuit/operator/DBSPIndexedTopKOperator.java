package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Apply a topK operation to each of the groups in an indexed collection.
 * This always sorts the elements of each group.
 * To sort the entire collection just group by (). */
public final class DBSPIndexedTopKOperator extends DBSPUnaryOperator {
    /** These values correspond to the SQL keywords
     * ROW, RANK, and DENSE RANK.  See e.g.:
     * https://learn.microsoft.com/en-us/sql/t-sql/functions/ranking-functions-transact-sql
     */
    @SuppressWarnings("JavadocLinkAsPlainText")
    public enum TopKNumbering {
        ROW_NUMBER,
        RANK,
        DENSE_RANK
    }

    public final TopKNumbering numbering;
    /** Limit K used by TopK.  Expected to be a constant */
    public final DBSPExpression limit;
    /** Optional closure which produces the output tuple.  The signature is
     * (i64, sorted_tuple) -> output_tuple.  i64 is the rank of the current row.
     * If this closure is missing it is assumed to produce just the sorted_tuple. */
    @Nullable
    public final DBSPClosureExpression outputProducer;

    static DBSPType outputType(DBSPTypeIndexedZSet sourceType, @Nullable DBSPClosureExpression outputProducer) {
        if (outputProducer == null)
            return sourceType;
        return new DBSPTypeIndexedZSet(sourceType.getNode(), sourceType.keyType,
                outputProducer.getResultType());
    }

    @Override
    public void accept(InnerVisitor visitor) {
        super.accept(visitor);
        visitor.property("limit");
        this.limit.accept(visitor);
        if (this.outputProducer != null) {
            visitor.property("outputProducer");
            this.outputProducer.accept(visitor);
        }
    }

    /**
     * Create an IndexedTopK operator.  This operator is incremental only.
     * For a non-incremental version it should be sandwiched between a D-I.
     * @param node            CalciteObject which produced this operator.
     * @param numbering       How items in each group are numbered.
     * @param comparator      A ComparatorExpression used to sort items in each group.
     * @param limit           Max number of records output in each group.
     * @param outputProducer  Optional function with signature (rank, tuple) which produces the output.
     * @param source          Input operator.
     */
    public DBSPIndexedTopKOperator(CalciteObject node, TopKNumbering numbering,
                                   DBSPComparatorExpression comparator, DBSPExpression limit,
                                   @Nullable DBSPClosureExpression outputProducer, OutputPort source) {
        super(node, "topK", comparator,
                outputType(source.getOutputIndexedZSetType(), outputProducer), source.isMultiset(), source);
        this.limit = limit;
        this.numbering = numbering;
        this.outputProducer = outputProducer;
        if (!this.outputType.is(DBSPTypeIndexedZSet.class))
            throw new InternalCompilerError("Expected the input to be an IndexedZSet type",
                    source.outputType());
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPIndexedTopKOperator(this.getNode(), this.numbering,
                    this.getFunction().to(DBSPComparatorExpression.class),
                    this.limit, this.outputProducer, newInputs.get(0)).copyAnnotations(this);
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPIndexedTopKOperator otherOperator = other.as(DBSPIndexedTopKOperator.class);
        if (otherOperator == null)
            return false;
        return this.numbering == otherOperator.numbering &&
                EquivalenceContext.equiv(this.outputProducer, otherOperator.outputProducer) &&
                EquivalenceContext.equiv(this.limit, otherOperator.limit);
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPIndexedTopKOperator(this.getNode(), this.numbering,
                Objects.requireNonNull(expression).to(DBSPComparatorExpression.class), this.limit,
                this.outputProducer, this.input()).copyAnnotations(this);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @SuppressWarnings("unused")
    public static DBSPIndexedTopKOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        DBSPExpression limit = fromJsonInner(node, "limit", decoder, DBSPExpression.class);
        TopKNumbering numbering = TopKNumbering.valueOf(Utilities.getStringProperty(node, "numbering"));
        DBSPClosureExpression outputProducer = null;
        if (node.has("outputProducer"))
            outputProducer = fromJsonInner(node, "outputProducer", decoder, DBSPClosureExpression.class);
        return new DBSPIndexedTopKOperator(CalciteObject.EMPTY, numbering,
                info.getFunction().to(DBSPComparatorExpression.class),
                limit, outputProducer, info.getInput(0))
                .addAnnotations(info.annotations(), DBSPIndexedTopKOperator.class);
    }
}
