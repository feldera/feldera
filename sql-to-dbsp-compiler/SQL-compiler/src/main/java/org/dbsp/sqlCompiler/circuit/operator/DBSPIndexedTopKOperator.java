package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEqualityComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
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
    /** Closure which produces the output tuple.  The signature is
     * (i64, sorted_tuple) -> output_tuple.  i64 is the rank of the current row. */
    public final DBSPClosureExpression outputProducer;
    /** Only used when numbering != ROW_NUMBER.
     * In general if the function of the operator is a DBSPComparatorExpression x,
     * the equalityComparator is a DBSPEqualityComparator(x).  But these
     * two fields are treated differently when generating code -
     * the function is converted to a declaration, whereas the comparator is not. */
    public final DBSPEqualityComparatorExpression equalityComparator;

    static DBSPType outputType(DBSPTypeIndexedZSet sourceType, @Nullable DBSPClosureExpression outputProducer) {
        if (outputProducer == null)
            return sourceType;
        return new DBSPTypeIndexedZSet(sourceType.getNode(), sourceType.keyType,
                outputProducer.getResultType());
    }

    /**
     * Create an IndexedTopK operator.  This operator is incremental only.
     * For a non-incremental version it should be sandwiched between a D-I.
     * @param node            CalciteObject which produced this operator.
     * @param numbering       How items in each group are numbered.
     * @param comparator      A {@link DBSPComparatorExpression} used to sort items in each group
     *                        (later could be a {@link DBSPPathExpression} too).
     * @param limit           Max number of records output in each group.
     * @param outputProducer  Optional function with signature (rank, tuple) which produces the output.
     * @param source          Input operator.
     */
    public DBSPIndexedTopKOperator(CalciteRelNode node, TopKNumbering numbering,
                                   DBSPExpression comparator, DBSPExpression limit,
                                   DBSPEqualityComparatorExpression equalityComparator,
                                   DBSPClosureExpression outputProducer, OutputPort source) {
        super(node, "topK", comparator,
                outputType(source.getOutputIndexedZSetType(), outputProducer), source.isMultiset(), source, true);
        Utilities.enforce(comparator.is(DBSPComparatorExpression.class) ||
                comparator.is(DBSPPathExpression.class));
        this.limit = limit;
        this.numbering = numbering;
        this.outputProducer = outputProducer;
        this.equalityComparator = equalityComparator;
        if (!this.outputType.is(DBSPTypeIndexedZSet.class))
            throw new InternalCompilerError("Expected the input to be an IndexedZSet type",
                    source.outputType());
    }

    @Override
    public void accept(InnerVisitor visitor) {
        super.accept(visitor);
        visitor.property("limit");
        this.limit.accept(visitor);
        visitor.property("outputProducer");
        this.outputProducer.accept(visitor);
        visitor.property("equalityComparator");
        this.equalityComparator.accept(visitor);
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPIndexedTopKOperator otherOperator = other.as(DBSPIndexedTopKOperator.class);
        if (otherOperator == null)
            return false;
        return this.numbering == otherOperator.numbering &&
                EquivalenceContext.equiv(this.equalityComparator, otherOperator.equalityComparator) &&
                EquivalenceContext.equiv(this.outputProducer, otherOperator.outputProducer) &&
                EquivalenceContext.equiv(this.limit, otherOperator.limit);
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPIndexedTopKOperator(this.getRelNode(), this.numbering,
                    Objects.requireNonNull(function).to(DBSPComparatorExpression.class),
                    this.limit, this.equalityComparator, this.outputProducer,
                    newInputs.get(0)).copyAnnotations(this);
        }
        return this;
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
        DBSPClosureExpression outputProducer = fromJsonInner(node, "outputProducer", decoder, DBSPClosureExpression.class);
        DBSPEqualityComparatorExpression equalityComparator =
                fromJsonInner(node, "equalityComparator", decoder, DBSPEqualityComparatorExpression.class);
        return new DBSPIndexedTopKOperator(CalciteEmptyRel.INSTANCE, numbering,
                info.getFunction(),
                limit, equalityComparator, outputProducer, info.getInput(0))
                .addAnnotations(info.annotations(), DBSPIndexedTopKOperator.class);
    }
}
