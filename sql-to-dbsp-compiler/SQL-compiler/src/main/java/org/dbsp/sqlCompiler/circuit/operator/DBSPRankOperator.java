package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPAsymmetricFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Incremental operator implementing RANK, DENSE RANK aggregates.
 * Corresponds to one of the DBSP operators rank_custom_order or
 * dense_rank_custom_order.  Similar to {@link DBSPIndexedTopKOperator}. */
public class DBSPRankOperator extends DBSPUnaryOperator
    implements IContainsIntegrator, IIncremental {
    public final DBSPIndexedTopKOperator.Numbering numbering;
    /** Closure which produces the output tuple.  The signature is
     * (i64, sorted_tuple) -> output_tuple.  i64 is the rank of the current row. */
    public final DBSPClosureExpression outputProducer;
    /** Projects input row on fields used for comparison */
    public final DBSPClosureExpression projectionFunc;
    /** Closure which compares a row with a tuple containing just the sort fields */
    public final DBSPAsymmetricFieldComparatorExpression rankCmpFunc;

    static DBSPType outputType(DBSPTypeIndexedZSet sourceType, @Nullable DBSPClosureExpression outputProducer) {
        if (outputProducer == null)
            return sourceType;
        return new DBSPTypeIndexedZSet(sourceType.getNode(), sourceType.keyType,
                outputProducer.getResultType());
    }

    /**
     * Create a {@link DBSPRankOperator} operator.  This operator is incremental only.
     * For a non-incremental version it should be sandwiched between a D-I.
     * @param node            CalciteObject which produced this operator.
     * @param numbering       How items in each group are numbered.
     * @param comparator      A {@link DBSPComparatorExpression} used to sort items in each group
     *                        (later could be a {@link DBSPPathExpression} too).
     * @param outputProducer  Optional function with signature (rank, tuple) which produces the output.
     * @param rankCmpFunc     Compares the rank of two elements.
     * @param projectionFunc  Projects the input row on the fields used for sorting.
     * @param source          Input operator.
     */
    public DBSPRankOperator(CalciteRelNode node, DBSPIndexedTopKOperator.Numbering numbering,
                            DBSPExpression comparator,
                            DBSPAsymmetricFieldComparatorExpression rankCmpFunc,
                            DBSPClosureExpression projectionFunc,
                            DBSPClosureExpression outputProducer, OutputPort source) {
        super(node, "rank", comparator,
                outputType(source.getOutputIndexedZSetType(), outputProducer),
                numbering.mayHaveDuplicates(), source);
        DBSPType valueType = source.getOutputIndexedZSetType().elementType;
        this.numbering = numbering;
        this.projectionFunc = projectionFunc;
        this.rankCmpFunc = rankCmpFunc;
        this.outputProducer = outputProducer;
        Utilities.enforce(comparator.is(DBSPComparatorExpression.class) ||
                comparator.is(DBSPPathExpression.class));
        Utilities.enforce(comparator.to(DBSPComparatorExpression.class).comparedValueType().sameType(valueType));
        Utilities.enforce(outputProducer.parameters.length == 2);
        Utilities.enforce(outputProducer.parameters[0].getType().is(DBSPTypeInteger.class));
        Utilities.enforce(outputProducer.parameters[1].getType().deref().sameType(valueType));
        DBSPType projectionResultType = projectionFunc.getResultType();
        Utilities.enforce(projectionFunc.parameters.length == 1);
        Utilities.enforce(projectionFunc.parameters[0].getType().deref().sameType(valueType));
        Utilities.enforce(rankCmpFunc.leftType.sameType(valueType));
        Utilities.enforce(rankCmpFunc.rightType.sameType(projectionResultType));
    }

    @Override
    public DBSPOperator with(@Nullable DBSPExpression function, DBSPType outputType, List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPRankOperator(this.getRelNode(),
                    this.numbering,
                    Objects.requireNonNull(function).to(DBSPComparatorExpression.class),
                    this.rankCmpFunc,
                    this.projectionFunc,
                    this.outputProducer,
                    newInputs.get(0)).copyAnnotations(this);
        }
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPRankOperator otherOperator = other.as(DBSPRankOperator.class);
        if (otherOperator == null)
            return false;
        return this.numbering == otherOperator.numbering &&
                EquivalenceContext.equiv(this.rankCmpFunc, otherOperator.rankCmpFunc) &&
                EquivalenceContext.equiv(this.outputProducer, otherOperator.outputProducer) &&
                EquivalenceContext.equiv(this.projectionFunc, otherOperator.projectionFunc);
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
        super.accept(visitor);
        visitor.property("projectionFunc");
        this.projectionFunc.accept(visitor);
        visitor.property("outputProducer");
        this.outputProducer.accept(visitor);
        visitor.property("rankCmpFunc");
        this.rankCmpFunc.accept(visitor);
    }

    @SuppressWarnings("unused")
    public static DBSPRankOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        DBSPExpression limit = fromJsonInner(node, "limit", decoder, DBSPExpression.class);
        DBSPIndexedTopKOperator.Numbering numbering = DBSPIndexedTopKOperator.Numbering.valueOf(Utilities.getStringProperty(node, "numbering"));
        DBSPClosureExpression projectionFunc = fromJsonInner(node, "projectionFunc", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression outputProducer = fromJsonInner(node, "outputProducer", decoder, DBSPClosureExpression.class);
        DBSPAsymmetricFieldComparatorExpression rankCmpFunc =
                fromJsonInner(node, "rankCmpFunc", decoder, DBSPAsymmetricFieldComparatorExpression.class);
        return new DBSPRankOperator(CalciteEmptyRel.INSTANCE, numbering,
                info.getFunction(), rankCmpFunc, projectionFunc, outputProducer, info.getInput(0))
                .addAnnotations(info.annotations(), DBSPRankOperator.class);
    }
}
