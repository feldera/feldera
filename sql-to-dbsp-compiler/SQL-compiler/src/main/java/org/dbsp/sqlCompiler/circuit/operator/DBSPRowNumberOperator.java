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

/** Incremental operator implementing the ROW_NUMBER window aggregates.
 * Corresponds to the DBSP operators row_number_custom_order.
 * Similar to {@link DBSPRankOperator}. */
public class DBSPRowNumberOperator extends DBSPUnaryOperator
    implements IContainsIntegrator, IIncremental {
    /** Closure which produces the output tuple.  The signature is
     * (i64, sorted_tuple) -> output_tuple.  i64 is the rank of the current row. */
    public final DBSPClosureExpression outputProducer;

    static DBSPType outputType(DBSPTypeIndexedZSet sourceType, @Nullable DBSPClosureExpression outputProducer) {
        if (outputProducer == null)
            return sourceType;
        return new DBSPTypeIndexedZSet(sourceType.getNode(), sourceType.keyType,
                outputProducer.getResultType());
    }

    /**
     * Create a {@link DBSPRowNumberOperator} operator.  This operator is incremental only.
     * For a non-incremental version it should be sandwiched between a D-I.
     * @param node            CalciteObject which produced this operator.
     * @param comparator      A {@link DBSPComparatorExpression} used to sort items in each group
     *                        (later could be a {@link DBSPPathExpression} too).
     * @param outputProducer  Optional function with signature (rank, tuple) which produces the output.
     * @param source          Input operator.
     */
    public DBSPRowNumberOperator(CalciteRelNode node,
                                 DBSPExpression comparator,
                                 DBSPClosureExpression outputProducer, OutputPort source) {
        super(node, "row_number_custom_order", comparator,
                outputType(source.getOutputIndexedZSetType(), outputProducer),
                false, source);
        DBSPType valueType = source.getOutputIndexedZSetType().elementType;
        this.outputProducer = outputProducer;
        Utilities.enforce(comparator.is(DBSPComparatorExpression.class) ||
                comparator.is(DBSPPathExpression.class));
        Utilities.enforce(comparator.to(DBSPComparatorExpression.class).comparedValueType().sameType(valueType));
        Utilities.enforce(outputProducer.parameters.length == 2);
        Utilities.enforce(outputProducer.parameters[0].getType().is(DBSPTypeInteger.class));
        Utilities.enforce(outputProducer.parameters[1].getType().deref().sameType(valueType));
    }

    @Override
    public DBSPOperator with(@Nullable DBSPExpression function, DBSPType outputType, List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPRowNumberOperator(this.getRelNode(),
                    Objects.requireNonNull(function).to(DBSPComparatorExpression.class),
                    this.outputProducer,
                    newInputs.get(0)).copyAnnotations(this);
        }
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPRowNumberOperator otherOperator = other.as(DBSPRowNumberOperator.class);
        if (otherOperator == null)
            return false;
        return EquivalenceContext.equiv(this.outputProducer, otherOperator.outputProducer);
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
        visitor.property("outputProducer");
        this.outputProducer.accept(visitor);
    }

    @SuppressWarnings("unused")
    public static DBSPRowNumberOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        DBSPExpression limit = fromJsonInner(node, "limit", decoder, DBSPExpression.class);
        DBSPClosureExpression outputProducer = fromJsonInner(node, "outputProducer", decoder, DBSPClosureExpression.class);
        return new DBSPRowNumberOperator(CalciteEmptyRel.INSTANCE,
                info.getFunction(), outputProducer, info.getInput(0))
                .addAnnotations(info.annotations(), DBSPRowNumberOperator.class);
    }
}
