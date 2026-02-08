package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

/**
 * Stateful percentile operator that maintains OrderStatisticsZSet per key.
 * Unlike the aggregate-based approach, this operator applies delta changes
 * incrementally (O(log n) per change) instead of rescanning all values.
 *
 * Supports computing multiple percentiles from a single shared tree when
 * they share the same ORDER BY column and direction (CONT vs DISC).
 *
 * This operator calls the stream methods percentile_cont() or percentile_disc()
 * defined in dbsp's operator/percentile.rs.
 */
public final class DBSPPercentileOperator extends DBSPUnaryOperator {
    /** The percentile values (each 0.0 to 1.0) */
    public final double[] percentiles;
    /** Whether to use PERCENTILE_CONT (true) or PERCENTILE_DISC (false) */
    public final boolean continuous;
    /** Sort order: true = ascending */
    public final boolean ascending;
    /** Closure that extracts the value to compute percentile over from input tuples.
     *  Signature: (key, value) -> percentile_value */
    public final DBSPClosureExpression valueExtractor;
    /** Post-processing closure for PERCENTILE_CONT interpolation.
     *  For types needing interpolation (integers -> f64), this handles the conversion.
     *  Signature: Option<V> -> ResultType */
    @Nullable
    public final DBSPClosureExpression postProcessor;

    public DBSPPercentileOperator(CalciteRelNode node,
                                  double[] percentiles,
                                  boolean continuous,
                                  boolean ascending,
                                  DBSPClosureExpression valueExtractor,
                                  @Nullable DBSPClosureExpression postProcessor,
                                  DBSPType outputType,
                                  OutputPort source) {
        super(node, "percentile", valueExtractor, outputType, source.isMultiset(), source);
        this.percentiles = percentiles;
        this.continuous = continuous;
        this.ascending = ascending;
        this.valueExtractor = valueExtractor;
        this.postProcessor = postProcessor;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        super.accept(visitor);
        visitor.property("percentileCount");
        new DBSPDoubleLiteral(this.percentiles.length).accept(visitor);
        for (int i = 0; i < this.percentiles.length; i++) {
            visitor.property("percentile_" + i);
            new DBSPDoubleLiteral(this.percentiles[i]).accept(visitor);
        }
        visitor.property("continuous");
        new DBSPBoolLiteral(this.continuous).accept(visitor);
        visitor.property("ascending");
        new DBSPBoolLiteral(this.ascending).accept(visitor);
        visitor.property("valueExtractor");
        this.valueExtractor.accept(visitor);
        if (this.postProcessor != null) {
            visitor.property("postProcessor");
            this.postProcessor.accept(visitor);
        }
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPPercentileOperator otherOp = other.as(DBSPPercentileOperator.class);
        if (otherOp == null)
            return false;
        return Arrays.equals(this.percentiles, otherOp.percentiles) &&
                this.continuous == otherOp.continuous &&
                this.ascending == otherOp.ascending;
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPPercentileOperator(
                    this.getRelNode(),
                    this.percentiles,
                    this.continuous,
                    this.ascending,
                    this.valueExtractor,
                    this.postProcessor,
                    outputType,
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
    public static DBSPPercentileOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        int percentileCount = (int) node.get("percentileCount").asDouble();
        double[] percentiles = new double[percentileCount];
        for (int i = 0; i < percentileCount; i++) {
            percentiles[i] = node.get("percentile_" + i).asDouble();
        }
        boolean continuous = node.get("continuous").asBoolean();
        boolean ascending = node.get("ascending").asBoolean();
        DBSPClosureExpression valueExtractor = fromJsonInner(node, "valueExtractor", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression postProcessor = null;
        if (node.has("postProcessor")) {
            postProcessor = fromJsonInner(node, "postProcessor", decoder, DBSPClosureExpression.class);
        }
        return new DBSPPercentileOperator(
                CalciteEmptyRel.INSTANCE,
                percentiles, continuous, ascending,
                valueExtractor, postProcessor,
                info.outputType(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPPercentileOperator.class);
    }
}
