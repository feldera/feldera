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
 * they share the same ORDER BY column. Each percentile can independently
 * be CONT or DISC, controlled by the isContinuous array.
 * Sort direction (ASC/DESC) does not require separate trees because
 * DESC with percentile p is equivalent to ASC with percentile (1-p);
 * the compiler normalizes DESC to ASC by inverting percentile values.
 *
 * This operator calls the stream method percentile() defined in
 * dbsp's operator/percentile.rs.
 */
public final class DBSPPercentileOperator extends DBSPUnaryOperator {
    /** The percentile values (each 0.0 to 1.0) */
    public final double[] percentiles;
    /** Per-percentile CONT (true) / DISC (false) flag */
    public final boolean[] isContinuous;
    /** Sort order: true = ascending */
    public final boolean ascending;
    /** Closure that extracts the value to compute percentile over from input tuples.
     *  Signature: (key, value) -> percentile_value */
    public final DBSPClosureExpression valueExtractor;

    public DBSPPercentileOperator(CalciteRelNode node,
                                  double[] percentiles,
                                  boolean[] isContinuous,
                                  boolean ascending,
                                  DBSPClosureExpression valueExtractor,
                                  DBSPType outputType,
                                  OutputPort source) {
        super(node, "percentile", valueExtractor, outputType, source.isMultiset(), source);
        assert percentiles.length == isContinuous.length :
                "percentiles and isContinuous must have the same length";
        this.percentiles = percentiles;
        this.isContinuous = isContinuous;
        this.ascending = ascending;
        this.valueExtractor = valueExtractor;
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
        visitor.property("isContinuousCount");
        new DBSPDoubleLiteral(this.isContinuous.length).accept(visitor);
        for (int i = 0; i < this.isContinuous.length; i++) {
            visitor.property("isContinuous_" + i);
            new DBSPBoolLiteral(this.isContinuous[i]).accept(visitor);
        }
        visitor.property("ascending");
        new DBSPBoolLiteral(this.ascending).accept(visitor);
        visitor.property("valueExtractor");
        this.valueExtractor.accept(visitor);
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPPercentileOperator otherOp = other.as(DBSPPercentileOperator.class);
        if (otherOp == null)
            return false;
        return Arrays.equals(this.percentiles, otherOp.percentiles) &&
                Arrays.equals(this.isContinuous, otherOp.isContinuous) &&
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
                    this.isContinuous,
                    this.ascending,
                    this.valueExtractor,
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
        int isContinuousCount = (int) node.get("isContinuousCount").asDouble();
        boolean[] isContinuous = new boolean[isContinuousCount];
        for (int i = 0; i < isContinuousCount; i++) {
            isContinuous[i] = node.get("isContinuous_" + i).asBoolean();
        }
        boolean ascending = node.get("ascending").asBoolean();
        DBSPClosureExpression valueExtractor = fromJsonInner(node, "valueExtractor", decoder, DBSPClosureExpression.class);
        return new DBSPPercentileOperator(
                CalciteEmptyRel.INSTANCE,
                percentiles, isContinuous, ascending,
                valueExtractor,
                info.outputType(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPPercentileOperator.class);
    }
}
