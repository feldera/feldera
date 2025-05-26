package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import java.util.List;
import java.util.Objects;

/** This operator represents a computation that is used aftr a global
 * aggregation (no group-by) to replace empty results with the expected zero.
 * This operator only used in the front-end, and is later expanded into the 
 * following subgraph:
 * 
 * <p>
 * The input is a zset like {}/{c->1}: either the empty set (for an empty input)
 * or the correct count with a weight of 1.
 * We need to produce {z->1}/{c->1}, where z is the actual zero of the fold above.
 * For this we synthesize the following graph:
 *     |
 * {}/{c->1}------------------------
 *    | map (|x| x -> z}           |
 * {}/{z->1}                       |
 *    | -                          |
 * {} {z->-1}   {z->1} (constant)  |
 *          \  /                  /
 *           +                   /
 *         {z->1}/{}  -----------
 *                 \ /
 *                  +
 *              {z->1}/{c->1}
 *                  |
 */
@NonCoreIR
public class DBSPAggregateZeroOperator extends DBSPUnaryOperator {
    /** Create an AggregateZero operator.
     *
     * @param node   Calcite node.
     * @param zero   Value of zero produced when input is empty.
     * @param source Input from aggregation.
     */
    public DBSPAggregateZeroOperator(
            CalciteRelNode node, DBSPExpression zero, OutputPort source) {
        super(node, "aggregate_zero", zero, TypeCompiler.makeZSet(zero.getType()),
                false, source);
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPAggregateZeroOperator(this.getRelNode(), Objects.requireNonNull(function),
                    newInputs.get(0))
                    .copyAnnotations(this);
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

    // equivalent inherited from base class

    @SuppressWarnings("unused")
    public static DBSPAggregateZeroOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPAggregateZeroOperator(CalciteEmptyRel.INSTANCE, info.getFunction(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPAggregateZeroOperator.class);
    }
}
