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
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** This class represents a join followed by a filter followed by a map.
 * The operator is eventually lowered to a join_flatmap,
 * where the synthesized function
 * returns None when filter(function) is false, and Some(map(function))
 * otherwise. */
public final class DBSPJoinFilterMapOperator extends DBSPJoinBaseOperator {
    // If the following is null, the function represents the combined function/filter
    // and the function returns Option.
    @Nullable
    public final DBSPExpression filter;
    @Nullable
    public final DBSPExpression map;

    public DBSPJoinFilterMapOperator(
            CalciteRelNode node, DBSPTypeZSet outputType,
            DBSPExpression function, @Nullable DBSPExpression filter, @Nullable DBSPExpression map,
            boolean isMultiset,
            OutputPort left, OutputPort right) {
        super(node, "join_flatmap", function, outputType, isMultiset, left, right);
        this.filter = filter;
        this.map = map;
        assert left.getOutputIndexedZSetType().keyType.sameType(right.getOutputIndexedZSetType().keyType);
    }

    public DBSPExpression getFilter() {
        return Objects.requireNonNull(this.filter);
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPJoinFilterMapOperator(
                this.getRelNode(), outputType.to(DBSPTypeZSet.class),
                Objects.requireNonNull(expression), this.filter, this.map,
                this.isMultiset, this.left(), this.right()).copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPJoinFilterMapOperator(
                    this.getRelNode(), this.getOutputZSetType(),
                    this.getFunction(), this.filter, this.map,
                    this.isMultiset, newInputs.get(0), newInputs.get(1)).copyAnnotations(this);
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPJoinFilterMapOperator jfm = other.as(DBSPJoinFilterMapOperator.class);
        if (jfm == null)
            return false;
        return EquivalenceContext.equiv(this.filter, jfm.filter) &&
                EquivalenceContext.equiv(this.map, jfm.map);
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
        if (this.filter != null) {
            visitor.property("filter");
            this.filter.accept(visitor);
        }
        if (this.map != null) {
            visitor.property("map");
            this.map.accept(visitor);
        }
    }

    @Override
    public DBSPJoinBaseOperator withFunctionAndInputs(DBSPExpression function, OutputPort left, OutputPort right) {
        return new DBSPJoinFilterMapOperator(this.getRelNode(), this.getOutputZSetType(), function,
                this.filter, this.map, this.isMultiset, left, right);
    }

    @SuppressWarnings("unused")
    public static DBSPJoinFilterMapOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        DBSPExpression filter = null;
        if (node.has("filter"))
            filter = fromJsonInner(node, "filter", decoder, DBSPExpression.class);
        DBSPExpression map = null;
        if (node.has("map"))
            map = fromJsonInner(node, "map", decoder, DBSPExpression.class);
        return new DBSPJoinFilterMapOperator(
                CalciteEmptyRel.INSTANCE, info.getZsetType(), info.getFunction(),
                filter, map,
                info.isMultiset(), info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPJoinFilterMapOperator.class);
    }
}
