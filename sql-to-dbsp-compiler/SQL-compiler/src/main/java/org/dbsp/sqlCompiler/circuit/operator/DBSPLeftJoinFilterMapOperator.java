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
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** This class represents a left join followed by a filter followed by a map.
 * The operator is eventually lowered to a left_join_flatmap,
 * where the synthesized function
 * returns None when filter(function) is false, and Some(map(function))
 * otherwise. */
public final class DBSPLeftJoinFilterMapOperator extends DBSPJoinBaseOperator {
    // If the following is null, the function represents the combined function/filter
    // and the function returns Option.
    @Nullable
    public final DBSPClosureExpression filter;
    @Nullable
    public final DBSPClosureExpression map;

    public DBSPLeftJoinFilterMapOperator(
            CalciteRelNode node, DBSPTypeZSet outputType,
            DBSPExpression function, @Nullable DBSPClosureExpression filter, @Nullable DBSPClosureExpression map,
            boolean isMultiset,
            OutputPort left, OutputPort right) {
        super(node, "left_join_flatmap", function, outputType, isMultiset, left, right);
        this.filter = filter;
        this.map = map;
        Utilities.enforce(left.getOutputIndexedZSetType().keyType.sameType(right.getOutputIndexedZSetType().keyType));
        Utilities.enforce(right.getOutputIndexedZSetType().elementType.mayBeNull);
    }

    public DBSPExpression getFilter() {
        return Objects.requireNonNull(this.filter);
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPLeftJoinFilterMapOperator(
                    this.getRelNode(), outputType.to(DBSPTypeZSet.class),
                    Objects.requireNonNull(function), this.filter, this.map,
                    this.isMultiset, newInputs.get(0), newInputs.get(1)).copyAnnotations(this);
        }
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPLeftJoinFilterMapOperator jfm = other.as(DBSPLeftJoinFilterMapOperator.class);
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

    @SuppressWarnings("unused")
    public static DBSPLeftJoinFilterMapOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        DBSPClosureExpression filter = null;
        if (node.has("filter"))
            filter = fromJsonInner(node, "filter", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression map = null;
        if (node.has("map"))
            map = fromJsonInner(node, "map", decoder, DBSPClosureExpression.class);
        return new DBSPLeftJoinFilterMapOperator(
                CalciteEmptyRel.INSTANCE, info.getZsetType(), info.getFunction(),
                filter, map,
                info.isMultiset(), info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPLeftJoinFilterMapOperator.class);
    }
}
