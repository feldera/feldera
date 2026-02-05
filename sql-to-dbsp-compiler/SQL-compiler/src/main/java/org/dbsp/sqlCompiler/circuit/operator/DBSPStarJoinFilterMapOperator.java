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

/** A Join operator that has N inputs, combined with a filter and a map;
 * each input is indexed and all keys must be of the same type;
 * this operator is incremental-only.  When the operator is lowered
 * the synthesized function returns None when filter(function) is false, and Some(map(function))
 * otherwise.  So the type of the function does NOT always match the output type of the operator.*/
public class DBSPStarJoinFilterMapOperator extends DBSPStarJoinBaseOperator implements IIncremental {
    // If the following is null, the function represents the combined function/filter
    // and the function returns Option.
    @Nullable
    public final DBSPClosureExpression filter;
    @Nullable
    public final DBSPClosureExpression map;

    public DBSPStarJoinFilterMapOperator(CalciteRelNode node, DBSPType outputType, DBSPClosureExpression function,
                                         @Nullable DBSPClosureExpression filter, @Nullable DBSPClosureExpression map,
                                         boolean isMultiset, List<OutputPort> inputs) {
        super(node, "inner_star_join_flatmap", outputType, function, isMultiset, inputs);
        this.filter = filter;
        this.map = map;
    }

    @Override
    public DBSPOperator with(@Nullable DBSPExpression function, DBSPType outputType, List<OutputPort> inputs, boolean force) {
        Utilities.enforce(function != null);
        if (this.mustReplace(force, function, inputs, outputType)) {
            return new DBSPStarJoinFilterMapOperator(this.getRelNode(), outputType, function.to(DBSPClosureExpression.class),
                    this.filter, this.map, this.isMultiset, inputs).copyAnnotations(this);
        }
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPStarJoinFilterMapOperator jfm = other.as(DBSPStarJoinFilterMapOperator.class);
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
    public static DBSPStarJoinFilterMapOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        DBSPClosureExpression filter = null;
        if (node.has("filter"))
            filter = fromJsonInner(node, "filter", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression map = null;
        if (node.has("map"))
            map = fromJsonInner(node, "map", decoder, DBSPClosureExpression.class);
        return new DBSPStarJoinFilterMapOperator(
                CalciteEmptyRel.INSTANCE, info.getZsetType(), info.getClosureFunction(),
                filter, map, info.isMultiset(), info.inputs())
                .addAnnotations(info.annotations(), DBSPStarJoinFilterMapOperator.class);
    }
}
