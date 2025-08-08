package org.dbsp.sqlCompiler.ir.aggregate;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/**
 * A linear aggregate is compiled as two functions and a constant
 * - a function 'map' from the row to a value of a group
 * - a 'postProcess' function that is applied to the final result of aggregation
 * - 'emptySetResult' is the result returned by a top-level aggregate (no group-by) for an empty set.
 * Notice that the postProcess function is *NOT* invoked if the result
 * of the aggregation is zero.  That's why in general the 'map'
 * function has to produce one extra result, which is the count of
 * elements in the group.  Only if this count is zero we know that the
 * group is empty.
 *
 * <p>For example, to compute SUM(x) we use the following linear aggregate:
 * - map = |r: &T| -> (T, i64) { (r.x != null ? r.x : 0, 1) }
 * - postProcess = |x: (T, i64)| -> T { x.0 }
 * - emptySetResult = None
 * Note that the postProcess parameter type is not a reference
 */
public class LinearAggregate extends IAggregate {
    public final DBSPClosureExpression map;
    public final DBSPClosureExpression postProcess;
    public final DBSPExpression emptySetResult;

    public LinearAggregate(
            CalciteObject origin,
            DBSPClosureExpression map,
            DBSPClosureExpression postProcess,
            DBSPExpression emptySetResult) {
        super(origin, emptySetResult.getType());
        this.map = map;
        this.postProcess = postProcess;
        this.emptySetResult = emptySetResult;

        DBSPType mapType = this.map.body.getType();
        Utilities.enforce(postProcess.parameters.length == 1);
        Utilities.enforce(map.parameters.length == 1);
        Utilities.enforce(mapType.sameType(postProcess.parameters[0].getType()));
    }

    /** Given an DBSPAggregate where all Implementation objects have a linearFunction
     * component, combine these linear functions into a single one. */
    public static LinearAggregate combine(
            CalciteObject node, DBSPCompiler compiler,
            DBSPVariablePath rowVar, List<LinearAggregate> aggregates) {
        // Essentially runs all the aggregates in the list in parallel
        DBSPParameter parameter = rowVar.asParameter();
        List<DBSPExpression> bodies = Linq.map(aggregates, c -> c.map.body);
        DBSPTupleExpression tuple = new DBSPTupleExpression(bodies, false);
        DBSPClosureExpression map = tuple.closure(parameter);
        // Zero
        DBSPExpression zero = new DBSPTupleExpression(
                Linq.map(aggregates, IAggregate::getEmptySetResult), false);
        // Post
        List<DBSPType> paramTypes = Linq.map(aggregates, c -> c.postProcess.parameters[0].getType());
        DBSPVariablePath postParam = new DBSPTypeTuple(node, paramTypes).var();
        List<DBSPExpression> posts = new ArrayList<>();
        for (int i = 0; i < aggregates.size(); i++) {
            posts.add(aggregates.get(i).postProcess.call(postParam.field(i)));
        }
        tuple = new DBSPTupleExpression(posts, false);
        DBSPClosureExpression post = tuple.closure(postParam)
                .reduce(compiler).to(DBSPClosureExpression.class);
        return new LinearAggregate(node, map, post, zero);
    }

    public void validate() {
        // These validation rules actually don't apply for window-based aggregates.
        DBSPType emptyResultType = this.emptySetResult.getType();
        DBSPType postProcessType = this.postProcess.getResultType();
        if (!emptyResultType.sameType(postProcessType)) {
            throw new InternalCompilerError("Post-process result type " + postProcessType +
                    " different from empty set type " + emptyResultType, this);
        }
    }

    /** Result produced for an empty set */
    public DBSPExpression getEmptySetResult() {
        return this.emptySetResult;
    }

    @Override
    public boolean isLinear() {
        return true;
    }

    @Override
    public List<DBSPParameter> getRowVariableReferences() {
        return Linq.list(this.map.parameters[0]);
    }

    @Override
    public boolean compatible(IAggregate other) {
        return other.is(LinearAggregate.class);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("map");
        this.map.accept(visitor);
        visitor.property("postProcess");
        this.postProcess.accept(visitor);
        visitor.property("emptySetResult");
        this.emptySetResult.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    public DBSPClosureExpression getPostprocessing() {
        return this.postProcess;
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        LinearAggregate o = other.as(LinearAggregate.class);
        if (o == null)
            return false;
        return this.map == o.map &&
                this.postProcess == o.postProcess &&
                this.emptySetResult == o.emptySetResult;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("[").increase();
        builder.append("increment=")
                .append(this.map)
                .newline()
                .append("postProcess=")
                .append(this.postProcess)
                .newline()
                .append("emptySetResult=")
                .append(this.emptySetResult)
                .newline();
        builder.newline().decrease().append("]");
        return builder;
    }

    public boolean equivalent(EquivalenceContext context, LinearAggregate other) {
        return context.equivalent(this.map, other.map) &&
                context.equivalent(this.postProcess, other.postProcess) &&
                context.equivalent(this.emptySetResult, other.emptySetResult);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new LinearAggregate(
                this.getNode(),
                this.map.deepCopy().to(DBSPClosureExpression.class),
                this.postProcess.deepCopy().to(DBSPClosureExpression.class),
                this.emptySetResult.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        return false;
    }

    @SuppressWarnings("unused")
    public static LinearAggregate fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPClosureExpression map = fromJsonInner(node, "map", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression postProcess = fromJsonInner(node, "postProcess", decoder, DBSPClosureExpression.class);
        DBSPExpression emptySetResult = fromJsonInner(node, "emptySetResult", decoder, DBSPExpression.class);
        return new LinearAggregate(CalciteObject.EMPTY, map, postProcess, emptySetResult);
    }
}
