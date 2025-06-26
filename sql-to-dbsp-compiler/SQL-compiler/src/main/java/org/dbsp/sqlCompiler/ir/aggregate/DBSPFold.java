package org.dbsp.sqlCompiler.ir.aggregate;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeSemigroup;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** Represents an expression corresponding to a DBSP Fold construct */
public class DBSPFold extends DBSPAggregator {
    public final DBSPExpression zero;
    public final DBSPClosureExpression increment;
    public final DBSPClosureExpression postProcess;
    public final DBSPTypeUser semigroup;

    public DBSPFold(
            CalciteObject node, DBSPTypeUser semigroup,
            DBSPExpression zero, DBSPClosureExpression increment,
            DBSPClosureExpression postProcess) {
        super(node, computeFoldType(increment, postProcess));
        this.zero = zero;
        this.increment = increment;
        this.postProcess = postProcess;
        this.semigroup = semigroup;
        Utilities.enforce(increment.parameters.length == 3);
        Utilities.enforce(zero.getType().sameType(increment.parameters[0].getType().deref()));
    }

    static DBSPTypeFunction computeFoldType(DBSPClosureExpression increment, DBSPClosureExpression postProcess) {
        return new DBSPTypeFunction(postProcess.getResultType(), increment.parameters[1].getType());
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPFold(this.node, this.semigroup,
                this.zero.deepCopy(), this.increment.deepCopy().to(DBSPClosureExpression.class),
                this.postProcess.deepCopy().to(DBSPClosureExpression.class));
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPFold o = other.as(DBSPFold.class);
        if (o == null)
            return false;
        return this.semigroup.sameType(o.semigroup) &&
                context.equivalent(this.zero, o.zero) &&
                context.equivalent(this.increment, o.increment) &&
                context.equivalent(this.postProcess, o.increment);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("semigroup");
        this.semigroup.accept(visitor);
        visitor.property("zero");
        this.zero.accept(visitor);
        visitor.property("increment");
        this.increment.accept(visitor);
        visitor.property("postProcess");
        this.postProcess.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPFold o = other.as(DBSPFold.class);
        if (o == null)
            return false;
        return this.zero == o.zero && this.postProcess == o.postProcess &&
                this.semigroup == o.semigroup && this.increment == o.increment;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("Fold::with_output(")
                .append(this.zero)
                .append(", ")
                .append(this.increment)
                .append(", ")
                .append(this.postProcess)
                .append(")");
    }

    @SuppressWarnings("unused")
    public static DBSPFold fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPTypeSemigroup semigroup = DBSPTypeSemigroup.fromJson(node, decoder);
        DBSPExpression zero = fromJsonInner(node, "zero", decoder, DBSPExpression.class);
        DBSPClosureExpression increment = fromJsonInner(node, "increment", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression postProcessing = fromJsonInner(
                node, "postProcessing", decoder, DBSPClosureExpression.class);
        return new DBSPFold(CalciteObject.EMPTY, semigroup, zero, increment, postProcessing);
    }
}
