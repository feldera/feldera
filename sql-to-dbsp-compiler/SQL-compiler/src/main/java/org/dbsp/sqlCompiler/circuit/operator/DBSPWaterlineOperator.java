package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Given a stream, it computes function(extractTS(stream), delay(this, init)).
 * This operator is special: the output is replicated for all workers.
 * See the comments for {@link DBSPApplyOperator}. */
public final class DBSPWaterlineOperator extends DBSPUnaryOperator {
    /** Initial value of waterline; a closure with 0 parameters */
    public final DBSPClosureExpression init;
    /** Function which extracts a timestamp; a closure with two parameters;
     * currently the second parameter is never used, and should always have the type &() */
    public final DBSPClosureExpression extractTs;

    public DBSPWaterlineOperator(CalciteObject node, DBSPClosureExpression init,
                                 DBSPClosureExpression extractTs,
                                 DBSPClosureExpression function, OutputPort input) {
        super(node, "waterline", function, function.getResultType(),
                false, input);
        this.init = init;
        this.extractTs = extractTs;
        assert init.parameters.length == 0;
        assert extractTs.parameters.length == 2;
        assert function.parameters.length == 2;
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPWaterlineOperator(this.getNode(), this.init,
                this.extractTs,
                Objects.requireNonNull(expression).to(DBSPClosureExpression.class),
                this.input()).copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPWaterlineOperator(this.getNode(), this.init,
                    this.extractTs,
                    this.getClosureFunction(),
                    newInputs.get(0))
                    .copyAnnotations(this);
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

    @Override
    public void accept(InnerVisitor visitor) {
        super.accept(visitor);
        visitor.property("init");
        this.init.accept(visitor);
        visitor.property("extractTs");
        this.extractTs.accept(visitor);
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPWaterlineOperator otherOperator = other.as(DBSPWaterlineOperator.class);
        if (otherOperator == null)
            return false;
        return this.init.equivalent(otherOperator.init);
    }

    @SuppressWarnings("unused")
    public static DBSPWaterlineOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        DBSPClosureExpression init = fromJsonInner(node, "init", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression extractTs = fromJsonInner(node, "extractTs", decoder, DBSPClosureExpression.class);
        return new DBSPWaterlineOperator(
                CalciteObject.EMPTY, init, extractTs, info.getClosureFunction(),
                info.getInput(0))
                .addAnnotations(info.annotations(), DBSPWaterlineOperator.class);
    }
}
