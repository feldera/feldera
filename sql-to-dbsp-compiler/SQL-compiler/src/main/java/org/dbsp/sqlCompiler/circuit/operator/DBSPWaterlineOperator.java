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
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** Given a stream, it computes function(extractTS(stream), delay(this, init)).
 * This operator is special: the output is replicated for all workers.
 * See the comments for {@link DBSPApplyOperator}. */
public final class DBSPWaterlineOperator extends DBSPUnaryOperator {
    /** Initial value of waterline; a closure with 0 parameters */
    public final DBSPClosureExpression init;
    /** Function which extracts a timestamp; a closure with two parameters;
     * currently the second parameter is never used, and should always have the type &() */
    public final DBSPClosureExpression extractTs;

    public DBSPWaterlineOperator(CalciteRelNode node, DBSPClosureExpression init,
                                 DBSPClosureExpression extractTs,
                                 DBSPClosureExpression function, OutputPort input) {
        super(node, "waterline", function, function.getResultType(),
                false, input);
        this.init = init;
        this.extractTs = extractTs;
        Utilities.enforce(init.parameters.length == 0);
        Utilities.enforce(extractTs.parameters.length == 2);
        Utilities.enforce(function.parameters.length == 2);
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPWaterlineOperator(this.getRelNode(), this.init,
                    this.extractTs, toClosure(function), newInputs.get(0))
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
                CalciteEmptyRel.INSTANCE, init, extractTs, info.getClosureFunction(),
                info.getInput(0))
                .addAnnotations(info.annotations(), DBSPWaterlineOperator.class);
    }
}
