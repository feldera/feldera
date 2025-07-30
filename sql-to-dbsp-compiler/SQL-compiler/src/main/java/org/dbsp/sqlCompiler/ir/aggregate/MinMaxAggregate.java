package org.dbsp.sqlCompiler.ir.aggregate;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** High-level representation of an aggregate that is a call to a Min, Max,
 * ArgMin or ArgMax function. This is lowered later into a concrete implementation. */
public class MinMaxAggregate extends NonLinearAggregate {
    public enum Operation {
        Min,
        Max,
        ArgMin,
        ArgMax,
    }
    public final Operation operation;
    /** A closure with signature |row| -> { compared value }, where 'row'
     * has the same type as the row variable (second parameter of the increment).
     * For min and max this is the argument value; for arg_min and arg_max this
     * contains both arguments.  This is currently only used for optimizing Min and Max
     * aggregate implementations. */
    public final DBSPClosureExpression comparedValue;

    public MinMaxAggregate(CalciteObject origin, DBSPExpression zero, DBSPClosureExpression increment,
                           DBSPExpression emptySetResult, DBSPTypeUser semigroup,
                           DBSPClosureExpression comparedValue,
                           // Only non-null for arg_max and arg_min
                           @Nullable DBSPClosureExpression postProcess,
                           Operation operation) {
        super(origin, zero, increment, postProcess, emptySetResult, semigroup);
        this.comparedValue = comparedValue;
        this.operation = operation;
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
        if (this.postProcess != null) {
            visitor.property("postProcess");
            this.postProcess.accept(visitor);
        }
        visitor.property("emptySetResult");
        this.emptySetResult.accept(visitor);
        visitor.property("aggregatedValue");
        this.comparedValue.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean compatible(IAggregate other, boolean appendOnlySources) {
        return appendOnlySources && other.is(MinMaxAggregate.class);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append(this.operation.toString());
        builder.append("[").increase();
        builder.append("zero=")
                .append(this.zero)
                .newline()
                .append("increment=")
                .append(this.increment);
        if (this.postProcess != null) {
            builder.newline()
                    .append("postProcess=")
                    .append(this.postProcess);
        }
        builder.newline()
                .append("emptySetResult=")
                .append(this.emptySetResult)
                .newline()
                .append("semigroup=")
                .append(this.semigroup)
                .newline()
                .append("comparedValue=")
                .append(this.comparedValue);
        builder.newline().decrease().append("]");
        return builder;
    }

    @Override
    public List<DBSPParameter> getRowVariableReferences() {
        return Linq.list(this.increment.parameters[1], this.comparedValue.parameters[0]);
    }

    @SuppressWarnings("unused")
    public static MinMaxAggregate fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression zero = fromJsonInner(node, "zero", decoder, DBSPExpression.class);
        DBSPClosureExpression increment = fromJsonInner(node, "increment", decoder, DBSPClosureExpression.class);
        DBSPExpression emptySetResult = fromJsonInner(node, "emptySetResult", decoder, DBSPExpression.class);
        DBSPTypeUser semigroup = fromJsonInner(node, "semigroup", decoder, DBSPTypeUser.class);
        DBSPClosureExpression comparedValue = fromJsonInner(node, "comparedValue", decoder, DBSPClosureExpression.class);
        DBSPClosureExpression postProcessing = null;
        if (node.has("postProcessing"))
            postProcessing = fromJsonInner(node, "postProcessing", decoder, DBSPClosureExpression.class);
        Operation operation = Operation.valueOf(Utilities.getStringProperty(node, "operation"));
        return new MinMaxAggregate(CalciteObject.EMPTY, zero, increment, emptySetResult, semigroup, comparedValue, postProcessing, operation);
    }
}
