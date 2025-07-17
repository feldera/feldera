package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/**
 * This expression is inserted very late in the circuit transformation
 * to handle (some) expressions that can lead to runtime panics.
 * Today it is only used for casts, but we hope to expand its uses.
 *
 * <p>The actual Rust representation has slightly different types
 * than this instruction; the argument type in particular will always
 * be a SqlResult[T], where T is the result type.
 */
public class DBSPHandleErrorExpression extends DBSPExpression {
    /** Create an expression that handles an error produced by another expression.
     *
     * @param node             Calcite node of the source expression.
     * @param index            All error handling expressions within one operator
     *                         are densely numbered; this is the number indexing them.
     * @param source           Expression producing a SqlResult.
     */
    public DBSPHandleErrorExpression(CalciteObject node, int index, DBSPExpression source) {
        super(node, source.getType());
        this.source = source;

        RuntimeBehavior behavior;
        if (source.is(DBSPCastExpression.class) && source.to(DBSPCastExpression.class).safe) {
            index = 0;
            behavior = RuntimeBehavior.ReturnNone;
        } else if (source.getSourcePosition().isValid()) {
            behavior = RuntimeBehavior.PanicWithSource;
        } else {
            index = 0;
            behavior = RuntimeBehavior.Panic;
        }
        this.runtimeBehavior = behavior;
        this.index = index;
    }

    public enum RuntimeBehavior {
        /** On error, panic in Rust. */
        Panic,
        /** On error, panic in Rust, but report source position;
         * in this case the 'index' field below has a valid value. */
        PanicWithSource,
        /** On error return None; this is the behavior of e.g., safe_cast in SQL */
        ReturnNone
    };

    public final RuntimeBehavior runtimeBehavior;
    /** Index in the table with error messages; used for PanicWithSource; 0 if unused. */
    public final int index;
    public final DBSPExpression source;

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("source");
        this.source.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPHandleErrorExpression o = other.as(DBSPHandleErrorExpression.class);
        if (o == null)
            return false;
        return this.source == o.source &&
                this.runtimeBehavior == o.runtimeBehavior &&
                this.index == o.index;
    }

    public String getFunction() {
        return switch (this.runtimeBehavior) {
            case Panic -> "handle_error";
            case PanicWithSource -> "handle_error_with_position";
            case ReturnNone -> "handle_error_safe";
        };
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.getFunction())
                .append("(")
                .append(this.source)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPHandleErrorExpression(this.node, this.index, this.source.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPHandleErrorExpression otherExpression = other.as(DBSPHandleErrorExpression.class);
        if (otherExpression == null)
            return false;
        return this.runtimeBehavior == otherExpression.runtimeBehavior &&
                this.index == otherExpression.index &&
                context.equivalent(this.source, otherExpression.source);
    }

    @SuppressWarnings("unused")
    public static DBSPHandleErrorExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression source = fromJsonInner(node, "source", decoder, DBSPExpression.class);
        int index = Utilities.getIntProperty(node, "index");
        return new DBSPHandleErrorExpression(CalciteObject.EMPTY, index, source);
    }
}
