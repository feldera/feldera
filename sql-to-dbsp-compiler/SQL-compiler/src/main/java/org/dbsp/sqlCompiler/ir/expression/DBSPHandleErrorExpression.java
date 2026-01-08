package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeSqlResult;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/**
 * This expression is inserted very late in the circuit transformation
 * to handle (some) expressions that can lead to runtime panics.
 *
 * <p>The actual Rust representation has slightly different types
 * than this instruction; the argument type in particular will always
 * be a SqlResult[T], where T is the result type.
 */
public class DBSPHandleErrorExpression extends DBSPExpression {
    public final RuntimeBehavior runtimeBehavior;
    /** Index in the table with error messages; used for PanicWithSource; 0 if unused. */
    public final int index;
    public final DBSPExpression source;
    public final boolean hasSourcePosition;

    /** Create an expression that handles an error produced by another expression.
     *
     * @param node             Calcite node of the source expression.
     * @param runtimeBehavior  How to handle errors at runtime.
     * @param index            All error handling expressions within one operator
     *                         are densely numbered; this is the number indexing them.
     *                         0 means "unused".
     * @param source           Expression producing a SqlResult.
     * @param hasSourcePosition True if the expression can access the source position information.
     */
    public DBSPHandleErrorExpression(CalciteObject node, int index, RuntimeBehavior runtimeBehavior,
                                     DBSPExpression source, boolean hasSourcePosition) {
        super(node, resultType(source.getType()));
        this.source = source;
        this.hasSourcePosition = hasSourcePosition;

        if (source.is(DBSPCastExpression.class) &&
                source.to(DBSPCastExpression.class).safe == DBSPCastExpression.CastType.SqlSafe) {
            index = 0;
        } else if (source.is(DBSPBinaryExpression.class)) {
            // A conversion function
            index = 0;
        } else if (source.getSourcePosition().isValid() && hasSourcePosition) {
            // Use the supplied index
        } else {
            index = 0;
        }
        this.runtimeBehavior = runtimeBehavior;
        this.index = index;
    }

    static DBSPType resultType(DBSPType inputType) {
        if (inputType.is(DBSPTypeSqlResult.class)) {
            return inputType.to(DBSPTypeSqlResult.class).getWrappedType();
        }
        return inputType;
    }

    public enum RuntimeBehavior {
        /** On error, panic in Rust. */
        Panic,
        /** On error, panic in Rust, but report source position;
         * in this case the 'index' field below has a valid value. */
        PanicWithSource,
        /** On error return None; this is the behavior of e.g., safe_cast in SQL */
        ReturnNone
    }

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
                this.hasSourcePosition == o.hasSourcePosition &&
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
        return new DBSPHandleErrorExpression(this.node, this.index, this.runtimeBehavior,
                this.source.deepCopy(), this.hasSourcePosition);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPHandleErrorExpression otherExpression = other.as(DBSPHandleErrorExpression.class);
        if (otherExpression == null)
            return false;
        return this.runtimeBehavior == otherExpression.runtimeBehavior &&
                this.hasSourcePosition == otherExpression.hasSourcePosition &&
                this.index == otherExpression.index &&
                context.equivalent(this.source, otherExpression.source);
    }

    @SuppressWarnings("unused")
    public static DBSPHandleErrorExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression source = fromJsonInner(node, "source", decoder, DBSPExpression.class);
        int index = Utilities.getIntProperty(node, "index");
        RuntimeBehavior runtimeBehavior = RuntimeBehavior.valueOf(Utilities.getStringProperty(node, "runtimeBehavior"));
        boolean hasSourcePosition = Utilities.getBooleanProperty(node, "hasSourcePosition");
        return new DBSPHandleErrorExpression(CalciteObject.EMPTY, index, runtimeBehavior, source, hasSourcePosition);
    }
}
