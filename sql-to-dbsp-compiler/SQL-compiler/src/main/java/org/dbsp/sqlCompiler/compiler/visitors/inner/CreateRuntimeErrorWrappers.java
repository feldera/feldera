package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts.ExpandSafeCasts;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts.ExpandUnsafeCasts;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPHandleErrorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Creates {@link DBSPHandleErrorExpression}
 * expressions from {@link DBSPCastExpression}s that are unwrapping an error.
 * Allocates an index for each such expression.  Each index
 * represents a different source position within the same operator. */
public class CreateRuntimeErrorWrappers extends ExpressionTranslator {
    record OperatorAndPosition(Long id, SourcePosition pos) {}
    ///  Map operator id to maximum index used so far in operator
    final Map<Long, Integer> maxAllocation = new HashMap<>();
    final Map<OperatorAndPosition, Integer> allocationMap = new HashMap<>();

    public CreateRuntimeErrorWrappers(DBSPCompiler compiler) {
        super(compiler);
    }

    int getIndex(SourcePosition position) {
        if (!position.isValid())
            return 0;

        OperatorAndPosition op;
        long operatorId;
        if (this.operatorContext == null) {
            operatorId = 0;
        } else {
            operatorId = this.operatorContext.id;
        }
        op = new OperatorAndPosition(operatorId, position);
        if (this.allocationMap.containsKey(op)) {
            return this.allocationMap.get(op);
        }

        int result;
        if (!this.maxAllocation.containsKey(operatorId)) {
            result = 0;
            Utilities.putNew(this.maxAllocation, operatorId, 1);
        } else {
            result = Utilities.getExists(this.maxAllocation, operatorId);
            this.maxAllocation.put(operatorId, result + 1);
        }
        Utilities.putNew(this.allocationMap, op, result);
        return result;
    }

    @Override
    protected void set(IDBSPInnerNode node, IDBSPInnerNode translation) {
        if (this.translationMap.containsKey(node)) {
            return;
        }
        this.translationMap.putNew(node, translation);
    }

    @Override
    public void postorder(DBSPCastExpression expression) {
        if (this.translationMap.containsKey(expression))
            return;
        DBSPExpression source = this.getE(expression.source);
        DBSPExpression cast = new DBSPCastExpression(expression.getNode(), source, expression.getType(), expression.safe);
        if (!expression.safe.isUnwrap()) {
            this.map(expression, cast);
            return;
        }

        // Wrap the cast into an error handler
        final boolean hasSourcePosition = this.operatorContext != null;
        final DBSPHandleErrorExpression.RuntimeBehavior behavior;
        if (expression.safe.isSafe()) {
            behavior = DBSPHandleErrorExpression.RuntimeBehavior.ReturnNone;
        } else if (source.getSourcePosition().isValid() && hasSourcePosition) {
            behavior = DBSPHandleErrorExpression.RuntimeBehavior.PanicWithSource;
        } else {
            behavior = DBSPHandleErrorExpression.RuntimeBehavior.Panic;
        }
        DBSPHandleErrorExpression handler = new DBSPHandleErrorExpression(
                    expression.getNode(), this.getIndex(expression.getSourcePosition().start), behavior, source,
                    hasSourcePosition);
        this.map(expression, handler);
    }

    @Override
    public void postorder(DBSPUnwrapExpression expression) {
        if (this.translationMap.containsKey(expression))
            return;
        DBSPExpression source = this.getE(expression.expression);
        DBSPExpression unwrap = new DBSPUnwrapExpression(expression.message, source.applyCloneIfNeeded());
        if (expression.neverFails()) {
            this.map(expression, unwrap);
            return;
        }
        final DBSPHandleErrorExpression.RuntimeBehavior behavior;
        final boolean hasSourcePosition = this.operatorContext != null;
        if (source.getSourcePosition().isValid() && hasSourcePosition) {
            behavior = DBSPHandleErrorExpression.RuntimeBehavior.PanicWithSource;
        } else {
            behavior = DBSPHandleErrorExpression.RuntimeBehavior.Panic;
        }
        DBSPHandleErrorExpression handler = new DBSPHandleErrorExpression(
                expression.getNode(), this.getIndex(expression.getSourcePosition().start),
                behavior, unwrap, hasSourcePosition);
        this.map(expression, handler);
    }

    /** Expand casts and build handlers for the produced errors */
    public static DBSPExpression wrapCasts(DBSPCompiler compiler, DBSPExpression expression) {
        ExpandSafeCasts expandSafe = new ExpandSafeCasts(compiler);
        ExpandUnsafeCasts expand = new ExpandUnsafeCasts(compiler);
        Simplify simplify = new Simplify(compiler);
        CreateRuntimeErrorWrappers wrap = new CreateRuntimeErrorWrappers(compiler);
        var simplified = simplify.apply(expression).to(DBSPExpression.class);
        var expanded = expandSafe.fixedPoint(simplified, 100);
        expanded = expand.fixedPoint(expanded, 100);
        var wrapped = wrap.apply(expanded);
        return wrapped.to(DBSPExpression.class);
    }
}
