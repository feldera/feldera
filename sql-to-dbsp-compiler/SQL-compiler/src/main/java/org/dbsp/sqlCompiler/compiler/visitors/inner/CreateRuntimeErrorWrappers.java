package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPHandleErrorExpression;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Creates {@link org.dbsp.sqlCompiler.ir.expression.DBSPHandleErrorExpression}
 * expressions and allocates an index for each one of them.  Each index
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
        // Wrap the cast into an error handler
        DBSPHandleErrorExpression handler = new DBSPHandleErrorExpression(
<<<<<<< Updated upstream
                expression.getNode(), this.getIndex(expression.getSourcePosition().start), cast,
                // source code may not be available outside an operator
                this.operatorContext != null);
=======
<<<<<<< Updated upstream
                expression.getNode(), this.getIndex(expression.getSourcePosition().start), cast);
=======
                expression.getNode(), this.getIndex(expression.getSourcePosition().start), cast,
                // source code may not be available outside an operator
                true);
>>>>>>> Stashed changes
>>>>>>> Stashed changes
        this.map(expression, handler);
    }

    public static DBSPExpression process(DBSPCompiler compiler, DBSPExpression expression) {
        CreateRuntimeErrorWrappers wrap = new CreateRuntimeErrorWrappers(compiler);
        return wrap.apply(expression).to(DBSPExpression.class);
    }
}
