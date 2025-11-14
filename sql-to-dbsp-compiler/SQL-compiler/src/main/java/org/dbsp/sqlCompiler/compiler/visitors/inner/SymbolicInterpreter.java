package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

/** A symbolic interpreter evaluates each expression to a symbolic value of type T.
 * It also maintains a symbolic value for each variable. */
public class SymbolicInterpreter<T> extends TranslateVisitor<T> {
    /** Current value for each named object. */
    protected final Scopes<IDBSPDeclaration, T> currentValue;

    public SymbolicInterpreter(DBSPCompiler compiler) {
        super(compiler);
        this.currentValue = new Scopes<>();
    }

    @Override
    public VisitDecision preorder(DBSPType type) {
        return VisitDecision.STOP;
    }

    @Nullable
    protected T getCurrentValue(IDBSPDeclaration decl) {
        return this.currentValue.get(decl);
    }

    protected void setCurrentValue(IDBSPDeclaration decl, T value) {
        this.currentValue.substitute(decl, value);
    }

    @Override
    public VisitDecision preorder(DBSPBlockExpression block) {
        this.currentValue.newContext();
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPBlockExpression block) {
        this.currentValue.popContext();
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        this.currentValue.newContext();
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPClosureExpression expression) {
        this.currentValue.popContext();
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        super.startVisit(node);
        this.currentValue.clear();
        this.currentValue.newContext();
    }

    @Override
    public void endVisit() {
        this.currentValue.popContext();
        this.currentValue.mustBeEmpty();
        super.endVisit();
    }
}
