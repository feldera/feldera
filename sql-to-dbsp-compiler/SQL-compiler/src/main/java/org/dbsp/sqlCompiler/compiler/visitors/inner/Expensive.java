package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.intern.InternInner;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;

/** Visitor which detects whether an expression contains "expensive" subexpressions.
 * Today any external function call is deemed expensive.
 * Also, an expression is expensive if it is very large. */
public class Expensive extends InnerVisitor {
    boolean expensive;
    int size;
    // An expression that has more than 100 terms is deemed "expensive"
    static final int SIZE_THRESHOLD = 100;

    public Expensive(DBSPCompiler compiler) {
        super(compiler);
        this.expensive = false;
        this.size = 0;
    }

    public boolean isExpensive() {
        return this.expensive || this.size > SIZE_THRESHOLD;
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        super.startVisit(node);
        this.expensive = false;
    }

    @Override
    public VisitDecision preorder(DBSPType type) {
        // Do not traverse types
        return VisitDecision.STOP;
    }

    @Override
    public void push(IDBSPInnerNode node) {
        super.push(node);
        this.size++;
    }

    @Override
    public VisitDecision preorder(DBSPApplyExpression expression) {
        String functionName = expression.getFunctionName();
        if (functionName != null &&
                (functionName.startsWith("tumble_") ||
                functionName.equals(InternInner.internFunction) ||
                functionName.equals(InternInner.uninternFunction))) {
            // These functions are actually not expensive.
            return VisitDecision.STOP;
        }
        this.expensive = true;
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPApplyMethodExpression unused) {
        this.expensive = true;
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBinaryExpression expression) {
        // Lowered to a runtime function call
        if (expression.opcode == DBSPOpcode.VARIANT_INDEX) {
            this.expensive = true;
            return VisitDecision.STOP;
        }
        return super.preorder(expression);
    }

    @Override
    public VisitDecision preorder(DBSPCastExpression expression) {
        // VARIANT casts are lowered to runtime function calls
        if (expression.getType().is(DBSPTypeVariant.class) ||
                expression.source.getType().is(DBSPTypeVariant.class)) {
            this.expensive = true;
            return VisitDecision.STOP;
        }
        return super.preorder(expression);
    }

    public static boolean isExpensive(DBSPCompiler compiler, DBSPExpression expression) {
        Expensive expensive = new Expensive(compiler);
        expensive.apply(expression);
        return expensive.isExpensive();
    }
}
