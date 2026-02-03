package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.util.Utilities;

record WindowBound(boolean inclusive, DBSPExpression expression) {
    WindowBound combine(WindowBound with, boolean lower) {
        Utilities.enforce(this.inclusive == with.inclusive);
        DBSPOpcode opcode = lower ? DBSPOpcode.MIN : DBSPOpcode.MAX;
        DBSPExpression expression = ExpressionCompiler.makeBinaryExpression(this.expression.getNode(),
                this.expression.getType(), opcode, this.expression, with.expression);
        return new WindowBound(this.inclusive, expression);
    }

    public String toString() {
        return this.expression.toString();
    }
}
