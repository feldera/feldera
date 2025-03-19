package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionsCSE;
import org.dbsp.sqlCompiler.compiler.visitors.inner.IRTransform;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ValueNumbering;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.Logger;

/** Perform common-subexpression elimination on the inner IR */
public class InnerCSE implements IRTransform {
    final ValueNumbering numbering;
    final ExpressionsCSE cse;

    /** Return 'true' for operators where we want to apply CSE */
    public static boolean process(DBSPOperator operator) {
        return !operator.is(DBSPConstantOperator.class);
    }

    InnerCSE(DBSPCompiler compiler) {
        this.numbering = new ValueNumbering(compiler);
        this.cse = new ExpressionsCSE(compiler, this.numbering.canonical);
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        if (!node.is(DBSPExpression.class))
            return node;
        this.numbering.apply(node);
        if (!this.numbering.foundAssignment) {
            return this.cse.apply(node);
        } else {
            Logger.INSTANCE.belowLevel("ExpressionsCSE", 1)
                    .append("Skipping ")
                    .appendSupplier(node::toString);
            return node;
        }
    }

    @Override
    public String toString() {
        return "InnerCSE";
    }

    @Override
    public void setOperatorContext(DBSPOperator operator) {
        this.numbering.setOperatorContext(operator);
        this.cse.setOperatorContext(operator);
    }
}
