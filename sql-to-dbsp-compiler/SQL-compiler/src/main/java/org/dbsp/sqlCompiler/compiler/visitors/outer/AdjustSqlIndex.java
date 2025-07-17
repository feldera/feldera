package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionTranslator;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeISize;

/** Converts a[index] into a[(isize)(index - 1] */
public class AdjustSqlIndex extends ExpressionTranslator {
    public AdjustSqlIndex(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public void postorder(DBSPBinaryExpression expression) {
        if (expression.opcode != DBSPOpcode.SQL_INDEX) {
            super.postorder(expression);
            return;
        }

        DBSPExpression left = this.getE(expression.left);
        DBSPExpression right = this.getE(expression.right);
        DBSPType indexType = right.getType();

        DBSPExpression sub1 = ExpressionCompiler.makeBinaryExpression(
                expression.getNode(), indexType, DBSPOpcode.SUB,
                right, indexType.to(IsNumericType.class).getOne());
        sub1 = sub1.cast(expression.getNode(), DBSPTypeISize.create(indexType.mayBeNull), false);
        Simplify simplify = new Simplify(this.compiler);
        DBSPExpression index = simplify.apply(sub1).to(DBSPExpression.class);
        DBSPExpression result = new DBSPBinaryExpression(
                expression.getNode(), expression.type, DBSPOpcode.RUST_INDEX, left, index);
        this.map(expression, result);
    }
}
