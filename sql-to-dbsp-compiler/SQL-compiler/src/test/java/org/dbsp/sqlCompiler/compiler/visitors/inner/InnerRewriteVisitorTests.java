package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link InnerRewriteVisitor}. */
public class InnerRewriteVisitorTests extends BaseSQLTests {
    final ExpressionBuilder b = new ExpressionBuilder();

    /** Replaces every 32-bit integer literal by 0. */
    static class ZeroLiterals extends InnerRewriteVisitor {
        ZeroLiterals(DBSPCompiler compiler) {
            super(compiler, false);
        }

        @Override
        public VisitDecision preorder(DBSPI32Literal literal) {
            this.map(literal, new DBSPI32Literal(0));
            return VisitDecision.STOP;
        }
    }

    /** A call rebuilt because one of its arguments changed keeps the source position of the call. */
    @Test
    public void rewrittenCallKeepsItsPosition() {
        CalciteObject position = CalciteObject.create(
                new SourcePositionRange(new SourcePosition(3, 20), new SourcePosition(3, 36)));
        DBSPApplyExpression call = new DBSPApplyExpression(position, "f", b.i32(), new DBSPI32Literal(1));
        IDBSPInnerNode rewritten = new ZeroLiterals(this.testCompiler()).apply(call);
        Assert.assertNotSame(call, rewritten);
        Assert.assertSame(position, rewritten.getNode());
    }
}
