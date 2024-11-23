package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.ExpressionTree;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionTreeTests {
    @Test
    public void testTree() {
        DBSPType i32 = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true);
        DBSPExpression expression =
                new DBSPTupleExpression(
                        new DBSPI32Literal(20),
                        new DBSPBinaryExpression(
                                CalciteObject.EMPTY,
                                i32,
                                DBSPOpcode.ADD,
                                i32.var(),
                                new DBSPI32Literal(6)));
        String tree = ExpressionTree.asTree(expression);
        Assert.assertTrue(tree.contains("DBSPBinaryExpression"));
    }
}
