package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

public class InternTests extends SqlIoTest {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.incrementalize = true;
        return options;
    }

    @Test
    public void testRecursive() {
        this.getCCS("""
                CREATE TABLE T(X VARCHAR INTERNED);
                DECLARE RECURSIVE VIEW V(v VARCHAR INTERNED);
                CREATE VIEW V AS SELECT v FROM V UNION SELECT X FROM T;
                """);
    }

    @Test
    public void testSort() {
        this.getCCS("""
                CREATE TABLE T(x VARCHAR INTERNED, y INT);
                CREATE VIEW V AS SELECT * FROM T ORDER BY x;""");
    }

    @Test
    public void testInterning() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT, s VARCHAR INTERNED, u VARCHAR);
                CREATE VIEW V AS SELECT MAX(u), SUM(x), s FROM T GROUP BY s;""");
        ccs.step("INSERT INTO T VALUES(0, 'a', 'b');", """
                 max | sum | s| weight
                ------------------------
                 b|      0 | a|  1""");
        ccs.step("INSERT INTO T VALUES(1, 'd', 'c');", """
                 max | sum | s| weight
                -----------------------
                 c|      1 | d| 1""");
        ccs.step("INSERT INTO T VALUES(2, 'a', 'c');", """
                 max| sum | s| weight
                ----------------------
                 b|     0 | a| -1
                 c|     2 | a| 1""");
        ccs.step("INSERT INTO T VALUES(NULL, NULL, NULL);", """
                 max | sum | s | weight
                ------------------------
                NULL |NULL |NULL | 1""");
    }

    @Test
    public void testUnion() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT, s VARCHAR INTERNED, u VARCHAR);
                CREATE TABLE S(x INT, s VARCHAR, u VARCHAR NOT NULL INTERNED);
                CREATE VIEW V AS SELECT * FROM T UNION ALL SELECT * FROM S;""");
        ccs.step("INSERT INTO T VALUES(0, 'a', 'b');", """
                 x | s| u| weight
                ----------------------
                 0 | a| b| 1""");
        ccs.step("INSERT INTO T VALUES(1, 'd', 'c');", """
                 x | s| u| weight
                ----------------------
                 1 | d| c| 1""");
        ccs.step("INSERT INTO S VALUES(2, 'a', 'c');", """
                 x | s| su| weight
                -----------------------
                 2 | a| c| 1""");
    }

    @Test
    public void nonStringInterned() {
        var compiler = this.testCompiler();
        compiler.options.ioOptions.quiet = false;
        compiler.submitStatementsForCompilation("""
                CREATE TABLE T(x INT INTERNED);
                CREATE VIEW V AS SELECT * FROM T;""");
        TestUtil.assertMessagesContain(compiler, "Illegal type interned");
    }

    @Test
    public void testUnnest() {
        this.getCCS("""
                CREATE TABLE T(x INT ARRAY, s VARCHAR INTERNED);
                CREATE VIEW V AS SELECT s, unnested FROM T, UNNEST(x) AS unnested;""");
    }

    @Test
    public void testTwoColumns() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x VARCHAR NOT NULL INTERNED, s VARCHAR INTERNED);
                CREATE VIEW V AS SELECT * FROM T;""");
        ccs.step("INSERT INTO T VALUES('x', 'y'), ('z', NULL);", """
                  x| y   | weight
                 -----------------
                  x| y|    1
                  z|NULL | 1""");
    }

    @Test
    public void testCastExpansions() {
        DBSPType i = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, true);
        DBSPType tuple = new DBSPTypeTuple(i, i);
        DBSPType nTuple = new DBSPTypeTuple(CalciteObject.EMPTY, true, Linq.list(i, i));
        DBSPVariablePath var = new DBSPVariablePath("t", tuple);
        DBSPVariablePath nVar = new DBSPVariablePath("n", nTuple);
        DBSPExpression expr0 = ExpressionCompiler.expandTupleCast(CalciteObject.EMPTY, var, tuple);
        Assert.assertEquals("Tup2::new((t.0), (t.1), )", expr0.toString());
        DBSPExpression expr1 = ExpressionCompiler.expandTupleCast(CalciteObject.EMPTY, nVar, tuple);
        Assert.assertEquals("Tup2::new((n.unwrap().0), (n.unwrap().1), )", expr1.toString());
        DBSPExpression expr2 = ExpressionCompiler.expandTupleCast(CalciteObject.EMPTY, var, nTuple);
        Assert.assertEquals("Some(Tup2::new((t.0), (t.1), ))", expr2.toString());
        DBSPExpression expr3 = ExpressionCompiler.expandTupleCast(CalciteObject.EMPTY, nVar, nTuple);
        Assert.assertEquals("""
                if n.is_none() {
                    None
                } else {
                    Some(Tup2::new((n.unwrap().0), (n.unwrap().1), ))
                }""", expr3.toString());
    }
}
