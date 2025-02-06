package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.graph.Port;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class OuterJoinTests extends SqlIoTest {
    @Override
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(true, true);
        // This causes the use of SourceSet operators
        // options.ioOptions.emitHandles = false;
        // Without the following ORDER BY causes failures
        options.languageOptions.ignoreOrderBy = true;
        return new DBSPCompiler(options);
    }

    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                CREATE TABLE A(X INT NOT NULL, Y INT NOT NULL);
                CREATE TABLE B(X INT, Y INT NOT NULL);
                CREATE TABLE C(X INT NOT NULL, Y INT);
                CREATE TABLE D(X INT, Y INT);
                
                INSERT INTO A VALUES(0, 0);
                INSERT INTO B VALUES(0, 0), (NULL, 0);
                INSERT INTO C VALUES(0, 0), (0, NULL);
                INSERT INTO D VALUES(0, 0), (0, NULL), (NULL, 0), (NULL, NULL);""");
    }

    static class CheckCommonIndex extends Passes {
        CheckCommonIndex(DBSPCompiler compiler, boolean isCommon) {
            super("CCI", compiler);
            Graph graph = new Graph(compiler);
            this.add(graph);
            CircuitWithGraphsVisitor v = new CircuitWithGraphsVisitor(compiler, graph.getGraphs()) {
                @Override
                public VisitDecision preorder(DBSPJoinOperator operator) {
                    DBSPOperator joinSource = operator.right().simpleNode();
                    List<Port<DBSPOperator>> succ = this.getGraph().getSuccessors(joinSource);
                    Assert.assertEquals(2, succ.size());
                    for (var s: succ) {
                        if (s.node() == operator) continue;
                        boolean isAnti = s.node().is(DBSPAntiJoinOperator.class);
                        if (isCommon)
                            Assert.assertTrue(isAnti);
                        else
                            Assert.assertFalse(isAnti);
                    }
                    return VisitDecision.STOP;
                }
            };
            this.add(v);
        }
    }

    @Test
    public void testCommonIndex() {
        CompilerCircuit cc;
        CheckCommonIndex cci;

        String common = """
                CREATE TABLE T(X INT, Y INT NOT NULL);
                CREATE TABLE S(X INT, Y INT NOT NULL);""";

        cc = this.getCC(common +
                "CREATE VIEW YX AS SELECT * FROM T LEFT JOIN S ON T.Y = S.X;");
        cci = new CheckCommonIndex(cc.compiler, true);
        cc.visit(cci);

        cc = this.getCCS(common +
                "CREATE VIEW YY AS SELECT * FROM T LEFT JOIN S ON T.Y = S.Y;");
        cci = new CheckCommonIndex(cc.compiler, true);
        cc.visit(cci);

        cc = this.getCCS(common +
                "CREATE VIEW XX AS SELECT * FROM T LEFT JOIN S ON T.X = S.X;");
        cci = new CheckCommonIndex(cc.compiler, true);
        cc.visit(cci);

        cc = this.getCCS(common +
                "CREATE VIEW XY AS SELECT * FROM T LEFT JOIN S ON T.X = S.Y;");
        cci = new CheckCommonIndex(cc.compiler, true);
        cc.visit(cci);
    }

    @Test
    public void test() {
        // validated on postgres
        this.qs("""
            SELECT * FROM A AS L LEFT JOIN A AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | lx | ly
            -------------------
              0 |  0 |  0 |  0
            (1 row)
            
            SELECT * FROM A AS L LEFT JOIN B AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
            (1 row)
            
            SELECT * FROM A AS L LEFT JOIN C AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
            (1 row)
            
            SELECT * FROM A AS L LEFT JOIN D AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
            (1 row)
            
            SELECT * FROM C AS L LEFT JOIN A AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
            (2 rows)
            
            SELECT * FROM C AS L LEFT JOIN B AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
            (2 rows)
            
            SELECT * FROM C AS L LEFT JOIN C AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
            (2 rows)
            
            SELECT * FROM C AS L LEFT JOIN D AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN A AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
                |  0 |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN B AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
                |  0 |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN C AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
                |  0 |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN D AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
                |  0 |    |
            (2 rows)
            
            SELECT * FROM D AS L LEFT JOIN A AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
                |  0 |    |
                |    |    |
            (4 rows)
            
            SELECT * FROM D AS L LEFT JOIN B AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
                |  0 |    |
                |    |    |
            (4 rows)
            
            SELECT * FROM D AS L LEFT JOIN C AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
                |  0 |    |
                |    |    |
            (4 rows)
            
            SELECT * FROM D AS L LEFT JOIN D AS R ON L.X = R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
                |  0 |    |
                |    |    |
            (4 rows)""");
    }

    @Test
    public void testNonEqui() {
        // validated on postgres
        this.qs("""
            SELECT * FROM A AS L LEFT JOIN A AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | lx | ly
            -------------------
              0 |  0 |    |
            (1 row)
            
            SELECT * FROM A AS L LEFT JOIN B AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
            (1 row)
            
            SELECT * FROM A AS L LEFT JOIN C AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
            (1 row)
            
            SELECT * FROM A AS L LEFT JOIN D AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
            (1 row)
            
            SELECT * FROM C AS L LEFT JOIN A AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
              0 |    |    |
            (2 rows)
            
            SELECT * FROM C AS L LEFT JOIN B AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
              0 |    |    |
            (2 rows)
            
            SELECT * FROM C AS L LEFT JOIN C AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
              0 |    |    |
            (2 rows)
            
            SELECT * FROM C AS L LEFT JOIN D AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
              0 |    |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN A AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
                |  0 |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN B AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
                |  0 |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN C AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
                |  0 |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN D AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
                |  0 |    |
            (2 rows)
            
            SELECT * FROM D AS L LEFT JOIN A AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
              0 |    |    |
                |  0 |    |
                |    |    |
            (4 rows)
            
            SELECT * FROM D AS L LEFT JOIN B AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
              0 |    |    |
                |  0 |    |
                |    |    |
            (4 rows)
            
            SELECT * FROM D AS L LEFT JOIN C AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
              0 |    |    |
                |  0 |    |
                |    |    |
            (4 rows)
            
            SELECT * FROM D AS L LEFT JOIN D AS R ON L.X < R.X and L.Y = R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |    |
              0 |    |    |
                |  0 |    |
                |    |    |
            (4 rows)""");
    }

    @Test
    public void testDistinct() {
        // validated on postgres
        this.qs("""
            SELECT * FROM A AS L LEFT JOIN A AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | lx | ly
            -------------------
              0 |  0 |  0 |  0
            (1 row)
            
            SELECT * FROM A AS L LEFT JOIN B AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
            (1 row)
            
            SELECT * FROM A AS L LEFT JOIN C AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
            (1 row)
            
            SELECT * FROM A AS L LEFT JOIN D AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
            (1 row)
            
            SELECT * FROM C AS L LEFT JOIN A AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
            (2 rows)
            
            SELECT * FROM C AS L LEFT JOIN B AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
            (2 rows)
            
            SELECT * FROM C AS L LEFT JOIN C AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |  0 |
            (2 rows)
            
            SELECT * FROM C AS L LEFT JOIN D AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |  0 |
            (2 rows)

            SELECT * FROM B AS L LEFT JOIN A AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
                |  0 |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN B AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
                |  0 |    |  0
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN C AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
                |  0 |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN D AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
                |  0 |    |  0
            (2 rows)

            SELECT * FROM D AS L LEFT JOIN A AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
                |  0 |    |
                |    |    |
            (4 rows)
            
            SELECT * FROM D AS L LEFT JOIN B AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
                |  0 |    |  0
                |    |    |
            (4 rows)
            
            SELECT * FROM D AS L LEFT JOIN C AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |  0 |
                |  0 |    |
                |    |    |
            (4 rows)
            
            SELECT * FROM D AS L LEFT JOIN D AS R ON L.X IS NOT DISTINCT FROM R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |  0 |
                |  0 |    |  0
                |    |    |
            (4 rows)""");
    }

    @Test
    public void testMix() {
        // validated on postgres
        this.qs("""
            SELECT * FROM A AS L LEFT JOIN A AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | lx | ly
            -------------------
              0 |  0 |  0 |  0
            (1 row)
            
            SELECT * FROM A AS L LEFT JOIN B AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
            (1 row)
            
            SELECT * FROM A AS L LEFT JOIN C AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
            (1 row)
            
            SELECT * FROM A AS L LEFT JOIN D AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
            (1 row)
            
            SELECT * FROM C AS L LEFT JOIN A AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
            (2 rows)
            
            SELECT * FROM C AS L LEFT JOIN B AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
            (2 rows)
            
            SELECT * FROM C AS L LEFT JOIN C AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |  0 |
            (2 rows)
            
            SELECT * FROM C AS L LEFT JOIN D AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |  0 |
            (2 rows)

            SELECT * FROM B AS L LEFT JOIN A AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
                |  0 |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN B AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
                |  0 |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN C AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
                |  0 |    |
            (2 rows)
            
            SELECT * FROM B AS L LEFT JOIN D AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
                |  0 |    |
            (2 rows)

            SELECT * FROM D AS L LEFT JOIN A AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
                |  0 |    |
                |    |    |
            (4 rows)
            
            SELECT * FROM D AS L LEFT JOIN B AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |    |
                |  0 |    |
                |    |    |
            (4 rows)
            
            SELECT * FROM D AS L LEFT JOIN C AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |  0 |
                |  0 |    |
                |    |    |
            (4 rows)
            
            SELECT * FROM D AS L LEFT JOIN D AS R ON L.X = R.X and L.Y IS NOT DISTINCT FROM R.Y;
             lx | ly | rx | ry
            -------------------
              0 |  0 |  0 |  0
              0 |    |  0 |
                |  0 |    |
                |    |    |
            (4 rows)""");
    }

    @Test
    public void issue3448() {
        var cc = this.getCC("""
                CREATE TABLE T1(a INT, b INT, c INT, d INT, e INT);
                CREATE TABLE T2(l INT, m INT, n INT, o INT, p INT);
                CREATE VIEW V AS
                select a, l from t1 left join t2 on t1.a = t2.l and t1.b < t2.m;""");
        InnerVisitor visitor = new InnerVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPTypeTuple type) {
                Assert.assertTrue(type.size() < 7);
            }
        };
        cc.visit(visitor.getCircuitVisitor(false));
    }
}
