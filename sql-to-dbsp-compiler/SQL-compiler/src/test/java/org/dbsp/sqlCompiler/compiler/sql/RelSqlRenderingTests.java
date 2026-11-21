package org.dbsp.sqlCompiler.compiler.sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.RelReference;
import org.dbsp.util.IndentStream;
import org.junit.Assert;
import org.junit.Test;

/** SQL provenance must not depend on previous renderings in the same process. */
public class RelSqlRenderingTests {
    private static RelNode joinPlan() {
        var options = new CompilerOptions();
        options.ioOptions.quiet = true;
        var compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t(k INT, v INT);
                CREATE VIEW v AS SELECT a.k, a.v, b.v AS other
                FROM t AS a JOIN t AS b ON a.k = b.k;
                """);
        compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        var plans = compiler.getPlans(new IndentStream(new StringBuilder()));
        return plans.keySet().stream().filter(node -> node instanceof Join)
                .findFirst().orElseThrow();
    }

    @Test public void relationalSqlIsIndependentOfRenderingHistory() {
        RelNode first = joinPlan();
        RelNode unrelated = joinPlan();
        String expected = CalciteRelNode.toSqlString(first);
        Assert.assertTrue(expected, expected.contains("JOIN"));
        for (int repeat = 0; repeat < 20; repeat++) {
            CalciteRelNode.toSqlString(unrelated);
            new RelReference.Live(unrelated).sql();
            Assert.assertEquals(expected, CalciteRelNode.toSqlString(first));
        }
    }

    @Test public void replayProvenanceIsIndependentOfRenderingHistory() {
        RelNode first = joinPlan();
        RelNode unrelated = joinPlan();
        var reference = new RelReference.Live(first);
        String expected = reference.sql();
        Assert.assertTrue(expected, expected.contains("JOIN"));
        for (int repeat = 0; repeat < 20; repeat++) {
            CalciteRelNode.toSqlString(unrelated);
            new RelReference.Live(unrelated).sql();
            Assert.assertEquals(expected, reference.sql());
        }
    }
}
