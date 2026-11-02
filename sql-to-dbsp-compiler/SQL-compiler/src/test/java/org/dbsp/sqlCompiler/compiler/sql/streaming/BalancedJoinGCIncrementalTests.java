package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** A join with a garbage-collected input is a hash join: a balanced join keeps its own
 * integral, which the GC operators of its inputs do not reach. */
public class BalancedJoinGCIncrementalTests extends StreamingTestBase {
    /** The {@code balanced} flag of each join of {@code sql}. */
    List<Boolean> balancedJoins(String sql) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        List<Boolean> result = new ArrayList<>();
        getCircuit(compiler).accept(new CircuitVisitor(compiler) {
            @Override
            public void postorder(DBSPJoinBaseOperator join) {
                result.add(join.balanced);
            }
        });
        return result;
    }

    @Test
    public void garbageCollectedJoinIsNotBalanced() {
        String program = """
                CREATE TABLE t(ts TIMESTAMP NOT NULL %s, k INT);
                CREATE TABLE s(ts TIMESTAMP NOT NULL %s, k INT);
                CREATE VIEW v AS SELECT t.k, s.ts FROM t JOIN s ON t.ts = s.ts;""";
        String lateness = "LATENESS INTERVAL 1 HOUR";
        Assert.assertEquals(List.of(false), this.balancedJoins(program.formatted(lateness, lateness)));
        Assert.assertEquals(List.of(true), this.balancedJoins(program.formatted("", "")));
    }
}
