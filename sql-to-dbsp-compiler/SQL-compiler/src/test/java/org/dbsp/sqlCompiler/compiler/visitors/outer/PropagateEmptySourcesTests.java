package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link PropagateEmptySources}. */
public class PropagateEmptySourcesTests extends BaseSQLTests {
    /** The operator that feeds view 'v' at the end of compilation. */
    DBSPOperator viewSource(String sql) {
        DBSPCircuit circuit = this.getCC(sql).getCircuit();
        for (DBSPOperator operator: circuit.allOperators) {
            DBSPSinkOperator sink = operator.as(DBSPSinkOperator.class);
            if (sink != null && sink.viewName.name().equalsIgnoreCase("v"))
                return sink.input().node();
        }
        throw new RuntimeException("Circuit has no view 'v'");
    }

    static String empty(String program) {
        return String.format(program, "WHERE FALSE");
    }

    static String full(String program) {
        return String.format(program, "");
    }

    /** Checks that view 'v' of 'program' is a constant when its input is empty, and is not
     * when its input is a full table. */
    void assertPropagates(String program) {
        Assert.assertTrue("View 'v' over an empty input did not collapse to a constant",
                this.viewSource(empty(program)).is(DBSPConstantOperator.class));
        Assert.assertFalse("View 'v' is a constant even over a full input, so the program " +
                        "does not test the propagation",
                this.viewSource(full(program)).is(DBSPConstantOperator.class));
    }

    @Test
    public void testFlatMap() {
        this.assertPropagates("""
                CREATE TABLE T(id INT, arr INT ARRAY);
                CREATE LOCAL VIEW E AS SELECT * FROM T %s;
                CREATE VIEW V AS SELECT E.id, x FROM E, UNNEST(E.arr) AS x;""");
    }

    @Test
    public void testLag() {
        this.assertPropagates("""
                CREATE TABLE T(id INT, v INT);
                CREATE LOCAL VIEW E AS SELECT * FROM T %s;
                CREATE VIEW V AS SELECT id, LAG(v) OVER (PARTITION BY id ORDER BY v) AS l FROM E;""");
    }

    @Test
    public void testIndexedTopK() {
        this.assertPropagates("""
                CREATE TABLE T(id INT, v INT);
                CREATE LOCAL VIEW E AS SELECT * FROM T %s;
                CREATE VIEW V AS SELECT * FROM
                (SELECT id, v, RANK() OVER (PARTITION BY id ORDER BY v) AS r FROM E) WHERE r <= 2;""");
    }

    @Test
    public void testAsofJoin() {
        this.assertPropagates("""
                CREATE TABLE T1(id INT, c2 VARCHAR);
                CREATE TABLE T2(id INT, c2 VARCHAR);
                CREATE LOCAL VIEW E AS SELECT * FROM T1 %s;
                CREATE VIEW V AS SELECT i.id, v.c2 AS v_c2 FROM E i
                LEFT ASOF JOIN T2 v MATCH_CONDITION ( i.c2 >= v.c2 ) ON i.id = v.id;""");
    }

    /** A global aggregate returns a row for an empty input, so its zero must survive. */
    @Test
    public void testGlobalAggregateZero() {
        Assert.assertFalse("COUNT(*) over an empty input lost its zero",
                this.viewSource(empty("""
                        CREATE TABLE T(v INT);
                        CREATE LOCAL VIEW E AS SELECT * FROM T %s;
                        CREATE VIEW V AS SELECT COUNT(*) FROM E;""")).is(DBSPConstantOperator.class));
    }
}
