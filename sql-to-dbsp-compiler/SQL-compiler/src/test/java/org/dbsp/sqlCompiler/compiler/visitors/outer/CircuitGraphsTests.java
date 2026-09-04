package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.Predicate;

/** Tests for the downstream search of {@link CircuitGraphs} */
public class CircuitGraphsTests extends BaseSQLTests {
    static final Predicate<DBSPOperator> IS_SINK = operator -> operator.is(DBSPSinkOperator.class);

    /** The only operator of class {@code clazz} among {@code operators} */
    static <T extends DBSPOperator> T single(Iterable<DBSPOperator> operators, Class<T> clazz) {
        List<DBSPOperator> found = Linq.where(Linq.list(operators), operator -> operator.is(clazz));
        Assert.assertEquals(found.toString(), 1, found.size());
        return found.get(0).to(clazz);
    }

    @Test
    public void searchCrossesRecursiveComponent() {
        var cc = this.getCC("""
                CREATE TABLE E(x INT);
                DECLARE RECURSIVE VIEW R(x INT);
                CREATE VIEW R AS SELECT x FROM E UNION SELECT x + 1 FROM R WHERE x < 10;""");
        DBSPCircuit circuit = cc.getCircuit();
        Graph graph = new Graph(cc.compiler);
        graph.apply(circuit);
        CircuitGraphs graphs = graph.getGraphs();

        DBSPNestedOperator recursive = single(circuit.getAllOperators(), DBSPNestedOperator.class);
        DBSPDistinctOperator distinct = single(recursive.getAllOperators(), DBSPDistinctOperator.class);
        DBSPSinkOperator sink = circuit.getSink(cc.compiler.canonicalName("R", false));
        Assert.assertNotNull(sink);

        // The recursive circuit contains no sink...
        Assert.assertNull(graphs.getGraph(recursive).closestSuccessor(distinct, IS_SINK));
        // ...so the search continues from the recursive circuit in the root circuit
        Assert.assertSame(sink, graphs.closestDownstream(recursive, distinct, IS_SINK));

        // A match inside the recursive circuit ends the search there
        DBSPDeltaOperator delta = single(recursive.getAllOperators(), DBSPDeltaOperator.class);
        Assert.assertSame(distinct, graphs.closestDownstream(
                recursive, delta, operator -> operator.is(DBSPDistinctOperator.class)));

        // From the root circuit the search passes through the recursive circuit
        DBSPSourceTableOperator source = single(circuit.getAllOperators(), DBSPSourceTableOperator.class);
        Assert.assertSame(sink, graphs.closestDownstream(circuit, source, IS_SINK));
    }
}
