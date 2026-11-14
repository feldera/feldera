package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.Linq;

import java.util.HashMap;
import java.util.Map;

/** Choose for each join whether it is dynamically balanced or just a hash join */
public class BalancedJoins extends CircuitCloneWithGraphsVisitor {
    /** Maps an output port to true if a GC operator garbage-collects the integral of the port */
    private final Map<OutputPort, Boolean> garbageCollected;

    protected BalancedJoins(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs);
        this.garbageCollected = new HashMap<>();
    }

    @Override
    public Token startVisit(IDBSPOuterNode circuit) {
        this.garbageCollected.clear();
        return super.startVisit(circuit);
    }

    /** True if a GC operator garbage-collects the integral of {@code port}.
     * Cached, because a port can feed thousands of joins. */
    private boolean isGarbageCollected(OutputPort port) {
        return this.garbageCollected.computeIfAbsent(port, p ->
                Linq.any(this.getGraph().getSuccessors(p.node()), succ -> succ.node().is(IGCOperator.class) &&
                        succ.node().to(IGCOperator.class).garbageCollects(p)));
    }

    private boolean canBalance(DBSPJoinBaseOperator join) {
        // Limitations from DBSP:
        ICircuit parent = this.getParent();
        // Inside recursive component cannot be adaptive
        if (parent.is(DBSPNestedOperator.class))
            return false;
        // Operators with GC cannot be adaptive: a balanced join keeps its own integral,
        // which the GC operators of its inputs do not reach
        return !this.isGarbageCollected(join.left()) && !this.isGarbageCollected(join.right());
    }

    @Override
    public void postorder(DBSPLeftJoinOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = new DBSPLeftJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), left, right, true)
                .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = new DBSPJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), left, right, true)
                .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPJoinIndexOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = new DBSPJoinIndexOperator(
                operator.getRelNode(), operator.getOutputIndexedZSetType(),
                operator.getFunction(), left, right, true)
                .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPLeftJoinIndexOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = new DBSPLeftJoinIndexOperator(
                operator.getRelNode(), operator.getOutputIndexedZSetType(),
                operator.getFunction(), left, right, true)
                .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = new DBSPJoinFilterMapOperator(
                operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.filter, operator.map,
                left, right, true)
                .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPLeftJoinFilterMapOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = new DBSPLeftJoinFilterMapOperator(
                operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.filter, operator.map,
                left, right, true)
                .copyAnnotations(operator);
        this.map(operator, result);
    }
}
