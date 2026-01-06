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
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.GCOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Linq;
import org.dbsp.util.graph.Port;

import java.util.List;

/** Choose for each join whether it is dynamically balanced or just a hash join */
public class BalancedJoins extends CircuitCloneWithGraphsVisitor {
    protected BalancedJoins(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
    }

    private boolean hasGcSuccessor(DBSPOperator operator) {
        for (Port<DBSPOperator> succ: this.getGraph().getSuccessors(operator)) {
            if (succ.node().is(GCOperator.class))
                // only input 0 of these operators affects the GC
                return succ.port() == 0;
        }
        return false;
    }

    private boolean canBalance(DBSPJoinBaseOperator join) {
        // Limitations from DBSP:
        ICircuit parent = this.getParent();
        // Inside recursive component cannot be adaptive
        if (parent.is(DBSPNestedOperator.class))
            return false;
        // Operators with GC cannot be adaptive
        List<Port<DBSPOperator>> leftSuccs = this.getGraph().getSuccessors(join.left().node());
        if (Linq.any(leftSuccs, s -> this.hasGcSuccessor(s.node())))
            return false;
        List<Port<DBSPOperator>> rightSuccs = this.getGraph().getSuccessors(join.right().node());
        return !Linq.any(rightSuccs, s -> this.hasGcSuccessor(s.node()));
    }

    @Override
    public void postorder(DBSPLeftJoinOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPJoinBaseOperator result = new DBSPLeftJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.isMultiset, left, right, true);
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
        DBSPJoinBaseOperator result = new DBSPJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.isMultiset, left, right, true);
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
        DBSPJoinBaseOperator result = new DBSPJoinIndexOperator(
                operator.getRelNode(), operator.getOutputIndexedZSetType(),
                operator.getFunction(), operator.isMultiset, left, right, true);
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
        DBSPJoinBaseOperator result = new DBSPLeftJoinIndexOperator(
                operator.getRelNode(), operator.getOutputIndexedZSetType(),
                operator.getFunction(), operator.isMultiset, left, right, true);
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
        DBSPJoinBaseOperator result = new DBSPJoinFilterMapOperator(
                operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.filter, operator.map,
                operator.isMultiset, left, right, true);
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
        DBSPJoinBaseOperator result = new DBSPLeftJoinFilterMapOperator(
                operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.filter, operator.map,
                operator.isMultiset, left, right, true);
        this.map(operator, result);
    }
}
