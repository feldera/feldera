package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.circuit.IDBSPNode;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.InnerVisitor;

import javax.annotation.Nullable;

/**
 * Invokes an InnerVisitor on each InnerNode reachable from this circuit.
 */
public class CircuitDelegateVisitor extends CircuitVisitor {
    private final InnerVisitor innerVisitor;

    public CircuitDelegateVisitor(IErrorReporter reporter, InnerVisitor visitor) {
        super(reporter, true);
        this.innerVisitor = visitor;
    }

    void doFunction(DBSPOperator node) {
        if (node.function != null)
            node.function.accept(this.innerVisitor);
    }

    void doOutputType(DBSPOperator node) {
        node.outputType.accept(this.innerVisitor);
    }

    void doAggregate(@Nullable DBSPAggregate aggregate) {
        if (aggregate != null)
            aggregate.accept(this.innerVisitor);
    }

    @Override
    public void postorder(DBSPOperator node) {
        this.doFunction(node);
        this.doOutputType(node);
    }

    @Override
    public void postorder(DBSPAggregateOperatorBase node) {
        this.doAggregate(node.aggregate);
        this.doFunction(node);
        this.doOutputType(node);
    }

    @Override
    public boolean preorder(DBSPPartialCircuit circuit) {
        for (IDBSPNode node : circuit.getAllOperators()) {
            DBSPOperator op = node.as(DBSPOperator.class);
            if (op != null)
                op.accept(this);
            else {
                IDBSPInnerNode inode = node.to(IDBSPInnerNode.class);
                inode.accept(this.innerVisitor);
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return super.toString() + ":" + this.innerVisitor;
    }
}
