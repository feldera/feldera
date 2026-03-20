package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;

/**
 * For each join make sure that the MapIndex preceding it (if it exists) is not shared
 */
class DuplicateSharedIndexes extends CircuitCloneWithGraphsVisitor {
    public DuplicateSharedIndexes(DBSPCompiler compiler, CircuitGraphs graph) {
        super(compiler, graph, false);
    }

    /**
     * If a MapIndex is shared, make a copy
     */
    @Nullable
    DBSPMapIndexOperator unshareIfNeeded(OutputPort input) {
        DBSPOperator node = input.node();
        if (!node.is(DBSPMapIndexOperator.class))
            return null;
        if (this.getGraph().getFanout(node) == 1)
            return null;
        DBSPMapIndexOperator index = node.to(DBSPMapIndexOperator.class);
        var copy = new DBSPMapIndexOperator(
                index.getRelNode(), index.getClosureFunction(), index.getOutputIndexedZSetType(),
                index.isMultiset, this.mapped(index.input()));
        this.addOperator(copy);
        return copy;
    }

    boolean processJoin(DBSPJoinBaseOperator join) {
        OutputPort left;
        OutputPort right;
        var lmi = this.unshareIfNeeded(join.left());
        boolean modified = false;
        if (lmi != null) {
            left = lmi.outputPort();
            modified = true;
        } else {
            left = this.mapped(join.left());
        }
        var rmi = this.unshareIfNeeded(join.right());
        if (rmi != null) {
            right = rmi.outputPort();
            modified = true;
        } else {
            right = this.mapped(join.right());
        }
        if (!modified) return false;
        DBSPSimpleOperator result = join.withInputs(Linq.list(left, right), true)
                .to(DBSPSimpleOperator.class);
        this.map(join, result);
        return true;
    }

    @Override
    public void postorder(DBSPLeftJoinOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPLeftJoinIndexOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPJoinIndexOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPLeftJoinFilterMapOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

}
