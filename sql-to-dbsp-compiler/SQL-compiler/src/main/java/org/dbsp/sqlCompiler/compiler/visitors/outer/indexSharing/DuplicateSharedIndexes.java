package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * For each join or star join make sure that the MapIndex preceding it (if it exists) is not shared
 */
class DuplicateSharedIndexes extends CircuitCloneWithGraphsVisitor {
    public DuplicateSharedIndexes(DBSPCompiler compiler, CircuitGraphs graph) {
        super(compiler, graph);
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

    /** Give the join its own copy of every shared MapIndex it reads;
     * false if it reads none. */
    boolean processJoin(DBSPSimpleOperator join) {
        List<OutputPort> sources = new ArrayList<>(join.inputs.size());
        boolean modified = false;
        for (OutputPort input : join.inputs) {
            DBSPMapIndexOperator copy = this.unshareIfNeeded(input);
            if (copy != null) {
                sources.add(copy.outputPort());
                modified = true;
            } else {
                sources.add(this.mapped(input));
            }
        }
        if (!modified) return false;
        DBSPSimpleOperator result = join.withInputs(sources, true)
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

    @Override
    public void postorder(DBSPStarJoinOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPStarJoinIndexOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPStarJoinFilterMapOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }
}
