package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;

/** Packs the aggregate list of each aggregate operator into one {@link DBSPFold} over a tuple
 * accumulator, because dbsp's {@code Fold} takes a single step function.  Only the Rust backend
 * needs the packed form: each entry of a list declares its own zero, step, and post-processing,
 * so the aggregates of a group are independent by construction. */
public class PackAggregateLists extends CircuitCloneVisitor {
    public PackAggregateLists(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPStreamAggregateOperator node) {
        if (node.function != null) {
            // OrderBy implemented as an aggregate
            super.postorder(node);
            return;
        }

        DBSPFold function = node.getAggregateList().asFold(this.compiler());
        DBSPSimpleOperator result = new DBSPStreamAggregateOperator(
                node.getRelNode(), node.getOutputIndexedZSetType(),
                function, null, this.mapped(node.input()));
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPAggregateOperator node) {
        if (node.function != null) {
            // OrderBy implemented as an aggregate
            super.postorder(node);
            return;
        }
        DBSPFold function = node.getAggregateList().asFold(this.compiler());
        DBSPSimpleOperator result = new DBSPAggregateOperator(
                node.getRelNode(), node.getOutputIndexedZSetType(),
                function, null, this.mapped(node.input()));
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator node) {
        if (node.aggregateList == null) {
            super.postorder(node);
            return;
        }
        DBSPFold function = node.getAggregateList().asFold(this.compiler());
        DBSPSimpleOperator result = new DBSPPartitionedRollingAggregateOperator(node.getRelNode(),
                node.partitioningFunction, function, null, node.lower, node.upper,
                node.getOutputIndexedZSetType(), this.mapped(node.input()));
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator node) {
        if (node.aggregateList == null) {
            super.postorder(node);
            return;
        }
        DBSPFold function = node.aggregateList.asFold(this.compiler());
        DBSPSimpleOperator result = new DBSPPartitionedRollingAggregateWithWaterlineOperator(node.getRelNode(),
                node.partitioningFunction, function, null, node.lower, node.upper,
                node.getOutputIndexedZSetType(),
                this.mapped(node.left()), this.mapped(node.right()));
        this.map(node, result);
    }
}
