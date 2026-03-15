package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

/** Implement {@link org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator} */
public class ImplementChains extends CircuitCloneVisitor {
    public ImplementChains(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPChainOperator node) {
        DBSPChainOperator.ComputationChain chain = node.chain.shrinkMaps(this.compiler);
        while (true) {
            var newChain = chain.shrinkMapFilterMap(this.compiler);
            if (newChain == chain)
                break;
            chain = newChain;
        }
        DBSPClosureExpression function = chain.collapse(this.compiler);
        boolean containsFilter = chain.containsFilter();
        DBSPSimpleOperator result;
        if (node.outputType.is(DBSPTypeZSet.class)) {
            if (containsFilter)
                result = new DBSPFlatMapOperator(
                        node.getRelNode(), function, node.getOutputZSetType(),
                        node.isMultiset, this.mapped(node.input()));
            else
                result = new DBSPMapOperator(
                        node.getRelNode(), function, node.getOutputZSetType(),
                        node.isMultiset, this.mapped(node.input()));
        } else {
            if (containsFilter)
                result = new DBSPFlatMapIndexOperator(
                        node.getRelNode(), function, node.getOutputIndexedZSetType(),
                        node.isMultiset, this.mapped(node.input()));
            else
                result = new DBSPMapIndexOperator(
                        node.getRelNode(), function, node.getOutputIndexedZSetType(),
                        node.isMultiset, this.mapped(node.input()));
        }
        this.map(node, result);
    }
}
