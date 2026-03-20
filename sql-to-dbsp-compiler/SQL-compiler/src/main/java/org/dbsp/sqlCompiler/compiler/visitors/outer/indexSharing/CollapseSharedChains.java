package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ChainVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Given a chain of Map/MapIndex operator, try to collapse it to a single MapIndex operator */
class CollapseSharedChains extends CircuitCloneVisitor {
    final List<FindMapChains.MapChain> chains;
    /** Key is last operation in each chain */
    final Map<DBSPUnaryOperator, FindMapChains.MapChain> tail;

    CollapseSharedChains(DBSPCompiler compiler, List<FindMapChains.MapChain> chains) {
        super(compiler, false);
        this.chains = chains;
        this.tail = new HashMap<>();
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        if (!this.tail.containsKey(operator)) {
            super.postorder(operator);
            return;
        }

        FindMapChains.MapChain chain = this.tail.get(operator);
        if (chain.operators().size() == 1) {
            // Trivial chain, nothing to do.
            super.postorder(operator);
            return;
        }
        OutputPort input = chain.head().input();
        DBSPType inputType = input.outputType();
        Collections.reverse(chain.operators());
        List<DBSPChainOperator.Computation> list = Linq.map(chain.operators(), ChainVisitor::getComputation);
        DBSPChainOperator.ComputationChain cc = new DBSPChainOperator.ComputationChain(inputType, list);
        cc = cc.shrinkMaps(this.compiler);
        if (cc.size() > 1) {
            // If this could not reduce it, the closure will be too complicated to analyze later; give up
            super.postorder(operator);
            return;
        }
        DBSPClosureExpression function = cc.collapse(this.compiler);
        var result = new DBSPMapIndexOperator(
                operator.getRelNode(), function, operator.getOutputIndexedZSetType(),
                operator.isMultiset, this.mapped(input));
        this.map(operator, result);
    }

    @Override
    public Token startVisit(IDBSPOuterNode circuit) {
        // Maps the head of a chain to the list of chains descending from it
        final Map<OutputPort, List<FindMapChains.MapChain>> head = new HashMap<>();
        for (FindMapChains.MapChain map : this.chains) {
            OutputPort chainHead = map.head().input();
            if (head.containsKey(chainHead)) {
                head.get(chainHead).add(map);
            } else {
                List<FindMapChains.MapChain> chains = new ArrayList<>();
                chains.add(map);
                head.put(chainHead, chains);
            }
        }

        // Remove all chains that do not share a head
        head.entrySet().removeIf(e -> e.getValue().size() < 2);
        for (List<FindMapChains.MapChain> l : head.values()) {
            for (var c : l) {
                this.tail.put(c.tail(), c);
            }
        }
        return super.startVisit(circuit);
    }
}
