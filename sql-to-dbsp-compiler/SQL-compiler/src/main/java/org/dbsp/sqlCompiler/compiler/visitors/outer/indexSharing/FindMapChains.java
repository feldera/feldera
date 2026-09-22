package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/**
 * Finds chains of operators in the graph that end in an input whose index may be shared,
 * and are formed only of Map and MapIndex
 */
class FindMapChains extends CircuitVisitor {
    final List<MapChain> chains;

    protected FindMapChains(DBSPCompiler compiler) {
        super(compiler);
        this.chains = new ArrayList<>();
    }

    List<DBSPUnaryOperator> findMapChain(OutputPort input) {
        List<DBSPUnaryOperator> list = new ArrayList<>();
        while (input.node().is(DBSPMapIndexOperator.class) ||
                input.node().is(DBSPMapOperator.class)) {
            list.add(input.node().to(DBSPUnaryOperator.class));
            input = input.simpleNode().inputs.get(0);
        }
        return list;
    }

    @Override
    public void postorder(DBSPOperator operator) {
        for (int inputIndex = 0; inputIndex < operator.inputs.size(); inputIndex++) {
            if (!FindSharedIndexes.canShareInputIntegral(operator, inputIndex))
                continue;
            List<DBSPUnaryOperator> chain = this.findMapChain(operator.inputs.get(inputIndex));
            if (!chain.isEmpty())
                // The chain is empty when compiling without -i and the input is an integrator
                this.chains.add(new MapChain(chain));
        }
    }

    record MapChain(List<DBSPUnaryOperator> operators) {
        DBSPUnaryOperator head() {
            return Utilities.last(this.operators);
        }

        public DBSPUnaryOperator tail() {
            return this.operators.get(0);
        }
    }
}
