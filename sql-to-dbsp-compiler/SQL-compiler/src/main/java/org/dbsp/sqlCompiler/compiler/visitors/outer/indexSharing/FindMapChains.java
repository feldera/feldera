package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/**
 * Finds chains of operators in the graph that end in an operator with an integral
 * and are formed only of Map and MapIndex.
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
    public void postorder(DBSPJoinBaseOperator join) {
        var left = this.findMapChain(join.left());
        if (!left.isEmpty())
            // This can happen when compiling without -i and the input is
            // an integrator, not a MapIndex
            this.chains.add(new MapChain(left));
        var right = this.findMapChain(join.right());
        if (!right.isEmpty())
            this.chains.add(new MapChain(right));
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
