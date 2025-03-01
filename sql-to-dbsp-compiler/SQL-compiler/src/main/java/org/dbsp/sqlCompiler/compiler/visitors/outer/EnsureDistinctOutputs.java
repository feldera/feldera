package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Linq;

import java.util.List;

/** Ensure that all Sink nodes produce sets.  Inserts distinct nodes
 * if necessary. */
public class EnsureDistinctOutputs extends CircuitCloneVisitor {
    public EnsureDistinctOutputs(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        OutputPort input = sources.get(0);
        if (input.isMultiset()) {
            DBSPStreamDistinctOperator distinct = new DBSPStreamDistinctOperator(
                    operator.getRelNode().intermediate(), input);
            this.addOperator(distinct);
            DBSPSimpleOperator result = operator.withInputs(Linq.list(distinct.outputPort()), true);
            this.map(operator, result);
        } else {
            super.postorder(operator);
        }
    }
}
