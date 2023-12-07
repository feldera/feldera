package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.Linq;

import java.util.List;

/**
 * Ensure that all Sink nodes produce sets.  Inserts distinct nodes
 * if necessary.
 */
public class EnsureDistinctOutputs extends CircuitCloneVisitor {
    public EnsureDistinctOutputs(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPOperator input = sources.get(0);
        if (input.isMultiset) {
            DBSPStreamDistinctOperator distinct = new DBSPStreamDistinctOperator(operator.getNode(), input);
            this.addOperator(distinct);
            DBSPOperator result = operator.withInputs(Linq.list(distinct), true);
            this.map(operator, result);
        } else {
            super.postorder(operator);
        }
    }
}
