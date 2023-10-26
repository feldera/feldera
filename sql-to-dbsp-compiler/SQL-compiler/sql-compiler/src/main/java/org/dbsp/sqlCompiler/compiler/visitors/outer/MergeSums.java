package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;

/**
 * Replace Sum followed by Sum by a single Sum.
 */
public class MergeSums extends CircuitCloneVisitor {
    public MergeSums(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        List<DBSPOperator> newSources = new ArrayList<>();
        for (DBSPOperator source: sources) {
            if (source.is(DBSPSumOperator.class)) {
                newSources.addAll(source.inputs);
            } else {
                newSources.add(source);
            }
        }
        DBSPOperator result = operator.withInputs(newSources, false);
        this.map(operator, result);
    }
}
