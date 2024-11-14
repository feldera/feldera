package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;

/** Replace Sum followed by Sum by a single Sum. */
public class MergeSums extends CircuitCloneVisitor {
    public MergeSums(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        List<OutputPort> newSources = new ArrayList<>();
        for (OutputPort source: sources) {
            if (source.node().is(DBSPSumOperator.class)) {
                newSources.addAll(source.node().inputs);
            } else {
                newSources.add(source);
            }
        }
        DBSPSimpleOperator result = operator.withInputs(newSources, false);
        this.map(operator, result);
    }
}
