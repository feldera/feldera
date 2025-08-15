package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;

/** Replace Sum followed by Sum by a single Sum.
 * Replace a sum with a single input by its input. */
public class MergeSums extends CircuitCloneVisitor {
    public MergeSums(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        if (sources.size() == 1) {
            this.map(operator.outputPort(), sources.get(0), false);
            return;
        }

        List<OutputPort> newSources = new ArrayList<>();
        for (OutputPort source: sources) {
            if (source.node().is(DBSPSumOperator.class)) {
                newSources.addAll(source.node().inputs);
            } else {
                newSources.add(source);
            }
        }
        DBSPSimpleOperator result = operator.withInputs(newSources, false)
                .to(DBSPSimpleOperator.class);
        this.map(operator, result);
    }
}
