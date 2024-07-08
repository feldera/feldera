package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMap;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;

/** Combine a Join followed by a filter into a JoinFilterMap. */
public class FilterJoin extends CircuitCloneVisitor {
    public FilterJoin(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (source.is(DBSPJoinOperator.class)) {
            DBSPOperator result =
                    new DBSPJoinFilterMap(source.getNode(), source.getOutputZSetType(),
                            source.getFunction(), operator.getFunction(), null,
                            source.isMultiset, source.inputs.get(0), source.inputs.get(1));
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }
}
