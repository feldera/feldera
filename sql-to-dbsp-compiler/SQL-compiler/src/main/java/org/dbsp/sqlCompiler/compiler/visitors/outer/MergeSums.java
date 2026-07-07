package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;

/** Replace Sum followed by Sum by a single Sum.
 * Replace a sum with a single input by its input.
 * Replace a + (neg(a)) with nothing.
 * Replace neg(neg(a)) with a. */
public class MergeSums extends CircuitCloneVisitor {
    public MergeSums(DBSPCompiler compiler) {
        super(compiler, false);
    }

    List<OutputPort> removeComplements(List<OutputPort> ports) {
        List<OutputPort> results = new ArrayList<>();
        // Find non-negated operators
        List<DBSPNegateOperator> negations = new ArrayList<>();
        for (OutputPort port: ports) {
            if (port.isSimpleNode()) {
                DBSPSimpleOperator source = port.simpleNode();
                if (source.is(DBSPNegateOperator.class)) {
                    negations.add(source.to(DBSPNegateOperator.class));
                    continue;
                }
            }
            results.add(port);
        }
        for (DBSPNegateOperator neg: negations) {
            OutputPort negated = neg.input();
            if (results.contains(negated)) {
                results.remove(negated);
            } else {
                results.add(neg.outputPort());
            }
        }
        return results;
    }

    @Override
    public void postorder(DBSPNegateOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (source.node().is(DBSPNegateOperator.class)) {
            DBSPNegateOperator neg = source.node().to(DBSPNegateOperator.class);
            this.map(operator.outputPort(), neg.input(), false);
        } else {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        sources = this.removeComplements(sources);

        if (sources.isEmpty()) {
            final DBSPExpression value;
            if (operator.outputType().is(DBSPTypeZSet.class)) {
                value = new DBSPZSetExpression(operator.getOutputZSetElementType());
            } else {
                value = new DBSPIndexedZSetExpression(CalciteObject.EMPTY, operator.getOutputIndexedZSetType());
            }
            DBSPConstantOperator constant = new DBSPConstantOperator(operator.getRelNode(), value, false);
            this.map(operator, constant);
            return;
        }

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
