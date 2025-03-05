package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;

/** Expands joins that do not have DBSP implementations, such as
 * {@link DBSPStreamAntiJoinOperator} and {@link DBSPStreamJoinIndexOperator}.
 * These are implemented using differentiators and integrators around a non-stream operator.
 * These operators should not appear in incremental circuits. */
public class ExpandJoins extends CircuitCloneVisitor {
    public ExpandJoins(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPStreamAntiJoinOperator operator) {
        var inputs = Linq.map(operator.inputs, this::mapped);
        List<OutputPort> diffs = new ArrayList<>(operator.inputs.size());
        for (OutputPort in: inputs) {
            DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(
                    operator.getRelNode().intermediate(), in);
            this.addOperator(diff);
            diffs.add(diff.outputPort());
        }
        DBSPAntiJoinOperator join = new DBSPAntiJoinOperator(
                operator.getRelNode().intermediate(), diffs.get(0), diffs.get(1));
        this.addOperator(join);
        DBSPIntegrateOperator integ = new DBSPIntegrateOperator(operator.getRelNode(), join.outputPort());
        this.map(operator, integ);
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator operator) {
        var inputs = Linq.map(operator.inputs, this::mapped);
        List<OutputPort> diffs = new ArrayList<>(operator.inputs.size());
        for (OutputPort in: inputs) {
            DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(
                    operator.getRelNode().intermediate(), in);
            this.addOperator(diff);
            diffs.add(diff.outputPort());
        }
        DBSPJoinIndexOperator join = new DBSPJoinIndexOperator(
                operator.getRelNode().intermediate(), operator.getOutputIndexedZSetType(), operator.getFunction(),
                operator.isMultiset, diffs.get(0), diffs.get(1));
        this.addOperator(join);
        DBSPIntegrateOperator integ = new DBSPIntegrateOperator(operator.getRelNode(), join.outputPort());
        this.map(operator, integ);
    }

}
