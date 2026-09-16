package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.JoinStrategy;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.util.Linq;
import org.dbsp.util.graph.Port;

import javax.annotation.Nullable;
import java.util.List;

/** Mark each join that DBSP can balance as {@code balanced}; the others stay plain hash joins.
 *
 * <p>DBSP cannot currently balance a join inside a recursive component,
 * or a join whose input feeds a garbage-collected operator.  Whether a balanced join runs
 * adaptively is the program-wide setting {@link ProgramMetadata#ADAPTIVE_JOINS}, which the
 * circuit carries to the runtime.  A strategy hint on a join that will not run adaptively,
 * for either reason, produces a warning. */
public class BalancedJoins extends CircuitCloneWithGraphsVisitor {
    /** Value of the {@link ProgramMetadata#ADAPTIVE_JOINS} setting. */
    final boolean adaptiveJoins;

    protected BalancedJoins(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs);
        this.adaptiveJoins = compiler.metadata.adaptiveJoins();
    }

    private boolean hasGcSuccessor(DBSPOperator operator) {
        for (Port<DBSPOperator> succ: this.getGraph().getSuccessors(operator)) {
            if (succ.node().is(IGCOperator.class))
                // only input 0 of these operators affects the GC
                return succ.port() == 0;
        }
        return false;
    }

    /** The reason DBSP cannot balance {@code join}, or null if it can. */
    @Nullable
    private String balancingObstacle(DBSPJoinBaseOperator join) {
        ICircuit parent = this.getParent();
        if (parent.is(DBSPNestedOperator.class))
            return "the join is inside a recursive view";
        List<Port<DBSPOperator>> leftSuccs = this.getGraph().getSuccessors(join.left().node());
        if (Linq.any(leftSuccs, s -> this.hasGcSuccessor(s.node())))
            return "the left input of the join is garbage-collected";
        List<Port<DBSPOperator>> rightSuccs = this.getGraph().getSuccessors(join.right().node());
        if (Linq.any(rightSuccs, s -> this.hasGcSuccessor(s.node())))
            return "the right input of the join is garbage-collected";
        return null;
    }

    private void warnHintsIgnored(List<JoinStrategy> hints, String reason) {
        for (JoinStrategy hint: hints)
            this.compiler.reportWarning(hint.getPosition(), "Hint ignored",
                    "Hint " + hint + " cannot be implemented: " + reason);
    }

    private boolean canBalance(DBSPJoinBaseOperator join) {
        List<JoinStrategy> hints = join.annotations.get(JoinStrategy.class);
        String obstacle = this.balancingObstacle(join);
        if (obstacle != null) {
            this.warnHintsIgnored(hints, obstacle);
            return false;
        }
        if (!this.adaptiveJoins)
            this.warnHintsIgnored(hints, "adaptive joins are off; enable them with SET "
                    + ProgramMetadata.ADAPTIVE_JOINS + " = ON");
        return true;
    }

    @Override
    public void postorder(DBSPLeftJoinOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = new DBSPLeftJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.isMultiset, left, right, true)
                .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = new DBSPJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.isMultiset, left, right, true)
                .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPJoinIndexOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = new DBSPJoinIndexOperator(
                operator.getRelNode(), operator.getOutputIndexedZSetType(),
                operator.getFunction(), operator.isMultiset, left, right, true)
                .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPLeftJoinIndexOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = new DBSPLeftJoinIndexOperator(
                operator.getRelNode(), operator.getOutputIndexedZSetType(),
                operator.getFunction(), operator.isMultiset, left, right, true)
                .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = new DBSPJoinFilterMapOperator(
                operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.filter, operator.map,
                operator.isMultiset, left, right, true)
                .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPLeftJoinFilterMapOperator operator) {
        if (!this.canBalance(operator)) {
            super.postorder(operator);
            return;
        }

        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = new DBSPLeftJoinFilterMapOperator(
                operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.filter, operator.map,
                operator.isMultiset, left, right, true)
                .copyAnnotations(operator);
        this.map(operator, result);
    }
}
