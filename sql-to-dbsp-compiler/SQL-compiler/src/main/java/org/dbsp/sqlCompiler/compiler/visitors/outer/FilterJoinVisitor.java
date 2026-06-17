package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.util.Logger;
import org.dbsp.util.Maybe;
import org.dbsp.util.Utilities;

import java.util.List;

/**
 * Optimize a join followed by a filter:
 * <ul>
 * <li>For simple joins attempt to pull the filter on one or both sides of the join
 * <li>Combine a {@link DBSPJoinOperator}, {@link DBSPLeftJoinOperator} or {@link DBSPStarJoinOperator}
 *  followed by a {@link DBSPFilterOperator} into a JoinFilterMap version of the same operator.
 * <uil> */
public class FilterJoinVisitor extends CircuitCloneWithGraphsVisitor {
    public FilterJoinVisitor(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs);
    }

    /** Search if an expression contains NoExpression as a sub-expression */
    static class ContainsNoExpression extends InnerVisitor {
        boolean found = false;

        public ContainsNoExpression(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public VisitDecision preorder(NoExpression expression) {
            this.found = true;
            return VisitDecision.STOP;
        }

        @Override
        public VisitDecision preorder(DBSPType type) {
            return VisitDecision.STOP;
        }

        static boolean search(DBSPCompiler compiler, DBSPExpression expression) {
            ContainsNoExpression cno = new ContainsNoExpression(compiler);
            cno.apply(expression);
            return cno.found;
        }
    }

    /**
     * Check if filter can be pushed to one input.  We use the following
     * algorithm: we compute the function joinFunction \circ filterFunction
     * and check whether it does not depend on one of the join inputs.
     * For that purpose we apply it to a NoExpression on that input
     * and check if the NoExpression appears in the resulting program.
     * @param filter  Filter operator immediately after a join
     * @param join  Join operator
     * @param isLeft  True if we only try to pull on the left input (for left joins).
     * @return        True if the predicate was pulled successfully.
     */
    boolean checkPredicatePull(DBSPFilterOperator filter, DBSPJoinBaseOperator join, boolean isLeft) {
        DBSPClosureExpression joinFunction = join.getClosureFunction();
        DBSPClosureExpression filterFunction = filter.getClosureFunction();
        Utilities.enforce(joinFunction.parameters.length == 3);
        OutputPort leftInput = null, rightInput = null;

        final DBSPVariablePath left = new DBSPTypeRawTuple(
                joinFunction.parameters[0].getType(),
                joinFunction.parameters[1].getType()).var();
        final DBSPClosureExpression leftProjection = joinFunction.call(
                        left.field(0),
                        left.field(1),
                        new NoExpression(joinFunction.parameters[2].getType()))
                .closure(left);
        final DBSPClosureExpression leftFilterPredicate = filterFunction.applyAfter(compiler, leftProjection, Maybe.YES);
        if (!ContainsNoExpression.search(this.compiler, leftFilterPredicate)) {
            // Can pull predicate on the left side, since it does not
            // depend on any field on the RHS
            DBSPFilterOperator leftFilter = new DBSPFilterOperator(
                    filter.getRelNode(),
                    leftFilterPredicate,
                    join.left());
            this.addOperator(leftFilter);
            leftInput = leftFilter.outputPort();
            rightInput = join.right();
        }

        if (!isLeft) {
            final DBSPVariablePath right = new DBSPTypeRawTuple(
                    joinFunction.parameters[0].getType(),
                    joinFunction.parameters[2].getType()).var();
            final DBSPClosureExpression rightProjection = joinFunction.call(
                            right.field(0),
                            new NoExpression(joinFunction.parameters[1].getType()),
                            right.field(1))
                    .closure(right);
            final DBSPClosureExpression rightFilterPredicate = filterFunction.applyAfter(compiler, rightProjection, Maybe.YES);
            if (!ContainsNoExpression.search(this.compiler, rightFilterPredicate)) {
                // Can pull predicate on the right side, since it does not
                // depend on any field on the LHS
                DBSPFilterOperator rightFilter = new DBSPFilterOperator(
                        filter.getRelNode(),
                        rightFilterPredicate,
                        join.right());
                this.addOperator(rightFilter);
                rightInput = rightFilter.outputPort();
                if (leftInput == null)
                    leftInput = join.left();
            }
        }

        // It is possible for a predicate to be pulled on both sides, if it only
        // depends on the key
        if (leftInput != null) {
            DBSPSimpleOperator newJoin = join.withInputs(List.of(leftInput, rightInput), false)
                    .to(DBSPSimpleOperator.class);
            // No more filter after the join
            this.map(filter, newJoin);
            Logger.INSTANCE.belowLevel(this, 1)
                    .append("Pulled predicate before join ")
                    .appendSupplier(filterFunction::toString)
                    .newline();
            return true;
        }
        return false;
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        if (source.node().is(DBSPJoinOperator.class) && (inputFanout == 1)) {
            DBSPJoinOperator join = source.node().to(DBSPJoinOperator.class);

            boolean pulled = this.checkPredicatePull(operator, join, false);
            if (pulled) return;

            CalciteRelNode node = join.getRelNode().after(operator.getRelNode());
            DBSPSimpleOperator result =
                    new DBSPJoinFilterMapOperator(node, source.getOutputZSetType(),
                            join.getFunction(), operator.getClosureFunction(), null,
                            join.isMultiset, join.inputs.get(0), join.inputs.get(1), join.balanced)
                            .copyAnnotations(operator);
            this.map(operator, result);
            return;
        } else if (source.node().is(DBSPLeftJoinOperator.class) && (inputFanout == 1)) {
            DBSPLeftJoinOperator join = source.node().to(DBSPLeftJoinOperator.class);

            boolean pulled = this.checkPredicatePull(operator, join, true);
            if (pulled) return;

            CalciteRelNode node = join.getRelNode().after(operator.getRelNode());
            DBSPSimpleOperator result =
                    new DBSPLeftJoinFilterMapOperator(node, source.getOutputZSetType(),
                            join.getFunction(), operator.getClosureFunction(), null,
                            join.isMultiset, join.inputs.get(0), join.inputs.get(1), join.balanced)
                            .copyAnnotations(operator);
            this.map(operator, result);
            return;
        } else if (source.node().is(DBSPStarJoinOperator.class) && (inputFanout == 1)) {
            DBSPStarJoinOperator join = source.node().to(DBSPStarJoinOperator.class);
            CalciteRelNode node = join.getRelNode().after(operator.getRelNode());
            DBSPSimpleOperator result =
                    new DBSPStarJoinFilterMapOperator(node, source.getOutputZSetType(),
                            join.getClosureFunction(), operator.getClosureFunction(), null,
                            join.isMultiset, join.inputs)
                            .copyAnnotations(operator);
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }
}
