package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.TriFunction;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/** Trim unused fields from filters.
 * This is harder than it looks.
 * A filter propagates unused fields upwards, but only if they are not
 * used in the filter computation itself.
 * The algorithm looks for projections following filters with unused fields,
 * and replaces both of them in one shot. */
public class TrimFilters extends CircuitCloneWithGraphsVisitor {
    // Map operators following the filters which were analyzed
    final AnalyzedSet<DBSPOperator> mapAfterFilter;

    public TrimFilters(DBSPCompiler compiler, CircuitGraphs graphs,
                       AnalyzedSet<DBSPOperator> mapAfterFilter) {
        super(compiler, graphs);
        this.mapAfterFilter = mapAfterFilter;
    }

    /** Readers available to this iteration. The Java oracle peeks before running
     * its visitor, while the live Rust pass consumes the same analysis state. */
    public List<Long> candidateReaders(ICircuit circuit, boolean consume) {
        var readers = new ArrayList<Long>();
        this.collectReaders(circuit, consume, readers);
        return readers;
    }

    private void collectReaders(ICircuit circuit, boolean consume, List<Long> readers) {
        for (var op : circuit.getAllOperators()) {
            if (op instanceof DBSPNestedOperator nested)
                this.collectReaders(nested, consume, readers);
            if (!(op instanceof DBSPMapOperator || op instanceof DBSPMapIndexOperator))
                continue;
            var map = op.to(DBSPUnaryOperator.class);
            if (!(map.getFunction() instanceof DBSPClosureExpression))
                continue;
            boolean seen = consume ? this.mapAfterFilter.done(op) : this.mapAfterFilter.contains(op);
            if (!seen && map.input().node() instanceof DBSPFilterOperator)
                readers.add(op.id);
        }
    }

    public boolean process(
            DBSPUnaryOperator operator,
            TriFunction<CalciteRelNode, DBSPClosureExpression, OutputPort, DBSPUnaryOperator> constructor) {
        final OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        if (!operator.getFunction().is(DBSPClosureExpression.class) ||
                this.mapAfterFilter.done(operator))
            return false;

        DBSPClosureExpression mapFunction = operator.getClosureFunction();
        Utilities.enforce(mapFunction.parameters.length == 1);
        DBSPParameter mapParam = mapFunction.parameters[0];
        if (source.node().is(DBSPFilterOperator.class) && inputFanout == 1) {
            DBSPFilterOperator filter = source.node().to(DBSPFilterOperator.class);
            DBSPClosureExpression filterFunction = filter.getClosureFunction();
            Utilities.enforce(filterFunction.parameters.length == 1);
            DBSPParameter filterParam = filterFunction.parameters[0];
            FindUsedFields mapFinder = new FindUsedFields(this.compiler);
            var mapUse = mapFinder.findUsedFields(mapFunction);
            final int depth;
            if (filter.input().outputType().is(DBSPTypeIndexedZSet.class))
                depth = 2;
            else
                depth = 1;

            if (mapUse.hasUnusedFields(depth)) {
                FieldUseMap mapUsed = mapUse.get(mapParam);

                FindUsedFields filterFinder = new FindUsedFields(this.compiler);
                var filterUse = filterFinder.findUsedFields(filterFunction);
                if (filterUse.hasUnusedFields(depth)) {
                    FieldUseMap filterUsed = filterUse.get(filterParam);
                    FieldUseMap reduced = mapUsed.reduce(filterUsed);
                    if (reduced.hasUnusedFields(depth)) {
                        filterUse.updateParameterValue(filterParam, reduced);
                        RewriteFields filterRewriter = filterUse.getFieldRewriter(this.compiler, depth);
                        DBSPClosureExpression newFilterFunc = filterRewriter.apply(filterFunction)
                                .to(DBSPClosureExpression.class);
                        DBSPClosureExpression preProjection = reduced.getProjection(depth);
                        Utilities.enforce(preProjection != null);

                        mapUse.updateParameterValue(mapParam, reduced);
                        RewriteFields mapRewriter = mapUse.getFieldRewriter(this.compiler, depth);
                        DBSPClosureExpression post = mapRewriter.rewriteClosure(mapFunction);

                        DBSPMapOperator pre = new DBSPMapOperator(filter.getRelNode(),
                                preProjection, filter.input());
                        this.addOperator(pre);

                        DBSPFilterOperator newFilter = new DBSPFilterOperator(
                                filter.getRelNode(), newFilterFunc, pre.outputPort());
                        this.addOperator(newFilter);

                        DBSPUnaryOperator postProj = constructor.apply(
                                operator.getRelNode(), post, newFilter.outputPort());
                        Utilities.enforce(postProj != null);
                        this.map(operator, postProj);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        @SuppressWarnings("DataFlowIssue")
        boolean done = this.process(operator, DBSPMapOperator::new);
        if (!done)
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        @SuppressWarnings("DataFlowIssue")
        boolean done = this.process(operator, DBSPMapIndexOperator::new);
        if (!done)
            super.postorder(operator);
    }
}
