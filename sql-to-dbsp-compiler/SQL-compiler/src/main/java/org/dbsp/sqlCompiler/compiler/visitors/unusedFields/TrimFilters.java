package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.TriFunction;

/** Trim unused fields from filters.
 * This is harder than it looks.
 * A filter propagates unused filters upwards, but only if they are not
 * used in the filter computation itself.
 * The algorithm looks for projections following filters with unused fields,
 * and replaces both of them in one shot. */
public class TrimFilters extends CircuitCloneWithGraphsVisitor {
    final AnalyzedSet<DBSPExpression> functionsAnalyzed;

    public TrimFilters(DBSPCompiler compiler, CircuitGraphs graphs,
                       AnalyzedSet<DBSPExpression> functionsAnalyzed) {
        super(compiler, graphs, false);
        this.functionsAnalyzed = functionsAnalyzed;
    }

    public boolean process(
            DBSPUnaryOperator operator,
            int depth,
            TriFunction<CalciteRelNode, DBSPClosureExpression, OutputPort, DBSPUnaryOperator> constructor) {
        OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        if (!operator.getFunction().is(DBSPClosureExpression.class) ||
            this.functionsAnalyzed.done(operator.getFunction()))
            return false;

        DBSPClosureExpression mapFunction = operator.getClosureFunction();
        assert mapFunction.parameters.length == 1;
        DBSPParameter mapParam = mapFunction.parameters[0];
        if (source.node().is(DBSPFilterOperator.class) && inputFanout == 1) {
            DBSPFilterOperator filter = source.node().to(DBSPFilterOperator.class);
            DBSPClosureExpression filterFunction = filter.getClosureFunction();
            assert filterFunction.parameters.length == 1;
            DBSPParameter filterParam = filterFunction.parameters[0];
            FindUnusedFields mapFinder = new FindUnusedFields(this.compiler);
            mapFinder.findUnusedFields(mapFunction);
            if (mapFinder.foundUnusedFields(depth)) {
                FieldUseMap mapUsed = mapFinder.parameterFieldMap.get(mapParam);

                FindUnusedFields filterFinder = new FindUnusedFields(this.compiler);
                filterFinder.findUnusedFields(filterFunction);
                if (filterFinder.foundUnusedFields(depth)) {
                    FieldUseMap filterUsed = filterFinder.parameterFieldMap.get(filterParam);
                    FieldUseMap reduced = mapUsed.reduce(filterUsed);
                    if (reduced.hasUnusedFields(depth)) {
                        filterFinder.setParameterUseMap(filterParam, reduced);
                        RewriteFields filterRewriter = filterFinder.getFieldRewriter(depth);
                        DBSPClosureExpression newFilterFunc = filterRewriter.apply(filterFunction)
                                .to(DBSPClosureExpression.class);
                        DBSPClosureExpression preProjection = reduced.getProjection(depth);
                        assert preProjection != null;

                        mapFinder.setParameterUseMap(mapParam, reduced);
                        RewriteFields mapRewriter = mapFinder.getFieldRewriter(depth);
                        DBSPClosureExpression post = mapRewriter.rewriteClosure(mapFunction);

                        DBSPMapOperator pre = new DBSPMapOperator(filter.getRelNode(),
                                preProjection, filter.input());
                        this.addOperator(pre);

                        DBSPFilterOperator newFilter = new DBSPFilterOperator(
                                filter.getRelNode(), newFilterFunc, pre.outputPort());
                        this.addOperator(newFilter);

                        DBSPUnaryOperator postProj = constructor.apply(
                                operator.getRelNode(), post, newFilter.outputPort());
                        assert postProj != null;
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
        boolean done = this.process(operator, 1, DBSPMapOperator::new);
        if (!done)
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        @SuppressWarnings("DataFlowIssue")
        boolean done = this.process(operator, 2, DBSPMapIndexOperator::new);
        if (!done)
            super.postorder(operator);
    }
}
