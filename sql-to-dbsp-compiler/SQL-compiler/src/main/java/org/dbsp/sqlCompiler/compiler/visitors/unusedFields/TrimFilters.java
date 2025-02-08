package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.util.TriFunction;

/** Trim unused fields from filters.
 * This is harder than it looks.
 * A filter propagates unused filters upwards, but only if they are not
 * used in the filter computation itself.
 * The algorithm looks for projections following filters with unused fields,
 * and replaces both of them in one shot. */
public class TrimFilters extends CircuitCloneWithGraphsVisitor {
    public TrimFilters(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
    }

    public boolean process(
            DBSPUnaryOperator operator,
            TriFunction<CalciteObject, DBSPClosureExpression, OutputPort, DBSPUnaryOperator> constructor) {
        OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        if (!operator.getFunction().is(DBSPClosureExpression.class))
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
            if (mapFinder.foundUnusedFields(2)) {
                FieldUseMap mapUsed = mapFinder.parameterFieldMap.get(mapParam);

                FindUnusedFields filterFinder = new FindUnusedFields(this.compiler);
                filterFinder.findUnusedFields(filterFunction);
                if (filterFinder.foundUnusedFields(2)) {
                    FieldUseMap filterUsed = filterFinder.parameterFieldMap.get(filterParam);
                    FieldUseMap reduced = mapUsed.reduce(filterUsed);
                    if (reduced.hasUnusedFields(2)) {
                        filterFinder.setParameterUseMap(filterParam, reduced);
                        RewriteFields filterRewriter = filterFinder.getFieldRewriter(1);
                        DBSPClosureExpression newFilterFunc = filterRewriter.apply(filterFunction)
                                .to(DBSPClosureExpression.class);
                        DBSPClosureExpression preProjection = reduced.getProjection(1);
                        assert preProjection != null;

                        mapFinder.setParameterUseMap(mapParam, reduced);
                        RewriteFields mapRewriter = mapFinder.getFieldRewriter(2);
                        DBSPClosureExpression post = mapRewriter.rewriteClosure(mapFunction);

                        DBSPMapOperator pre = new DBSPMapOperator(filter.getNode(),
                                preProjection, filter.input());
                        this.addOperator(pre);

                        DBSPFilterOperator newFilter = new DBSPFilterOperator(
                                filter.getNode(), newFilterFunc, pre.outputPort());
                        this.addOperator(newFilter);

                        DBSPUnaryOperator postProj = constructor.apply(
                                operator.getNode(), post, newFilter.outputPort());
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
