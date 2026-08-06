package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Expensive;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Maybe;

/**
 * Move filters up in a plan, towards sources.
 * - pull a filter above an aggregate if the filter only looks
 * at what the aggregate groups on.
 * - pull a filter above a map/mapindex if it doesn't become "too complex".
 * - pull filters above noop, integrators, differentiators
 * - combine two consecutive filters into one */
public class PullFilterVisitor extends CircuitCloneWithGraphsVisitor {
    public PullFilterVisitor(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        if (inputFanout != 1) {
            super.postorder(operator);
            return;
        }
        if (source.node().is(DBSPAggregateLinearPostprocessOperator.class)
                || source.node().is(DBSPAggregateOperatorBase.class)
                || source.node().is(DBSPChainAggregateOperator.class)) {
            DBSPClosureExpression filter = operator.getClosureFunction();
            DBSPTypeIndexedZSet aggInputType = source.node().inputs.get(0).getOutputIndexedZSetType();
            DBSPType elementType = operator.input().getOutputIndexedZSetType().elementType.ref();
            DBSPVariablePath var = aggInputType.getKVRefType().var();
            DBSPExpression newFilter =
                    filter.call(new DBSPRawTupleExpression(var.field(0), new NoExpression(elementType)))
                            .closure(var)
                            .reduce(this.compiler);
            boolean hasNoExpression = FilterJoinVisitor.ContainsNoExpression.search(this.compiler, newFilter);
            if (!hasNoExpression) {
                DBSPSimpleOperator newFilterOperator = new DBSPFilterOperator(operator.getRelNode(),
                        newFilter, source.simpleNode().inputs.get(0))
                        .copyAnnotations(operator);
                this.addOperator(newFilterOperator);
                DBSPSimpleOperator result =
                        source.simpleNode().withInputs(Linq.list(newFilterOperator.outputPort()), true)
                                .to(DBSPSimpleOperator.class);
                this.map(operator, result);
                return;
            }
        } else if (source.node().is(DBSPMapOperator.class)
                || source.node().is(DBSPMapIndexOperator.class)) {
            DBSPClosureExpression mapClosure = source.simpleNode().getClosureFunction();
            DBSPClosureExpression filterClosure = operator.getClosureFunction();
            // If we combine the two, the body of the map will be essentially executed twice
            // so do it only if the map body is not expensive
            boolean isExpensive = Expensive.isExpensive(compiler, mapClosure);
            if (!isExpensive) {
                final DBSPClosureExpression newFilter;
                if (source.node().is(DBSPMapOperator.class)) {
                    newFilter = filterClosure.applyAfter(this.compiler, mapClosure, Maybe.YES);
                } else {
                    DBSPExpression argument = new DBSPRawTupleExpression(
                            mapClosure.body.field(0).borrow(),
                            mapClosure.body.field(1).borrow());
                    DBSPExpression apply = filterClosure.call(argument).reduce(this.compiler());
                    newFilter = apply.closure(mapClosure.parameters);
                }
                DBSPSimpleOperator newFilterOperator = new DBSPFilterOperator(operator.getRelNode(),
                        newFilter, source.simpleNode().inputs.get(0))
                        .copyAnnotations(operator);
                this.addOperator(newFilterOperator);
                DBSPSimpleOperator result =
                        source.simpleNode().withInputs(Linq.list(newFilterOperator.outputPort()), true)
                                .to(DBSPSimpleOperator.class);
                this.map(operator, result);
                return;
            }
        } else if (source.node().is(DBSPNoopOperator.class)
                || source.node().is(DBSPDifferentiateOperator.class)
                || source.node().is(DBSPIntegrateOperator.class)) {
            DBSPSimpleOperator newFilterOperator =
                    operator.withInputs(Linq.list(source.simpleNode().inputs.get(0)), true)
                            .copyAnnotations(operator)
                            .to(DBSPSimpleOperator.class);
            this.addOperator(newFilterOperator);
            DBSPSimpleOperator result =
                    source.simpleNode().withInputs(Linq.list(newFilterOperator.outputPort()), true)
                            .to(DBSPSimpleOperator.class);
            this.map(operator, result);
            return;
        } else if (source.node().is(DBSPFilterOperator.class)) {
            DBSPClosureExpression clo1 = source.simpleNode().getClosureFunction();
            DBSPClosureExpression clo2 = operator.getClosureFunction();
            DBSPVariablePath var = clo1.parameters[0].type.var();
            DBSPClosureExpression newFilter =
                    new DBSPBinaryExpression(
                            CalciteObject.EMPTY, clo1.getResultType(),
                            DBSPOpcode.AND,
                            clo1.call(var).reduce(this.compiler),
                            clo2.call(var).reduce(this.compiler)).closure(var);
            DBSPSimpleOperator result = new DBSPFilterOperator(
                    operator.getRelNode().after(source.simpleNode().getRelNode()),
                    newFilter, source.simpleNode().inputs.get(0))
                    .copyAnnotations(operator);
            this.map(operator, result);
            return;
        } else if (source.node().is(DBSPConstantOperator.class)) {
            DBSPClosureExpression filter = operator.getClosureFunction();
            DBSPConstantOperator c = source.node().to(DBSPConstantOperator.class);
            DBSPConstantOperator filteredConstant = PropagateConstants.filterConstant(this.compiler, c, filter);
            if (filteredConstant != null) {
                this.map(operator, filteredConstant);
                return;
            }
        }
        super.postorder(operator);
    }
}
