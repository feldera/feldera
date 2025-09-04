package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Expensive;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Maybe;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/** Implement {@link org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator} */
public class ImplementChains extends CircuitCloneVisitor {
    public ImplementChains(DBSPCompiler compiler) {
        super(compiler, false);
    }

    /** Compose pairs of maps that can be efficiently composed, taking into advantage
     * the fact that function composition is associative. */
    DBSPChainOperator.ComputationChain shrink(DBSPChainOperator.ComputationChain chain) {
        List<DBSPChainOperator.Computation> result = new ArrayList<>();
        for (DBSPChainOperator.Computation comp: chain.computations()) {
            if (result.isEmpty() || comp.kind() == DBSPChainOperator.ComputationKind.Filter) {
                result.add(comp);
            } else {
                DBSPChainOperator.Computation last = Utilities.removeLast(result);
                if (last.kind() == DBSPChainOperator.ComputationKind.Filter) {
                    result.add(last);
                    result.add(comp);
                    continue;
                }

                Expensive expensive = new Expensive(compiler);
                expensive.apply(last.closure());
                if (expensive.isExpensive()) {
                    result.add(last);
                } else {
                    DBSPClosureExpression composed;
                    if (last.kind() == DBSPChainOperator.ComputationKind.Map) {
                        composed = comp.closure().applyAfter(compiler, last.closure(), Maybe.MAYBE);
                    } else {
                        DBSPClosureExpression lastFunction = last.closure();
                        DBSPExpression argument = new DBSPRawTupleExpression(
                                lastFunction.body.field(0).borrow(),
                                lastFunction.body.field(1).borrow());
                        DBSPExpression apply = comp.closure().call(argument);
                        composed = apply.reduce(this.compiler())
                                .closure(lastFunction.parameters);
                    }
                    comp = new DBSPChainOperator.Computation(comp.kind(), composed);
                }
                result.add(comp);
            }
        }

        if (result.size() == chain.size())
            return chain;
        return new DBSPChainOperator.ComputationChain(chain.inputType(), result);
    }

    @Override
    public void postorder(DBSPChainOperator node) {
        DBSPChainOperator.ComputationChain chain = this.shrink(node.chain);
        DBSPClosureExpression function = chain.collapse(this.compiler);
        boolean containsFilter = chain.containsFilter();
        DBSPSimpleOperator result;
        if (node.outputType.is(DBSPTypeZSet.class)) {
            if (containsFilter)
                result = new DBSPFlatMapOperator(
                        node.getRelNode(), function, node.getOutputZSetType(),
                        node.isMultiset, this.mapped(node.input()));
            else
                result = new DBSPMapOperator(
                        node.getRelNode(), function, node.getOutputZSetType(),
                        node.isMultiset, this.mapped(node.input()));
        } else {
            if (containsFilter)
                result = new DBSPFlatMapIndexOperator(
                        node.getRelNode(), function, node.getOutputIndexedZSetType(),
                        node.isMultiset, this.mapped(node.input()));
            else
                result = new DBSPMapIndexOperator(
                        node.getRelNode(), function, node.getOutputIndexedZSetType(),
                        node.isMultiset, this.mapped(node.input()));
        }
        this.map(node, result);
    }
}
