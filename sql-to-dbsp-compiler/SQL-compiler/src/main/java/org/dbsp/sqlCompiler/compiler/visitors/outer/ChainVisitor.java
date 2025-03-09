package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;

import javax.annotation.Nullable;

/** Combine chains of Map/MapIndex/Filter into Chain operators */
public class ChainVisitor extends CircuitCloneWithGraphsVisitor {
    public ChainVisitor(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
    }

    @Nullable
    static DBSPChainOperator.Computation getComputation(DBSPSimpleOperator operator) {
        if (operator.is(DBSPMapOperator.class)) {
            if (!operator.getFunction().is(DBSPClosureExpression.class))
                return null;
            return new DBSPChainOperator.Computation(
                    DBSPChainOperator.ComputationKind.Map, operator.getClosureFunction());
        } else if (operator.is(DBSPMapIndexOperator.class)) {
            return new DBSPChainOperator.Computation(
                    DBSPChainOperator.ComputationKind.MapIndex, operator.getClosureFunction());
        } else if (operator.is(DBSPFilterOperator.class)) {
            return new DBSPChainOperator.Computation(
                    DBSPChainOperator.ComputationKind.Filter, operator.getClosureFunction());
        } else {
            return null;
        }
    }

    @Nullable
    DBSPChainOperator chain(DBSPUnaryOperator operator) {
        if (this.getGraph().getFanout(operator.input().node()) != 1)
            return null;
        DBSPChainOperator.Computation computation = getComputation(operator);
        if (computation == null)
            return null;
        OutputPort in = this.mapped(operator.input());
        if (in.node().is(DBSPChainOperator.class)) {
            DBSPChainOperator chainOp = in.node().to(DBSPChainOperator.class);
            DBSPChainOperator.ComputationChain chain = chainOp.chain;
            return new DBSPChainOperator(operator.getNode(),
                    chain.add(computation), operator.isMultiset, chainOp.input());
        } else {
            if (!in.node().is(DBSPSimpleOperator.class))
                return null;
            DBSPSimpleOperator inSimple = in.simpleNode();
            DBSPChainOperator.Computation inComputation = getComputation(inSimple);
            if (inComputation == null)
                return null;
            DBSPChainOperator.ComputationChain chain = new DBSPChainOperator.ComputationChain(
                    inSimple.inputs.get(0).outputType());
            chain = chain.add(inComputation).add(computation);
            return new DBSPChainOperator(operator.getNode(), chain, operator.isMultiset, inSimple.inputs.get(0));
        }
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        DBSPChainOperator chain = this.chain(operator);
        if (chain != null) {
            this.map(operator, chain);
        } else {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        DBSPChainOperator chain = this.chain(operator);
        if (chain != null) {
            this.map(operator, chain);
        } else {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        DBSPChainOperator chain = this.chain(operator);
        if (chain != null) {
            this.map(operator, chain);
        } else {
            super.postorder(operator);
        }
    }
}
