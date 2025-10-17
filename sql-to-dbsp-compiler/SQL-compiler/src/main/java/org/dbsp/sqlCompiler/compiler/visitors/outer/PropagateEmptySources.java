package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import java.util.ArrayList;
import java.util.List;

/** Simplifies some operators if they have empty sources. */
public class PropagateEmptySources extends CircuitCloneVisitor {
    final List<DBSPOperator> emptySources;

    public PropagateEmptySources(DBSPCompiler compiler) {
        super(compiler, false);
        this.emptySources = new ArrayList<>();
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
        DBSPExpression expression = operator.getFunction();
        if (expression.is(DBSPZSetExpression.class)) {
            DBSPZSetExpression lit = expression.to(DBSPZSetExpression.class);
            if (lit.isEmpty())
                this.emptySources.add(operator);
        } else if (expression.is(DBSPIndexedZSetExpression.class)) {
            DBSPIndexedZSetExpression lit = expression.to(DBSPIndexedZSetExpression.class);
            if (lit.isEmpty())
                this.emptySources.add(operator);
        }
        super.postorder(operator);
    }

    public static DBSPExpression emptySet(DBSPType type) {
        if (type.is(DBSPTypeZSet.class)) {
            DBSPTypeZSet z = type.to(DBSPTypeZSet.class);
            return DBSPZSetExpression.emptyWithElementType(z.elementType);
        } else {
            return new DBSPIndexedZSetExpression(type.getNode(), type);
        }
    }

    boolean replaceUnary(DBSPUnaryOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (this.emptySources.contains(source.node())) {
            DBSPType outputType = operator.getType();
            DBSPExpression value = emptySet(outputType);
            DBSPConstantOperator result = new DBSPConstantOperator(
                    operator.getRelNode(), value, operator.isMultiset);
            this.emptySources.add(result);
            this.map(operator, result);
            return false;
        }
        return true;
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamAggregateOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPNegateOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPIntegrateOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDifferentiateOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamDistinctOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPNoopOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPViewOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        List<OutputPort> newSources = new ArrayList<>();
        for (OutputPort prev: operator.inputs) {
            OutputPort source = this.mapped(prev);
            if (this.emptySources.contains(source.node()))
                continue;
            newSources.add(source);
        }
        if (newSources.isEmpty()) {
            DBSPExpression value = emptySet(operator.getType());
            DBSPConstantOperator result = new DBSPConstantOperator(
                    operator.getRelNode(), value, operator.isMultiset);
            this.emptySources.add(result);
            this.map(operator, result);
            return;
        } else if (newSources.size() == 1) {
            this.map(operator.outputPort(), newSources.get(0), false);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSubtractOperator operator) {
        OutputPort left = this.mapped(operator.inputs.get(0));
        OutputPort right = this.mapped(operator.inputs.get(1));
        if (this.emptySources.contains(right.node())) {
            if (this.emptySources.contains(left.node())) {
                DBSPExpression value = emptySet(operator.getType());
                DBSPConstantOperator result = new DBSPConstantOperator(
                        operator.getRelNode(), value, operator.isMultiset);
                this.emptySources.add(result);
                this.map(operator, result);
            } else {
                this.map(operator.outputPort(), left, false);
            }
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        for (OutputPort prev: operator.inputs) {
            OutputPort source = this.mapped(prev);
            if (this.emptySources.contains(source.node())) {
                DBSPExpression value = emptySet(operator.getType());
                DBSPConstantOperator result = new DBSPConstantOperator(
                        operator.getRelNode(), value, operator.isMultiset);
                this.emptySources.add(result);
                this.map(operator, result);
                return;
            }
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        for (OutputPort prev: operator.inputs) {
            OutputPort source = this.mapped(prev);
            if (this.emptySources.contains(source.node())) {
                DBSPExpression value = emptySet(operator.getType());
                DBSPConstantOperator result = new DBSPConstantOperator(
                        operator.getRelNode(), value, operator.isMultiset);
                this.emptySources.add(result);
                this.map(operator, result);
                return;
            }
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPLeftJoinOperator operator) {
        OutputPort left = this.mapped(operator.left());
        if (this.emptySources.contains(left.node())) {
            DBSPExpression value = emptySet(operator.getType());
            DBSPConstantOperator result = new DBSPConstantOperator(
                    operator.getRelNode(), value, operator.isMultiset);
            this.emptySources.add(result);
            this.map(operator, result);
            return;
        }
        OutputPort right = this.mapped(operator.right());
        if (this.emptySources.contains(right.node())) {
            // The join becomes a Map.
            // The function |k, l, r| { ... }
            // is specialized by substituting r with None.
            DBSPClosureExpression closure = operator.getClosureFunction();
            DBSPVariablePath var = left.getOutputIndexedZSetType().getKVRefType().var(closure.getNode());
            DBSPClosureExpression function = closure.call(
                    var.field(0).deref().applyCloneIfNeeded().borrow(),
                    var.field(1).deref().applyCloneIfNeeded().borrow(),
                    closure.parameters[2].getType().deref().none().borrow()).closure(var);
            DBSPMapOperator map = new DBSPMapOperator(operator.getRelNode(), function, left);
            this.map(operator, map);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPLeftJoinIndexOperator operator) {
        OutputPort left = this.mapped(operator.left());
        if (this.emptySources.contains(left.node())) {
            DBSPExpression value = emptySet(operator.getType());
            DBSPConstantOperator result = new DBSPConstantOperator(
                    operator.getRelNode(), value, operator.isMultiset);
            this.emptySources.add(result);
            this.map(operator, result);
            return;
        }
        // TODO: could optimize empty RHS, but this probably won't occur
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPLeftJoinFilterMapOperator operator) {
        OutputPort left = this.mapped(operator.left());
        if (this.emptySources.contains(left.node())) {
            DBSPExpression value = emptySet(operator.getType());
            DBSPConstantOperator result = new DBSPConstantOperator(
                    operator.getRelNode(), value, operator.isMultiset);
            this.emptySources.add(result);
            this.map(operator, result);
            return;
        }
        // TODO: could optimize empty RHS, but this probably won't occur
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamAntiJoinOperator operator) {
        // Empty left input -> empty result
        OutputPort left = this.mapped(operator.left());
        if (this.emptySources.contains(left.node())) {
            DBSPExpression value = emptySet(operator.getType());
            DBSPConstantOperator result = new DBSPConstantOperator(
                    operator.getRelNode(), value, operator.isMultiset);
            this.emptySources.add(result);
            this.map(operator, result);
            return;
        }
        // Empty right input -> result is left input
        OutputPort right = this.mapped(operator.right());
        if (this.emptySources.contains(right.node())) {
            this.map(operator.outputPort(), left, false);
            return;
        }
        super.postorder(operator);
    }
}
