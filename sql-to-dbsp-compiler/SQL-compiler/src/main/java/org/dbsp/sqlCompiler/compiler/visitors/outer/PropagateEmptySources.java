package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIndexedZSetLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Simplifies some operators if they have empty sources.
 */
public class PropagateEmptySources extends CircuitCloneVisitor {
    final List<DBSPOperator> emptySources;

    public PropagateEmptySources(IErrorReporter reporter) {
        super(reporter, false);
        this.emptySources = new ArrayList<>();
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
        DBSPExpression expression = operator.getFunction();
        if (expression.is(DBSPZSetLiteral.class)) {
            DBSPZSetLiteral lit = expression.to(DBSPZSetLiteral.class);
            if (lit.isEmpty())
                this.emptySources.add(operator);
        }
        super.postorder(operator);
    }

    DBSPLiteral emptyLiteral(DBSPType type) {
        if (type.is(DBSPTypeZSet.class)) {
            DBSPTypeZSet z = type.to(DBSPTypeZSet.class);
            return DBSPZSetLiteral.emptyWithElementType(z.elementType);
        } else {
            return new DBSPIndexedZSetLiteral(type.getNode(), type);
        }
    }

    boolean replaceUnary(DBSPUnaryOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (this.emptySources.contains(source)) {
            DBSPType outputType = operator.getType();
            DBSPLiteral value = this.emptyLiteral(outputType);
            DBSPConstantOperator result = new DBSPConstantOperator(operator.getNode(), value, operator.isMultiset);
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
        List<DBSPOperator> newSources = new ArrayList<>();
        for (DBSPOperator prev: operator.inputs) {
            DBSPOperator source = this.mapped(prev);
            if (this.emptySources.contains(source))
                continue;
            newSources.add(source);
        }
        if (newSources.isEmpty()) {
            DBSPLiteral value = this.emptyLiteral(operator.getType());
            DBSPConstantOperator result = new DBSPConstantOperator(operator.getNode(), value, operator.isMultiset);
            this.emptySources.add(result);
            this.map(operator, result);
            return;
        } else if (newSources.size() == 1) {
            this.map(operator, newSources.get(0), false);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSubtractOperator operator) {
        DBSPOperator left = this.mapped(operator.inputs.get(0));
        DBSPOperator right = this.mapped(operator.inputs.get(1));
        if (this.emptySources.contains(right)) {
            if (this.emptySources.contains(left)) {
                DBSPLiteral value = this.emptyLiteral(operator.getType());
                DBSPConstantOperator result = new DBSPConstantOperator(operator.getNode(), value, operator.isMultiset);
                this.emptySources.add(result);
                this.map(operator, result);
            } else {
                this.map(operator, left, false);
            }
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        for (DBSPOperator prev: operator.inputs) {
            DBSPOperator source = this.mapped(prev);
            if (this.emptySources.contains(source)) {
                DBSPLiteral value = this.emptyLiteral(operator.getType());
                DBSPConstantOperator result = new DBSPConstantOperator(operator.getNode(), value, operator.isMultiset);
                this.emptySources.add(result);
                this.map(operator, result);
                return;
            }
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        for (DBSPOperator prev: operator.inputs) {
            DBSPOperator source = this.mapped(prev);
            if (this.emptySources.contains(source)) {
                DBSPLiteral value = this.emptyLiteral(operator.getType());
                DBSPConstantOperator result = new DBSPConstantOperator(operator.getNode(), value, operator.isMultiset);
                this.emptySources.add(result);
                this.map(operator, result);
                return;
            }
        }
        super.postorder(operator);
    }
}
