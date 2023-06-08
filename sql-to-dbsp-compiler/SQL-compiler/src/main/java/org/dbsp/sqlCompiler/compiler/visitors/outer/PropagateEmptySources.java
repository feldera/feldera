package org.dbsp.sqlCompiler.compiler.backend.optimize;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentialOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowAggregateOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.visitors.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIndexedZSetLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;

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
            if (lit.size() == 0)
                this.emptySources.add(operator);
        }
        super.postorder(operator);
    }

    DBSPLiteral emptyLiteral(DBSPType type) {
        if (type.is(DBSPTypeZSet.class))
            return new DBSPZSetLiteral(type);
        else
            return new DBSPIndexedZSetLiteral(null, type);
    }

    boolean replaceUnary(DBSPUnaryOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (this.emptySources.contains(source)) {
            DBSPType outputType = operator.getNonVoidType();
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
    public void postorder(DBSPAggregateOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPIncrementalAggregateOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPWindowAggregateOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPIncrementalDistinctOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPIndexOperator operator) {
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
    public void postorder(DBSPIntegralOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDifferentialOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        if (this.replaceUnary(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPNoopOperator operator) {
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
            DBSPLiteral value = this.emptyLiteral(operator.getNonVoidType());
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
                DBSPLiteral value = this.emptyLiteral(operator.getNonVoidType());
                DBSPConstantOperator result = new DBSPConstantOperator(operator.getNode(), value, operator.isMultiset);
                this.emptySources.add(result);
                this.map(operator, result);
                return;
            } else {
                this.map(operator, left, false);
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
                DBSPLiteral value = this.emptyLiteral(operator.getNonVoidType());
                DBSPConstantOperator result = new DBSPConstantOperator(operator.getNode(), value, operator.isMultiset);
                this.emptySources.add(result);
                this.map(operator, result);
                return;
            }
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPIncrementalJoinOperator operator) {
        for (DBSPOperator prev: operator.inputs) {
            DBSPOperator source = this.mapped(prev);
            if (this.emptySources.contains(source)) {
                DBSPLiteral value = this.emptyLiteral(operator.getNonVoidType());
                DBSPConstantOperator result = new DBSPConstantOperator(operator.getNode(), value, operator.isMultiset);
                this.emptySources.add(result);
                this.map(operator, result);
                return;
            }
        }
        super.postorder(operator);
    }
}
