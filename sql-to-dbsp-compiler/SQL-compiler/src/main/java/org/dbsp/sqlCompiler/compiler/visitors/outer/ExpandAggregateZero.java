package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.GlobalAggregate;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAtomicSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Replaces the {@link DBSPAggregateZeroOperator} with a graph;
 * see the diagram in the definition of this operator. */
public class ExpandAggregateZero extends Passes {
    // Maps the aggregate zero operators in recursive components to constant values that have be inserted
    // in the outer component.
    final Map<DBSPAggregateZeroOperator, DBSPExpression> zeros = new HashMap<>();

    ExpandAggregateZero(DBSPCompiler compiler) {
        super("ExpandAggregateZero", compiler);
        this.add(new CollectZeros(compiler, zeros));
        this.add(new ExpandAggregateZeros(compiler, zeros));
    }

    static class CollectZeros extends CircuitVisitor {
        final Map<DBSPAggregateZeroOperator, DBSPExpression> zeros;

        CollectZeros(DBSPCompiler compiler, Map<DBSPAggregateZeroOperator, DBSPExpression> zeros) {
            super(compiler);
            this.zeros = zeros;
        }

        @Override
        public void postorder(DBSPAggregateZeroOperator operator) {
            DBSPExpression emptySetResult = operator.getFunction();
            DBSPZSetExpression constant = new DBSPZSetExpression(emptySetResult);
            if (this.getParent().is(DBSPNestedOperator.class)) {
                Utilities.putNew(this.zeros, operator, constant);
            }
        }
    }

    static class ExpandAggregateZeros extends CircuitCloneVisitor {
        int crdId = 0;

        final Map<DBSPAggregateZeroOperator, DBSPExpression> zeros;
        final Map<DBSPAggregateZeroOperator, DBSPConstantOperator> zeroOperators;

        public ExpandAggregateZeros(
                DBSPCompiler compiler,
                Map<DBSPAggregateZeroOperator, DBSPExpression> zeros) {
            super(compiler, false);
            this.zeros = zeros;
            this.zeroOperators = new HashMap<>();
        }

        @Override
        public Token startVisit(IDBSPOuterNode circuit) {
            this.crdId = 0;
            return super.startVisit(circuit);
        }

        @Override
        public VisitDecision preorder(DBSPCircuit circuit) {
            // Insert first all the new constants
            VisitDecision result = super.preorder(circuit);
            for (var entry: this.zeros.entrySet()) {
                DBSPAggregateZeroOperator agg = entry.getKey();
                CalciteRelNode node = agg.getRelNode();
                DBSPConstantOperator op = new DBSPConstantOperator(
                        node.intermediate(), entry.getValue(), false);
                this.addOperator(op);
                Utilities.putNew(this.zeroOperators, agg, op);
            }
            return result;
        }

        @Override
        public void postorder(DBSPAggregateZeroOperator operator) {
            int id = this.crdId++;
            GlobalAggregate ga = new GlobalAggregate(id);

            CalciteRelNode node = operator.getRelNode();
            DBSPExpression emptySetResult = operator.getFunction();
            OutputPort input = this.mapped(operator.input());
            DBSPVariablePath _t = emptySetResult.getType().ref().var();
            DBSPClosureExpression toZero = emptySetResult.closure(_t);
            DBSPSimpleOperator map1 = new DBSPMapOperator(node.intermediate(), toZero, input)
                    .addAnnotation(ga, DBSPSimpleOperator.class);
            this.addOperator(map1);
            DBSPSimpleOperator neg = new DBSPNegateOperator(node.intermediate(), map1.outputPort())
                    .addAnnotation(ga, DBSPSimpleOperator.class);
            this.addOperator(neg);
            DBSPSimpleOperator constant;
            if (this.zeroOperators.containsKey(operator)) {
                constant = Utilities.getExists(this.zeroOperators, operator);
                DBSPSimpleOperator delta = new DBSPDeltaOperator(node.intermediate(), constant.outputPort())
                        .addAnnotation(ga, DBSPSimpleOperator.class);
                this.addOperator(delta);
                constant = new DBSPIntegrateOperator(node.intermediate(), delta.outputPort())
                        .addAnnotation(ga, DBSPSimpleOperator.class);
            } else {
                constant = new DBSPConstantOperator(
                        node.intermediate(), new DBSPZSetExpression(emptySetResult), false)
                        .addAnnotation(ga, DBSPSimpleOperator.class);
            }
            this.addOperator(constant);
            DBSPSimpleOperator sum = new DBSPAtomicSumOperator(node, Linq.list(constant.outputPort(), neg.outputPort(), input))
                    .addAnnotation(ga, DBSPSimpleOperator.class);
            this.map(operator, sum);
        }
    }
}
