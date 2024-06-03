package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOutputOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRadixTreeAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedTreeAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertFeedbackOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWeighOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Expands each operator into a lower level representation, that closely
 * mimics the DBSP runtime representation.
 */
public class ExpandOperators extends CircuitCloneVisitor {
    public final int verbosity;
    public final Map<DBSPOperator, OperatorExpansion> expansion;

    public ExpandOperators(IErrorReporter reporter, int verbosity) {
        super(reporter, false);
        this.verbosity = verbosity;
        this.expansion = new HashMap<>();
    }

    @Override
    public void postorder(DBSPIntegrateOperator operator) {
        if (verbosity < 2) {
            this.replace(operator);
            return;
        }

        DBSPOperator input = this.mapped(operator.input());
        DBSPDelayOutputOperator delayOutput = new DBSPDelayOutputOperator(
                operator.getNode(), operator.outputType, operator.input().isMultiset, operator.comment);
        this.addOperator(delayOutput);
        DBSPSumOperator sum = new DBSPSumOperator(operator.getNode(), input, delayOutput);
        this.map(operator, sum);
        DBSPDelayOperator delay = new DBSPDelayOperator(operator.getNode(), sum, delayOutput);
        this.addOperator(delay);
        Utilities.putNew(this.expansion, operator, new IntegralExpansion(delayOutput, sum, delay));
    }

    @Override
    public void postorder(DBSPDelayedIntegralOperator operator) {
        if (verbosity < 2) {
            this.replace(operator);
            return;
        }

        DBSPOperator input = this.mapped(operator.input());
        DBSPDelayOutputOperator delayOutput = new DBSPDelayOutputOperator(
                operator.getNode(), operator.outputType, operator.input().isMultiset, operator.comment);
        this.addOperator(delayOutput);
        DBSPSumOperator sum = new DBSPSumOperator(operator.getNode(), input, delayOutput);
        this.addOperator(sum);
        DBSPDelayOperator delay = new DBSPDelayOperator(operator.getNode(), sum, delayOutput);
        this.map(operator, delay);
        Utilities.putNew(this.expansion, operator, new IntegralExpansion(delayOutput, sum, delay));
    }

    @Override
    public void postorder(DBSPDifferentiateOperator operator) {
        if (verbosity < 2) {
            this.replace(operator);
            return;
        }

        DBSPOperator input = this.mapped(operator.input());
        DBSPDelayOperator delay = new DBSPDelayOperator(operator.getNode(), input);
        this.addOperator(delay);
        DBSPSubtractOperator sub = new DBSPSubtractOperator(operator.getNode(), input, delay);
        this.map(operator, sub);
        Utilities.putNew(this.expansion, operator, new DifferentialExpansion(delay, sub));
    }

    @Override
    public void postorder(DBSPStreamAggregateOperator operator) {
        if (operator.isLinear) {
            DBSPOperator input = this.mapped(operator.input());
            DBSPExpression function = operator.getAggregate().combineLinear();
            DBSPTypeIndexedZSet ix = input.getOutputIndexedZSetType();
            DBSPVariablePath arg = new DBSPVariablePath("kv",
                    new DBSPTypeTuple(ix.keyType.ref(), ix.elementType.ref()));
            DBSPExpression body = function.call(arg.field(1));
            DBSPExpression closure = body.closure(arg.asParameter());
            DBSPWeighOperator weigh = new DBSPWeighOperator(operator.getNode(), closure, input);
            this.addOperator(weigh);
            DBSPExpression weightedSum = new DBSPConstructorExpression(
                    new DBSPPathExpression(
                            DBSPTypeAny.getDefault(),
                            new DBSPPath("WeightedSum")),
                    DBSPTypeAny.getDefault());
            DBSPStreamAggregateOperator result = new DBSPStreamAggregateOperator(operator.getNode(),
                    operator.getOutputIndexedZSetType(), weightedSum, null, weigh, false);
            this.map(operator, result);
            Utilities.putNew(this.expansion, operator, new StreamAggregateExpansion(weigh, result));
        } else {
            this.replace(operator);
        }
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        DBSPOperator input = this.mapped(operator.input());
        /*
        if (operator.isLinear) {
            throw new UnimplementedException(operator.getNode());
        } else {}
        For now treat linear and non-linear operators identically.
         */
        DBSPIntegrateOperator integrator = new DBSPIntegrateOperator(operator.getNode(), input);
        this.addOperator(integrator);
        DBSPPrimitiveAggregateOperator agg = new DBSPPrimitiveAggregateOperator(operator.getNode(),
                operator.function, operator.outputType, input, integrator);
        this.addOperator(agg);
        DBSPUpsertFeedbackOperator upsert = new DBSPUpsertFeedbackOperator(operator.getNode(), agg);
        Utilities.putNew(this.expansion, operator, new AggregateExpansion(integrator, agg, upsert));
        this.map(operator, upsert);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        DBSPOperator input = this.mapped(operator.input());
        DBSPIntegrateOperator integrator = new DBSPIntegrateOperator(operator.getNode(), input);
        this.addOperator(integrator);
        DBSPStreamDistinctOperator distinct = new DBSPStreamDistinctOperator(operator.getNode(), integrator);
        Utilities.putNew(this.expansion, operator, new DistinctExpansion(integrator, distinct));
        this.map(operator, distinct);
    }

    @Override
    public void postorder(DBSPPartitionedTreeAggregateOperator operator) {
        DBSPOperator input = this.mapped(operator.input());

        DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getNode(), input);
        this.addOperator(integral);
        DBSPDelayOutputOperator delayOutput = new DBSPDelayOutputOperator(
                operator.getNode(), operator.outputType, false, operator.comment);
        this.addOperator(delayOutput);
        DBSPOperator result = new DBSPPartitionedRadixTreeAggregateOperator(
                operator.getNode(), operator.function, operator.aggregate, input, integral, delayOutput);
        this.map(operator, result);

        // These two collectively make a delayed integrator operator
        DBSPSumOperator sum = new DBSPSumOperator(operator.getNode(), result, delayOutput);
        this.addOperator(sum);
        DBSPDelayOperator delay = new DBSPDelayOperator(operator.getNode(), sum, delayOutput);
        this.addOperator(delay);
        // TODO: add expansion
    }

    @Override
    public void postorder(DBSPUpsertFeedbackOperator operator) {
        if (verbosity < 2) {
            this.replace(operator);
            return;
        }

        DBSPOperator input = this.mapped(operator.input());
        DBSPDelayOutputOperator delayOutput = new DBSPDelayOutputOperator(
                operator.getNode(), operator.outputType, false, operator.comment);
        this.addOperator(delayOutput);

        DBSPUpsertOperator upsert = new DBSPUpsertOperator(operator.getNode(), input, delayOutput);
        this.map(operator, upsert);
        // These two collectively make a delayed integrator operator
        DBSPSumOperator sum = new DBSPSumOperator(operator.getNode(), upsert, delayOutput);
        this.addOperator(sum);
        DBSPDelayOperator delay = new DBSPDelayOperator(operator.getNode(), sum, delayOutput);
        this.addOperator(delay);
        Utilities.putNew(this.expansion, operator, new UpsertExpansion(delayOutput, upsert, sum, delay));
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        List<DBSPOperator> inputs = Linq.map(operator.inputs, this::mapped);
        DBSPDelayedIntegralOperator leftIntegrator = new DBSPDelayedIntegralOperator(operator.getNode(), inputs.get(0));
        this.addOperator(leftIntegrator);
        DBSPDelayedIntegralOperator rightIntegrator = new DBSPDelayedIntegralOperator(operator.getNode(), inputs.get(1));
        this.addOperator(rightIntegrator);
        DBSPStreamJoinOperator deltaJoin = new DBSPStreamJoinOperator(operator.getNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.isMultiset, inputs.get(0), inputs.get(1));
        this.addOperator(deltaJoin);
        DBSPStreamJoinOperator leftJoin = new DBSPStreamJoinOperator(operator.getNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.isMultiset, inputs.get(0), rightIntegrator);
        this.addOperator(leftJoin);
        DBSPStreamJoinOperator rightJoin = new DBSPStreamJoinOperator(operator.getNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.isMultiset, leftIntegrator, inputs.get(1));
        this.addOperator(rightJoin);
        DBSPSumOperator sum = new DBSPSumOperator(operator.getNode(), deltaJoin, leftJoin, rightJoin);
        this.map(operator, sum);
        Utilities.putNew(this.expansion, operator, new JoinExpansion(leftIntegrator, rightIntegrator,
                leftJoin, rightJoin, deltaJoin, sum));
    }
}
