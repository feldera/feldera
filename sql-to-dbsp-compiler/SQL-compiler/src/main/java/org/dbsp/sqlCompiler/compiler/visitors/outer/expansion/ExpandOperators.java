package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctIncrementalOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertFeedbackOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.KeyPropagation;
import org.dbsp.sqlCompiler.compiler.visitors.outer.recursive.SubstituteLeftJoins;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.NullableFunction;
import org.dbsp.util.NullablePredicate;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Expands each operator into a lower level representation, that closely
 * mimics the DBSP runtime representation. */
public class ExpandOperators extends CircuitCloneVisitor {
    public final Map<DBSPSimpleOperator, OperatorExpansion> expansion;
    public final NullablePredicate<OutputPort> isApendOnly;
    public final NullableFunction<DBSPSimpleOperator, KeyPropagation.JoinDescription> joinDescriptions;

    public ExpandOperators(DBSPCompiler compiler,
                           NullablePredicate<OutputPort> isApendOnly,
                           NullableFunction<DBSPSimpleOperator, KeyPropagation.JoinDescription> joinDescriptions) {
        // Force replacement so all operators are new.
        super(compiler, true);
        this.expansion = new HashMap<>();
        this.isApendOnly = isApendOnly;
        this.joinDescriptions = joinDescriptions;
    }

    void addExpansion(DBSPSimpleOperator operator, OperatorExpansion expansion) {
        this.expansion.put(operator, expansion);
    }

    void identity(DBSPSimpleOperator operator) {
        // Replace an operator with another one of the same kind
        super.replace(operator);
        OutputPort replacement = this.mapped(operator.outputPort());
        this.addExpansion(operator, new ReplacementExpansion(replacement.simpleNode()));
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPSubtractOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPNegateOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPLagOperator operator) {
        // This is not an identity, but we pretend it is
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPHopOperator operator) {
        // This is not exactly true, since hop operators are expanded into a map and
        // a flatmap, but it turns out that for monotonicity analysis it's simpler
        // to analyze the original operator.
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPFlatMapOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPSourceMapOperator operator) {
        throw new InternalCompilerError("Didn't expect to find a SourceMapOperator at this stage " + operator);
    }

    @Override
    public void postorder(DBSPWindowOperator operator) { this.identity(operator); }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPViewOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPAntiJoinOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPStreamAntiJoinOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPFilterOperator node) {
        this.identity(node);
    }

    @Override
    public void postorder(DBSPNoopOperator node) {
        this.identity(node);
    }

    @Override
    public void postorder(DBSPAsofJoinOperator node) {
        // This is not entirely accurate, but for now we don't need a more precise model
        // In fact this join expands into two integrators and a quaternary operator
        // (taking the two deltas and the two integrals as inputs).
        this.identity(node);
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        OutputPort input = this.mapped(operator.input());
        DBSPIntegrateOperator integrator = new DBSPIntegrateOperator(operator.getRelNode(), input);
        this.addOperator(integrator);
        DBSPPrimitiveAggregateOperator agg = new DBSPPrimitiveAggregateOperator(operator.getRelNode(),
                operator.function, operator.outputType, input, integrator.outputPort());
        this.addOperator(agg);
        DBSPUpsertFeedbackOperator upsert = new DBSPUpsertFeedbackOperator(operator.getRelNode(), agg.outputPort());
        this.addExpansion(operator, new AggregateExpansion(integrator, agg, upsert));
        this.map(operator, upsert);
    }

    @Override
    public void postorder(DBSPChainAggregateOperator operator) {
        // We lie about this one, in fact it contains an integrator
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        OutputPort input = this.mapped(operator.input());
        DBSPIntegrateOperator integrator = new DBSPIntegrateOperator(operator.getRelNode(), input);
        this.addOperator(integrator);
        DBSPDistinctIncrementalOperator distinct =
                new DBSPDistinctIncrementalOperator(operator.getRelNode(), integrator.outputPort(), input);
        this.addExpansion(operator, new DistinctExpansion(integrator, distinct));
        this.map(operator, distinct);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) {
        // This is not true, but we don't care here about the internal structure
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
        // This is not true, but we don't care here about the internal structure
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessRetainKeysOperator operator) {
        throw new UnsupportedException("These operators should not have been introduced yet",
                operator.getNode());
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPLeftJoinOperator operator) {
        List<OutputPort> inputs = Linq.map(operator.inputs, this::mapped);

        List<OutputPort> sumInputs = new ArrayList<>();
        DBSPDelayedIntegralOperator leftIntegrator = new DBSPDelayedIntegralOperator(operator.getRelNode(), inputs.get(0));
            leftIntegrator.copyAnnotations(operator.left().node());
            this.addOperator(leftIntegrator);

        DBSPStreamJoinOperator rightJoin = new DBSPStreamJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.isMultiset, leftIntegrator.outputPort(), inputs.get(1));
            this.addOperator(rightJoin);
            sumInputs.add(rightJoin.outputPort());
        DBSPDelayedIntegralOperator rightIntegrator = new DBSPDelayedIntegralOperator(operator.getRelNode(), inputs.get(1));
            rightIntegrator.copyAnnotations(operator.right().node());
            this.addOperator(rightIntegrator);

        DBSPStreamJoinOperator leftJoin = new DBSPStreamJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.isMultiset, inputs.get(0), rightIntegrator.outputPort());
            this.addOperator(leftJoin);
            sumInputs.add(leftJoin.outputPort());
        DBSPStreamJoinOperator deltaJoin = new DBSPStreamJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.isMultiset, inputs.get(0), inputs.get(1));
        this.addOperator(deltaJoin);
        sumInputs.add(deltaJoin.outputPort());

        DBSPAntiJoinOperator antiJoin = new DBSPAntiJoinOperator(operator.getRelNode(), inputs.get(0), inputs.get(1));
        this.addOperator(antiJoin);

        DBSPClosureExpression function = SubstituteLeftJoins.createMapFunction(this.compiler, operator)
                .ensureTree(this.compiler).to(DBSPClosureExpression.class);
        DBSPMapOperator map = new DBSPMapOperator(operator.getRelNode(), function, antiJoin.outputPort());
        this.addOperator(map);
        sumInputs.add(map.outputPort());

        DBSPSumOperator sum = new DBSPSumOperator(operator.getRelNode(), sumInputs);
        this.map(operator, sum);
        this.addExpansion(operator, new LeftJoinExpansion(leftIntegrator, rightIntegrator,
                leftJoin, rightJoin, deltaJoin, antiJoin, map, sum));
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        List<OutputPort> inputs = Linq.map(operator.inputs, this::mapped);

        boolean hasLeftIntegrator = true;
        boolean hasRightIntegrator = true;
        KeyPropagation.JoinDescription jd = this.joinDescriptions.apply(operator);
        if (jd != null) {
            if (jd.leftIsKey()) {
                Boolean appendOnly = this.isApendOnly.test(operator.right());
                if (appendOnly != null && appendOnly) {
                    hasRightIntegrator = false;
                }
            } else {
                Boolean appendOnly = this.isApendOnly.test(operator.left());
                if (appendOnly != null && appendOnly) {
                    hasLeftIntegrator = false;
                }
            }
        }

        @Nullable DBSPDelayedIntegralOperator leftIntegrator = null;
        @Nullable DBSPStreamJoinOperator rightJoin = null;
        List<OutputPort> sumInputs = new ArrayList<>();
        if (hasLeftIntegrator) {
            leftIntegrator = new DBSPDelayedIntegralOperator(operator.getRelNode(), inputs.get(0));
            leftIntegrator.copyAnnotations(operator.left().node());
            this.addOperator(leftIntegrator);

            rightJoin = new DBSPStreamJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                    operator.getFunction(), operator.isMultiset, leftIntegrator.outputPort(), inputs.get(1));
            this.addOperator(rightJoin);
            sumInputs.add(rightJoin.outputPort());
        }
        @Nullable DBSPDelayedIntegralOperator rightIntegrator = null;
        @Nullable DBSPStreamJoinOperator leftJoin = null;
        if (hasRightIntegrator) {
            rightIntegrator = new DBSPDelayedIntegralOperator(operator.getRelNode(), inputs.get(1));
            rightIntegrator.copyAnnotations(operator.right().node());
            this.addOperator(rightIntegrator);

            leftJoin = new DBSPStreamJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                    operator.getFunction(), operator.isMultiset, inputs.get(0), rightIntegrator.outputPort());
            this.addOperator(leftJoin);
            sumInputs.add(leftJoin.outputPort());
        }
        DBSPStreamJoinOperator deltaJoin = new DBSPStreamJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.isMultiset, inputs.get(0), inputs.get(1));
        this.addOperator(deltaJoin);
        sumInputs.add(deltaJoin.outputPort());

        DBSPSumOperator sum = new DBSPSumOperator(operator.getRelNode(), sumInputs);
        this.map(operator, sum);
        this.addExpansion(operator, new JoinExpansion(leftIntegrator, rightIntegrator,
                leftJoin, rightJoin, deltaJoin, sum));
    }

    @Override
    public void postorder(DBSPJoinIndexOperator operator) {
        // Similar to a join, but we do not handle foreign keys,
        // so all integrators are unconditional.
        List<OutputPort> inputs = Linq.map(operator.inputs, this::mapped);

        List<OutputPort> sumInputs = new ArrayList<>();
        DBSPDelayedIntegralOperator leftIntegrator = new DBSPDelayedIntegralOperator(operator.getRelNode(), inputs.get(0));
        leftIntegrator.copyAnnotations(operator.left().node());
        this.addOperator(leftIntegrator);

        DBSPStreamJoinIndexOperator rightJoin = new DBSPStreamJoinIndexOperator(operator.getRelNode(),
                operator.getOutputIndexedZSetType(),
                operator.getFunction(), operator.isMultiset, leftIntegrator.outputPort(), inputs.get(1));
        this.addOperator(rightJoin);
        sumInputs.add(rightJoin.outputPort());
        DBSPDelayedIntegralOperator rightIntegrator = new DBSPDelayedIntegralOperator(operator.getRelNode(), inputs.get(1));
        rightIntegrator.copyAnnotations(operator.right().node());
        this.addOperator(rightIntegrator);

        DBSPStreamJoinIndexOperator leftJoin = new DBSPStreamJoinIndexOperator(operator.getRelNode(),
                operator.getOutputIndexedZSetType(),
                operator.getFunction(), operator.isMultiset, inputs.get(0), rightIntegrator.outputPort());
        this.addOperator(leftJoin);
        sumInputs.add(leftJoin.outputPort());
        DBSPStreamJoinIndexOperator deltaJoin = new DBSPStreamJoinIndexOperator(operator.getRelNode(),
                operator.getOutputIndexedZSetType(),
                operator.getFunction(), operator.isMultiset, inputs.get(0), inputs.get(1));
        this.addOperator(deltaJoin);
        sumInputs.add(deltaJoin.outputPort());

        DBSPSumOperator sum = new DBSPSumOperator(operator.getRelNode(), sumInputs);
        this.map(operator, sum);
        this.addExpansion(operator, new JoinIndexExpansion(leftIntegrator, rightIntegrator,
                leftJoin, rightJoin, deltaJoin, sum));
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator operator) {
        List<OutputPort> inputs = Linq.map(operator.inputs, this::mapped);

        boolean hasLeftIntegrator = true;
        boolean hasRightIntegrator = true;
        KeyPropagation.JoinDescription jd = this.joinDescriptions.apply(operator);
        if (jd != null) {
            if (jd.leftIsKey()) {
                Boolean appendOnly = this.isApendOnly.test(operator.right());
                if (appendOnly != null && appendOnly) {
                    hasRightIntegrator = false;
                }
            } else {
                Boolean appendOnly = this.isApendOnly.test(operator.left());
                if (appendOnly != null && appendOnly) {
                    hasLeftIntegrator = false;
                }
            }
        }

        @Nullable DBSPDelayedIntegralOperator leftIntegrator = null;
        @Nullable DBSPStreamJoinOperator leftJoin = null;
        @Nullable DBSPFilterOperator leftFilter = null;
        @Nullable DBSPDelayedIntegralOperator rightIntegrator = null;
        @Nullable DBSPStreamJoinOperator rightJoin = null;
        @Nullable DBSPFilterOperator rightFilter = null;
        List<OutputPort> sumInputs = new ArrayList<>();

        if (hasLeftIntegrator) {
            leftIntegrator = new DBSPDelayedIntegralOperator(operator.getRelNode(), inputs.get(0));
            this.addOperator(leftIntegrator);
            leftIntegrator.copyAnnotations(operator.left().node());
            rightJoin = new DBSPStreamJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                    operator.getFunction(), operator.isMultiset, leftIntegrator.outputPort(), inputs.get(1));
            this.addOperator(rightJoin);
            rightFilter = new DBSPFilterOperator(operator.getRelNode(), operator.getFilter(), rightJoin.outputPort());
            this.addOperator(rightFilter);
            sumInputs.add(rightFilter.outputPort());
        }

        if (hasRightIntegrator) {
            rightIntegrator = new DBSPDelayedIntegralOperator(operator.getRelNode(), inputs.get(1));
            this.addOperator(rightIntegrator);
            rightIntegrator.copyAnnotations(operator.right().node());
            leftJoin = new DBSPStreamJoinOperator(operator.getRelNode(), operator.getOutputZSetType(),
                    operator.getFunction(), operator.isMultiset, inputs.get(0), rightIntegrator.outputPort());
            this.addOperator(leftJoin);
            leftFilter = new DBSPFilterOperator(operator.getRelNode(), operator.getFilter(), leftJoin.outputPort());
            this.addOperator(leftFilter);
            sumInputs.add(leftFilter.outputPort());
        }

        DBSPTypeZSet type = operator.getOutputZSetType();
        DBSPStreamJoinOperator deltaJoin = new DBSPStreamJoinOperator(operator.getRelNode(), type,
                operator.getFunction(), operator.isMultiset, inputs.get(0), inputs.get(1));
        this.addOperator(deltaJoin);
        DBSPFilterOperator filter = new DBSPFilterOperator(operator.getRelNode(), operator.getFilter(), deltaJoin.outputPort());
        this.addOperator(filter);
        sumInputs.add(filter.outputPort());

        DBSPSumOperator sum = new DBSPSumOperator(operator.getRelNode(), sumInputs);
        this.map(operator, sum);
        this.addExpansion(operator, new JoinFilterMapExpansion(leftIntegrator, rightIntegrator,
                leftJoin, rightJoin, deltaJoin, leftFilter, rightFilter, filter, sum));
    }
}
