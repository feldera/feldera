package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctIncrementalOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertFeedbackOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWeighOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.KeyPropagation;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
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
    public final Map<DBSPOperator, OperatorExpansion> expansion;
    public final NullablePredicate<DBSPOperator> isApendOnly;
    public final NullableFunction<DBSPOperator, KeyPropagation.JoinDescription> joinDescriptions;

    public ExpandOperators(IErrorReporter reporter,
                           NullablePredicate<DBSPOperator> isApendOnly,
                           NullableFunction<DBSPOperator, KeyPropagation.JoinDescription> joinDescriptions) {
        super(reporter, true);
        this.expansion = new HashMap<>();
        this.isApendOnly = isApendOnly;
        this.joinDescriptions = joinDescriptions;
    }

    void addExpansion(DBSPOperator operator, OperatorExpansion expansion) {
        this.expansion.put(operator, expansion);
    }

    void identity(DBSPOperator operator) {
        // Replace an operator with another one of the same kind
        super.replace(operator);
        DBSPOperator replacement = this.mapped(operator);
        this.addExpansion(operator, new ReplacementExpansion(replacement));
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
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
    public void postorder(DBSPSourceMapOperator operator) {
        throw new InternalCompilerError("Didn't expect to find a SourceMapOperator at this stage " + operator);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPViewOperator operator) {
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
    public void postorder(DBSPStreamAggregateOperator operator) {
        if (operator.isLinear) {
            DBSPOperator input = this.mapped(operator.input());
            DBSPExpression function = operator.getAggregate().combineLinear();
            DBSPTypeIndexedZSet ix = input.getOutputIndexedZSetType();
            DBSPVariablePath arg = new DBSPVariablePath(
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
            this.addExpansion(operator, new StreamAggregateExpansion(weigh, result));
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
        this.addExpansion(operator, new AggregateExpansion(integrator, agg, upsert));
        this.map(operator, upsert);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        DBSPOperator input = this.mapped(operator.input());
        DBSPIntegrateOperator integrator = new DBSPIntegrateOperator(operator.getNode(), input);
        this.addOperator(integrator);
        DBSPDistinctIncrementalOperator distinct =
                new DBSPDistinctIncrementalOperator(operator.getNode(), integrator, input);
        this.addExpansion(operator, new DistinctExpansion(integrator, distinct));
        this.map(operator, distinct);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) {
        // This is not true, but we don't care here about the internal structure
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        this.identity(operator);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        List<DBSPOperator> inputs = Linq.map(operator.inputs, this::mapped);

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
        List<DBSPOperator> sumInputs = new ArrayList<>();
        if (hasLeftIntegrator) {
            leftIntegrator = new DBSPDelayedIntegralOperator(operator.getNode(), inputs.get(0));
            leftIntegrator.copyAnnotations(operator.left());
            this.addOperator(leftIntegrator);

            rightJoin = new DBSPStreamJoinOperator(operator.getNode(), operator.getOutputZSetType(),
                    operator.getFunction(), operator.isMultiset, leftIntegrator, inputs.get(1));
            this.addOperator(rightJoin);
            sumInputs.add(rightJoin);
        }
        @Nullable DBSPDelayedIntegralOperator rightIntegrator = null;
        @Nullable DBSPStreamJoinOperator leftJoin = null;
        if (hasRightIntegrator) {
            rightIntegrator = new DBSPDelayedIntegralOperator(operator.getNode(), inputs.get(1));
            rightIntegrator.copyAnnotations(operator.right());
            this.addOperator(rightIntegrator);

            leftJoin = new DBSPStreamJoinOperator(operator.getNode(), operator.getOutputZSetType(),
                    operator.getFunction(), operator.isMultiset, inputs.get(0), rightIntegrator);
            this.addOperator(leftJoin);
            sumInputs.add(leftJoin);
        }
        DBSPStreamJoinOperator deltaJoin = new DBSPStreamJoinOperator(operator.getNode(), operator.getOutputZSetType(),
                operator.getFunction(), operator.isMultiset, inputs.get(0), inputs.get(1));
        this.addOperator(deltaJoin);
        sumInputs.add(deltaJoin);

        DBSPSumOperator sum = new DBSPSumOperator(operator.getNode(), sumInputs);
        this.map(operator, sum);
        this.addExpansion(operator, new JoinExpansion(leftIntegrator, rightIntegrator,
                leftJoin, rightJoin, deltaJoin, sum));
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator operator) {
        List<DBSPOperator> inputs = Linq.map(operator.inputs, this::mapped);

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
        List<DBSPOperator> sumInputs = new ArrayList<>();

        if (hasLeftIntegrator) {
            leftIntegrator = new DBSPDelayedIntegralOperator(operator.getNode(), inputs.get(0));
            this.addOperator(leftIntegrator);
            leftIntegrator.copyAnnotations(operator.left());
            rightJoin = new DBSPStreamJoinOperator(operator.getNode(), operator.getOutputZSetType(),
                    operator.getFunction(), operator.isMultiset, leftIntegrator, inputs.get(1));
            this.addOperator(rightJoin);
            rightFilter = new DBSPFilterOperator(operator.getNode(), operator.getFilter(), rightJoin);
            this.addOperator(rightFilter);
            sumInputs.add(rightFilter);
        }

        if (hasRightIntegrator) {
            rightIntegrator = new DBSPDelayedIntegralOperator(operator.getNode(), inputs.get(1));
            this.addOperator(rightIntegrator);
            rightIntegrator.copyAnnotations(operator.right());
            leftJoin = new DBSPStreamJoinOperator(operator.getNode(), operator.getOutputZSetType(),
                    operator.getFunction(), operator.isMultiset, inputs.get(0), rightIntegrator);
            this.addOperator(leftJoin);
            leftFilter = new DBSPFilterOperator(operator.getNode(), operator.getFilter(), leftJoin);
            this.addOperator(leftFilter);
            sumInputs.add(leftFilter);
        }

        DBSPTypeZSet type = operator.getOutputZSetType();
        DBSPStreamJoinOperator deltaJoin = new DBSPStreamJoinOperator(operator.getNode(), type,
                operator.getFunction(), operator.isMultiset, inputs.get(0), inputs.get(1));
        this.addOperator(deltaJoin);
        DBSPFilterOperator filter = new DBSPFilterOperator(operator.getNode(), operator.getFilter(), deltaJoin);
        this.addOperator(filter);
        sumInputs.add(filter);

        DBSPSumOperator sum = new DBSPSumOperator(operator.getNode(), sumInputs);
        this.map(operator, sum);
        this.addExpansion(operator, new JoinFilterMapExpansion(leftIntegrator, rightIntegrator,
                leftJoin, rightJoin, deltaJoin, leftFilter, rightFilter, filter, sum));
    }
}
