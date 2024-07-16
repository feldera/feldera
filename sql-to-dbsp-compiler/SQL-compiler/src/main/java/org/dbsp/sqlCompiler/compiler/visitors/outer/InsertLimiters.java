package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.apache.commons.math3.util.Pair;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApply2Operator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPControlledFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.IHasColumnsMetadata;
import org.dbsp.sqlCompiler.compiler.IHasLateness;
import org.dbsp.sqlCompiler.compiler.IHasWatermark;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.ScalarMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.AggregateExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.CommonJoinExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.DistinctExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.JoinExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.JoinFilterMapExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.OperatorExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.ReplacementExpansion;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.annotation.AlwaysMonotone;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.IsBoundedType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeTypedBox;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** As a result of the Monotonicity analysis, this pass inserts new operators:
 * - ControlledFilter operators to throw away tuples that are not "useful"
 * - apply operators that compute the bounds that drive the controlled filters
 * - waterline operators near sources with lateness information
 * - DBSPIntegrateTraceRetainKeysOperator to prune data from integral operators
 * - DBSPPartitionedRollingAggregateWithWaterline operators
 *
 * <P>This visitor is tricky because it operates on a circuit, but takes the information
 * required to rewrite the graph from a different circuit, the expandedCircuit and
 * the expandedInto map.  Moreover, the current circuit is being rewritten by the
 * visitor while it is being processed.
 **/
public class InsertLimiters extends CircuitCloneVisitor {
    /** For each operator in the expansion of the operators of this circuit
     * the list of its monotone output columns */
    public final Monotonicity.MonotonicityInformation expansionMonotoneValues;
    /** Circuit that contains the expansion of the circuit we are modifying */
    public final DBSPCircuit expandedCircuit;
    /** Maps each original operator to the set of operators it was expanded to */
    public final Map<DBSPOperator, OperatorExpansion> expandedInto;
    /** Maps each operator to the one that computes its lower bound.
     * The keys in this map can be both operators from this circuit and from
     * the expanded circuit. */
    public final Map<DBSPOperator, DBSPOperator> bound;

    public InsertLimiters(IErrorReporter reporter,
                          DBSPCircuit expandedCircuit,
                          Monotonicity.MonotonicityInformation expansionMonotoneValues,
                          Map<DBSPOperator, OperatorExpansion> expandedInto) {
        super(reporter, false);
        this.expandedCircuit = expandedCircuit;
        this.expansionMonotoneValues = expansionMonotoneValues;
        this.expandedInto = expandedInto;
        this.bound = new HashMap<>();
    }

    void markBound(DBSPOperator operator, DBSPOperator bound) {
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Bound for ")
                .append(operator.toString())
                .append(" computed by ")
                .append(bound.toString())
                .newline();
        Utilities.putNew(this.bound, operator, bound);
    }

    /**
     * @param operatorFromExpansion Operator produced as the expansion of
     *                              another operator.
     * @param input                 Input of the operatorFromExpansion which
     *                              is used.
     * @return Add an operator which computes the smallest legal value
     * for the output of an operator. */
    @SuppressWarnings("SameParameterValue")
    @Nullable
    DBSPApplyOperator addBounds(@Nullable DBSPOperator operatorFromExpansion, int input) {
        if (operatorFromExpansion == null)
            return null;
        MonotoneExpression monotone = this.expansionMonotoneValues.get(operatorFromExpansion);
        if (monotone == null)
            return null;
        DBSPOperator source = operatorFromExpansion.inputs.get(input);  // Even for binary operators
        DBSPOperator boundSource = Utilities.getExists(this.bound, source);
        DBSPClosureExpression function = monotone.getReducedExpression().to(DBSPClosureExpression.class);
        DBSPApplyOperator bound = new DBSPApplyOperator(operatorFromExpansion.getNode(), function,
                function.getFunctionType().resultType, boundSource,
                "(" + operatorFromExpansion.getDerivedFrom() + ")");
        this.addOperator(bound);  // insert directly into circuit
        this.markBound(operatorFromExpansion, bound);
        return bound;
    }

    void nonMonotone(DBSPOperator operator) {
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Not monotone: ")
                .append(operator.toString())
                .newline();
    }

    @Nullable
    ReplacementExpansion getReplacement(DBSPOperator operator) {
        OperatorExpansion expanded = this.expandedInto.get(operator);
        if (expanded == null)
            return null;
        return expanded.to(ReplacementExpansion.class);
    }

    @Override
    public void postorder(DBSPHopOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null)
            this.addBounds(expanded.replacement, 0);
        else
            this.nonMonotone(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null)
            this.addBounds(expanded.replacement, 0);
        else
            this.nonMonotone(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null) {
            DBSPOperator bound = this.addBounds(expanded.replacement, 0);
            if (operator != expanded.replacement && bound != null)
                this.markBound(operator, bound);
        } else {
            this.nonMonotone(operator);
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null) {
            DBSPOperator bound = this.processFilter(expanded.replacement.to(DBSPFilterOperator.class));
            if (operator != expanded.replacement && bound != null) {
                this.markBound(operator, bound);
            }
        } else {
            this.nonMonotone(operator);
        }
        super.postorder(operator);
    }

    @Nullable
    DBSPOperator processFilter(DBSPFilterOperator expansion) {
        return this.addBounds(expansion, 0);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null) {
            DBSPOperator bound = this.addBounds(expanded.replacement, 0);
            if (operator != expanded.replacement && bound != null)
                this.markBound(operator, bound);
        } else {
            this.nonMonotone(operator);
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPAggregateOperator aggregator) {
        DBSPOperator source = this.mapped(aggregator.input());
        OperatorExpansion expanded = this.expandedInto.get(aggregator);
        if (expanded == null) {
            this.nonMonotone(aggregator);
            super.postorder(aggregator);
            return;
        }

        AggregateExpansion ae = expanded.to(AggregateExpansion.class);
        DBSPOperator limiter = this.bound.get(aggregator.input());
        if (limiter == null) {
            super.postorder(aggregator);
            this.nonMonotone(aggregator);
            return;
        }

        DBSPOperator filteredAggregator = aggregator.withInputs(Linq.list(source), false);
        // We use the input 0; input 1 comes from the integrator
        DBSPOperator limiter2 = this.addBounds(ae.aggregator, 0);
        if (limiter2 == null) {
            this.map(aggregator, filteredAggregator);
            return;
        }

        this.addOperator(filteredAggregator);
        MonotoneExpression monotoneValue2 = this.expansionMonotoneValues.get(ae.aggregator);
        IMaybeMonotoneType projection2 = Monotonicity.getBodyType(Objects.requireNonNull(monotoneValue2));

        // The before and after filters are actually identical for now.
        DBSPIntegrateTraceRetainKeysOperator before = DBSPIntegrateTraceRetainKeysOperator.create(
                aggregator.getNode(), source, projection2, limiter2);
        this.addOperator(before);
        // output of 'before' is not used in the graph, but the DBSP Rust layer will use it

        DBSPIntegrateTraceRetainKeysOperator after = DBSPIntegrateTraceRetainKeysOperator.create(
                aggregator.getNode(), filteredAggregator, projection2, limiter2);
        this.addOperator(after);
        // output of 'after'' is not used in the graph, but the DBSP Rust layer will use it

        DBSPApplyOperator limiter3 = this.addBounds(ae.upsert, 0);
        this.markBound(aggregator, Objects.requireNonNull(limiter3));

        this.map(aggregator, filteredAggregator, false);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded == null) {
            super.postorder(operator);
            this.nonMonotone(operator);
            return;
        }

        DBSPOperator source = expanded.replacement.inputs.get(0);
        MonotoneExpression inputValue = this.expansionMonotoneValues.get(source);
        if (inputValue == null) {
            super.postorder(operator);
            this.nonMonotone(operator);
            return;
        }

        DBSPOperator boundSource = this.bound.get(source);
        if (boundSource == null) {
            super.postorder(operator);
            this.nonMonotone(operator);
            return;
        }

        // Preserve the field that the data is indexed on from the source
        IMaybeMonotoneType projection = Monotonicity.getBodyType(inputValue);
        PartiallyMonotoneTuple tuple = projection.to(PartiallyMonotoneTuple.class);
        IMaybeMonotoneType tuple0 = tuple.getField(0);
        // Drop field 1 of the value projection.
        if (!tuple0.mayBeMonotone()) {
            super.postorder(operator);
            this.nonMonotone(operator);
            return;
        }

        // Compute the waterline for the new rolling aggregate operator
        DBSPTypeTupleBase varType = projection.getType().to(DBSPTypeTupleBase.class);
        assert varType.size() == 2 : "Expected a pair, got " + varType;
        varType = new DBSPTypeRawTuple(varType.tupFields[0].ref(), varType.tupFields[1].ref());
        final DBSPVariablePath var = varType.var();
        DBSPExpression body = var.field(0).deref();
        body = this.wrapTypedBox(body, true);
        DBSPClosureExpression closure = body.closure(var.asParameter());
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.errorReporter, operator, MonotoneTransferFunctions.ArgumentKind.IndexedZSet, projection);
        MonotoneExpression monotone = analyzer.applyAnalysis(closure);
        Objects.requireNonNull(monotone);

        DBSPClosureExpression function = monotone.getReducedExpression().to(DBSPClosureExpression.class);
        DBSPOperator waterline = new DBSPApplyOperator(operator.getNode(), function,
                function.getFunctionType().resultType, boundSource,
                "(" + operator.getDerivedFrom() + ")");
        this.addOperator(waterline);
        Logger.INSTANCE.belowLevel(this, 2)
                .append("WATERLINE FUNCTION: ")
                .append(function)
                .newline();

        // The bound for the output is different from the waterline
        body = new DBSPRawTupleExpression(new DBSPTupleExpression(var.field(0).deref()));
        closure = body.closure(var.asParameter());
        analyzer = new MonotoneTransferFunctions(
                this.errorReporter, operator, MonotoneTransferFunctions.ArgumentKind.IndexedZSet, projection);
        monotone = analyzer.applyAnalysis(closure);
        Objects.requireNonNull(monotone);

        function = monotone.getReducedExpression().to(DBSPClosureExpression.class);
        Logger.INSTANCE.belowLevel(this, 2)
                .append("BOUND FUNCTION: ")
                .append(function)
                .newline();

        DBSPOperator bound = new DBSPApplyOperator(operator.getNode(), function,
                function.getFunctionType().resultType, boundSource,
                "(" + operator.getDerivedFrom() + ")");
        this.addOperator(bound);
        this.markBound(expanded.replacement, bound);

        DBSPPartitionedRollingAggregateWithWaterlineOperator replacement =
                new DBSPPartitionedRollingAggregateWithWaterlineOperator(operator.getNode(),
                        operator.partitioningFunction, operator.function, operator.aggregate,
                        operator.lower, operator.upper, operator.getOutputIndexedZSetType(),
                        this.mapped(operator.input()), waterline);
        this.map(operator, replacement);
    }

    public void postorder(DBSPStreamJoinOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null)
            this.processStreamJoin(expanded.replacement.to(DBSPStreamJoinOperator.class));
        else
            this.nonMonotone(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        OperatorExpansion expanded = this.expandedInto.get(operator);
        if (expanded == null) {
            super.postorder(operator);
            this.nonMonotone(operator);
            return;
        }
        DistinctExpansion expansion = expanded.to(DistinctExpansion.class);
        DBSPOperator sourceLimiter = this.bound.get(operator.input());
        if (sourceLimiter == null) {
            super.postorder(operator);
            this.nonMonotone(operator);
            return;
        }

        DBSPOperator result = operator.withInputs(Linq.list(source), false);
        MonotoneExpression sourceMonotone = this.expansionMonotoneValues.get(expansion.distinct.right());
        IMaybeMonotoneType projection = Monotonicity.getBodyType(Objects.requireNonNull(sourceMonotone));
        DBSPIntegrateTraceRetainKeysOperator r = DBSPIntegrateTraceRetainKeysOperator.create(
                        operator.getNode(), source, projection, sourceLimiter);
        this.addOperator(r);
        // Same limiter as the source
        this.markBound(expansion.distinct, sourceLimiter);
        this.markBound(operator, sourceLimiter);
        this.map(operator, result, true);
    }

    @Nullable
    DBSPOperator gcJoin(DBSPBinaryOperator join, CommonJoinExpansion expansion) {
        DBSPOperator leftLimiter = this.bound.get(join.left());
        DBSPOperator rightLimiter = this.bound.get(join.right());
        if (leftLimiter == null && rightLimiter == null) {
            return null;
        }

        DBSPOperator left = this.mapped(join.left());
        DBSPOperator right = this.mapped(join.right());
        DBSPOperator result = join.withInputs(Linq.list(left, right), false);
        if (leftLimiter != null) {
            MonotoneExpression leftMonotone = this.expansionMonotoneValues.get(
                    expansion.getLeftIntegrator().input());
            // Yes, the limit of the left input is applied to the right one.
            IMaybeMonotoneType leftProjection = Monotonicity.getBodyType(Objects.requireNonNull(leftMonotone));
            // Check if the "key" field is monotone
            if (leftProjection.to(PartiallyMonotoneTuple.class).getField(0).mayBeMonotone()) {
                DBSPIntegrateTraceRetainKeysOperator r = DBSPIntegrateTraceRetainKeysOperator.create(
                        join.getNode(), right, leftProjection, leftLimiter);
                this.addOperator(r);
            }
        }

        if (rightLimiter != null) {
            MonotoneExpression rightMonotone = this.expansionMonotoneValues.get(
                    expansion.getRightIntegrator().input());
            // Yes, the limit of the right input is applied to the left one.
            IMaybeMonotoneType rightProjection = Monotonicity.getBodyType(Objects.requireNonNull(rightMonotone));
            // Check if the "key" field is monotone
            if (rightProjection.to(PartiallyMonotoneTuple.class).getField(0).mayBeMonotone()) {
                DBSPIntegrateTraceRetainKeysOperator l = DBSPIntegrateTraceRetainKeysOperator.create(
                        join.getNode(), left, rightProjection, rightLimiter);
                this.addOperator(l);
            }
        }
        return result;
    }

    @Override
    public void postorder(DBSPJoinOperator join) {
        OperatorExpansion expanded = this.expandedInto.get(join);
        if (expanded == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }
        JoinExpansion expansion = expanded.to(JoinExpansion.class);
        DBSPOperator result = this.gcJoin(join, expansion);
        if (result == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

        this.processIntegral(expansion.leftIntegrator);
        this.processIntegral(expansion.rightIntegrator);
        this.processStreamJoin(expansion.leftDelta);
        this.processStreamJoin(expansion.rightDelta);
        this.processStreamJoin(expansion.both);
        this.processSum(expansion.sum);

        this.map(join, result, true);
    }

    private void processIntegral(DBSPDelayedIntegralOperator replacement) {
        if (replacement.hasAnnotation(a -> a.is(AlwaysMonotone.class))) {
            DBSPOperator limiter = this.bound.get(replacement.input());
            if (limiter != null) {
                this.markBound(replacement, limiter);
            } else {
                this.nonMonotone(replacement);
            }
        }
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator join) {
        OperatorExpansion expanded = this.expandedInto.get(join);
        if (expanded == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }
        JoinFilterMapExpansion expansion = expanded.to(JoinFilterMapExpansion.class);
        DBSPOperator result = this.gcJoin(join, expansion);
        if (result == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

        this.processIntegral(expansion.leftIntegrator);
        this.processIntegral(expansion.rightIntegrator);
        this.processStreamJoin(expansion.leftDelta);
        this.processStreamJoin(expansion.rightDelta);
        this.processStreamJoin(expansion.both);
        this.processFilter(expansion.filter);
        this.processFilter(expansion.leftFilter);
        this.processFilter(expansion.rightFilter);
        this.processSum(expansion.sum);

        // If one of the filters leftFilter or rightFilter has monotone outputs,
        // we can use these to GC the input of the join using
        // DBSPIntegrateTraceRetainValuesOperator.

        Projection proj = new Projection(this.errorReporter);
        proj.apply(join.getFunction());
        assert(proj.isProjection);
        List<Pair<Integer, Integer>> outputs = proj.getOutputs();
        int leftSize = Linq.where(outputs, o -> o.getFirst() == 1).size();
        int rightSize = outputs.size() - leftSize;

        DBSPTypeTuple keyType = join.getKeyType().to(DBSPTypeTuple.class);
        int keySize = keyType.size();
        List<IMaybeMonotoneType> keyParts = new ArrayList<>();
        for (int i = 0; i < keySize; i++)
            // a key projection with no monotone fields
            keyParts.add(NonMonotoneType.nonMonotone(keyType.getFieldType(i)));
        PartiallyMonotoneTuple keyPart = new PartiallyMonotoneTuple(keyParts, false, false);

        // Check the left side and insert a GC operator if possible
        DBSPOperator leftLimiter = this.bound.get(expansion.leftFilter);
        if (leftLimiter != null) {
            MonotoneExpression monotone = this.expansionMonotoneValues.get(expansion.leftFilter);
            IMaybeMonotoneType projection = Monotonicity.getBodyType(Objects.requireNonNull(monotone));
            if (projection.mayBeMonotone()) {
                PartiallyMonotoneTuple tuple = projection.to(PartiallyMonotoneTuple.class);
                assert tuple.size() == outputs.size();

                // Reorganize the tuple into two tuples for left and right fields
                List<IMaybeMonotoneType> value = new ArrayList<>();
                for (int i = 0; i < leftSize; i++)
                    // keep the monotone fields from the value
                    value.add(tuple.getField(i));
                PartiallyMonotoneTuple valuePart = new PartiallyMonotoneTuple(value, false, false);

                // Put the fields together
                PartiallyMonotoneTuple together = new PartiallyMonotoneTuple(
                        Linq.list(keyPart, valuePart), true, false);

                // From the leftLimiter we need to keep only the fields for the left part
                DBSPVariablePath var = Objects.requireNonNull(tuple.getProjectedType()).ref().var();
                List<DBSPExpression> monotoneFields = new ArrayList<>();
                int index = 0;
                for (int i = 0; i < leftSize; i++) {
                    if (tuple.getField(i).mayBeMonotone())
                        monotoneFields.add(var.deref().field(index++));
                }

                DBSPExpression func = new DBSPTupleExpression(monotoneFields, false);
                DBSPApplyOperator extractLeft = new DBSPApplyOperator(
                        join.getNode(), func.closure(var.asParameter()), func.getType(), leftLimiter, null);
                this.addOperator(extractLeft);

                DBSPIntegrateTraceRetainValuesOperator l = DBSPIntegrateTraceRetainValuesOperator.create(
                        join.getNode(), this.mapped(join.left()), together, extractLeft);
                this.addOperator(l);
            }
        }

        // Exact same procedure on the right hand side
        DBSPOperator rightLimiter = this.bound.get(expansion.rightFilter);
        if (rightLimiter != null) {
            MonotoneExpression monotone = this.expansionMonotoneValues.get(expansion.rightFilter);
            IMaybeMonotoneType projection = Monotonicity.getBodyType(Objects.requireNonNull(monotone));
            if (projection.mayBeMonotone()) {
                PartiallyMonotoneTuple tuple = projection.to(PartiallyMonotoneTuple.class);
                assert tuple.size() == outputs.size();

                // Reorganize the tuple into two tuples for left and right fields
                List<IMaybeMonotoneType> value = new ArrayList<>();
                for (int i = 0; i < rightSize; i++)
                    // keep the monotone fields from the value
                    value.add(tuple.getField(leftSize + i));
                PartiallyMonotoneTuple valuePart = new PartiallyMonotoneTuple(value, false, false);

                // Put the fields together
                PartiallyMonotoneTuple together = new PartiallyMonotoneTuple(
                        Linq.list(keyPart, valuePart), true, false);

                // From the rightLimiter we need to keep only the fields for the right part
                DBSPVariablePath var = Objects.requireNonNull(tuple.getProjectedType()).ref().var();
                List<DBSPExpression> monotoneFields = new ArrayList<>();
                int index = 0;
                for (int i = 0; i < leftSize; i++) {
                    if (tuple.getField(i).mayBeMonotone())
                        index++;
                }
                for (int i = 0; i < rightSize; i++) {
                    if (tuple.getField(leftSize + i).mayBeMonotone())
                        monotoneFields.add(var.deref().field(index++));
                }

                DBSPExpression func = new DBSPTupleExpression(monotoneFields, false);
                DBSPApplyOperator extractRight = new DBSPApplyOperator(
                        join.getNode(), func.closure(var.asParameter()), func.getType(), rightLimiter, null);
                this.addOperator(extractRight);

                DBSPIntegrateTraceRetainValuesOperator r = DBSPIntegrateTraceRetainValuesOperator.create(
                        join.getNode(), this.mapped(join.right()), together, extractRight);
                this.addOperator(r);
            }
        }

        this.map(join, result, true);
    }

    /** Given two expressions with the same type, compute the MAX expression pointwise,
     * only on their monotone fields.
     * @param left             Left expression.
     * @param right            Right expression.
     * @param leftProjection   Describes monotone fields of left expression.
     * @param rightProjection  Describes monotone fields of right expression. */
    DBSPExpression max(DBSPExpression left,
                       DBSPExpression right,
                       IMaybeMonotoneType leftProjection,
                       IMaybeMonotoneType rightProjection) {
        if (leftProjection.is(ScalarMonotoneType.class)) {
            if (leftProjection.is(NonMonotoneType.class)) {
                return right;
            } else if (rightProjection.is(NonMonotoneType.class)) {
                return left;
            } else {
                return ExpressionCompiler.makeBinaryExpression(left.getNode(),
                        left.getType(), DBSPOpcode.MAX, left, right);
            }
        } else if (leftProjection.is(PartiallyMonotoneTuple.class)) {
            PartiallyMonotoneTuple l = leftProjection.to(PartiallyMonotoneTuple.class);
            PartiallyMonotoneTuple r = rightProjection.to(PartiallyMonotoneTuple.class);
            assert r.size() == l.size();
            List<DBSPExpression> fields = new ArrayList<>();
            int leftIndex = 0;
            int rightIndex = 0;
            for (int i = 0; i < l.size(); i++) {
                IMaybeMonotoneType li = l.getField(i);
                IMaybeMonotoneType ri = r.getField(i);
                DBSPExpression le = null;
                DBSPExpression re = null;
                if (li.mayBeMonotone()) {
                    le = left.field(leftIndex++);
                }
                if (ri.mayBeMonotone()) {
                    re = right.field(rightIndex++);
                }
                if (le == null && re == null)
                    continue;
                if (le == null) {
                    fields.add(re);
                } else if (re == null) {
                    fields.add(le);
                } else {
                    fields.add(max(le, re, li, ri));
                }
            }
            return new DBSPTupleExpression(CalciteObject.EMPTY, fields);
        }
        throw new UnimplementedException(left.getNode());
    }

    void processStreamJoin(DBSPStreamJoinOperator expanded) {
        String comment = "(" + expanded.getDerivedFrom() + ")";
        MonotoneExpression monotoneValue = this.expansionMonotoneValues.get(expanded);
        if (monotoneValue == null || !monotoneValue.mayBeMonotone()) {
            this.nonMonotone(expanded);
            return;
        }
        DBSPOperator leftLimiter = this.bound.get(expanded.left());
        DBSPOperator rightLimiter = this.bound.get(expanded.right());
        if (leftLimiter == null && rightLimiter == null) {
            this.nonMonotone(expanded);
            return;
        }

        PartiallyMonotoneTuple out = Monotonicity.getBodyType(monotoneValue).to(PartiallyMonotoneTuple.class);
        DBSPType outputType = out.getProjectedType();
        assert outputType != null;
        DBSPOperator merger;
        PartiallyMonotoneTuple leftMono = null;
        MonotoneExpression lm = this.expansionMonotoneValues.get(expanded.left());
        if (lm != null) {
            leftMono = Monotonicity.getBodyType(lm).to(PartiallyMonotoneTuple.class);
        }
        PartiallyMonotoneTuple rightMono = null;
        MonotoneExpression rm = this.expansionMonotoneValues.get(expanded.right());
        if (rm != null) {
            rightMono = Monotonicity.getBodyType(rm).to(PartiallyMonotoneTuple.class);
        }

        if (leftLimiter != null && rightLimiter != null) {
            // (kl, l), (kr, r) -> (union(kl, kr), l, r)
            assert leftMono != null;
            assert rightMono != null;
            DBSPVariablePath l = new DBSPVariablePath(leftLimiter.outputType.ref());
            DBSPVariablePath r = new DBSPVariablePath(rightLimiter.outputType.ref());
            DBSPExpression[] fields = new DBSPExpression[3];
            int leftIndex = 0;
            int rightIndex = 0;

            if (leftMono.getField(0).mayBeMonotone()) {
                leftIndex++;
                if (rightMono.getField(0).mayBeMonotone()) {
                    rightIndex++;
                    fields[0] = max(
                            l.deref().field(0),
                            r.deref().field(0),
                            leftMono.getField(0),
                            rightMono.getField(0));
                } else {
                    fields[0] = l.deref().field(0);
                }
            } else {
                if (rightMono.getField(0).mayBeMonotone()) {
                    rightIndex++;
                    fields[0] = r.deref().field(0);
                } else {
                    fields[0] = new DBSPTupleExpression();
                }
            }

            if (leftMono.getField(1).mayBeMonotone())
                fields[1] = l.deref().field(leftIndex);
            else
                fields[1] = new DBSPTupleExpression();
            if (rightMono.getField(1).mayBeMonotone())
                fields[2] = r.deref().field(rightIndex);
            else
                fields[2] = new DBSPTupleExpression();

            DBSPClosureExpression closure =
                    new DBSPRawTupleExpression(fields)
                            .closure(l.asParameter(), r.asParameter());
            merger = new DBSPApply2Operator(expanded.getNode(), closure,
                    closure.getResultType(), leftLimiter, rightLimiter);
        } else if (leftLimiter != null) {
            // (k, l) -> (k, l, Tup0<>)
            assert leftMono != null;
            DBSPVariablePath var = new DBSPVariablePath(leftLimiter.outputType.ref());
            DBSPExpression k = new DBSPTupleExpression();
            int currentField = 0;
            if (leftMono.getField(0).mayBeMonotone()) {
                k = var.deref().field(currentField++);
            }
            DBSPExpression l = new DBSPTupleExpression();
            if (leftMono.getField(1).mayBeMonotone())
                l = var.deref().field(currentField);
            DBSPClosureExpression closure =
                    new DBSPRawTupleExpression(
                            k,
                            l,
                            new DBSPTupleExpression())
                            .closure(var.asParameter());
            merger = new DBSPApplyOperator(
                    expanded.getNode(), closure, closure.getResultType(), leftLimiter, comment);
        } else {
            // (k, r) -> (k, Tup0<>, r)
            assert rightMono != null;
            DBSPVariablePath var = new DBSPVariablePath(rightLimiter.outputType.ref());
            DBSPExpression k = new DBSPTupleExpression();
            DBSPExpression r = new DBSPTupleExpression();
            int currentField = 0;
            if (rightMono.getField(0).mayBeMonotone()) {
                k = var.deref().field(currentField++);
            }
            if (rightMono.getField(1).mayBeMonotone())
                r = var.deref().field(currentField);
            DBSPClosureExpression closure =
                    new DBSPRawTupleExpression(
                            k,
                            new DBSPTupleExpression(),
                            r)
                            .closure(var.asParameter());
            merger = new DBSPApplyOperator(
                    expanded.getNode(), closure, closure.getResultType(), rightLimiter, comment);
        }

        this.addOperator(merger);
        DBSPClosureExpression clo = monotoneValue.getReducedExpression().to(DBSPClosureExpression.class);
        DBSPOperator limiter = new DBSPApplyOperator(expanded.getNode(), clo,
                    outputType, merger, comment);
        this.addOperator(limiter);
        this.markBound(expanded, limiter);
    }

    /** Generates a closure that computes the max of two tuple timestamps fieldwise */
    DBSPClosureExpression timestampMax(CalciteObject node, DBSPTypeTupleBase type) {
        // Generate the max function for the timestamp tuple
        DBSPVariablePath left = type.ref().var();
        DBSPVariablePath right = type.ref().var();
        List<DBSPExpression> maxes = new ArrayList<>();
        for (int i = 0; i < type.size(); i++) {
            DBSPType ftype = type.tupFields[i];
            maxes.add(ExpressionCompiler.makeBinaryExpression(node, ftype, DBSPOpcode.MAX,
                    left.deref().field(i), right.deref().field(i)));
        }
        DBSPExpression max = new DBSPTupleExpression(maxes, false);
        return max.closure(left.asParameter(), right.asParameter());
    }

    /** Process LATENESS annotations.
     * @return Return the original operator if there aren't any annotations, or
     * the operator that produces the result of the input filtered otherwise. */
    DBSPOperator processLateness(DBSPOperator operator, DBSPOperator expansion) {
        MonotoneExpression expression = this.expansionMonotoneValues.get(expansion);
        if (expression == null) {
            this.nonMonotone(expansion);
            return operator;
        }
        List<DBSPExpression> timestamps = new ArrayList<>();
        List<DBSPExpression> minimums = new ArrayList<>();
        int index = 0;
        DBSPVariablePath t = operator.getOutputZSetType().elementType.ref().var();
        for (IHasLateness column: operator.to(IHasColumnsMetadata.class).getLateness()) {
            DBSPExpression lateness = column.getLateness();
            if (lateness != null) {
                DBSPExpression field = t.deref().field(index);
                DBSPType type = field.getType();
                field = ExpressionCompiler.makeBinaryExpression(operator.getNode(), field.getType(),
                        DBSPOpcode.SUB, field, lateness);
                timestamps.add(field);
                DBSPExpression min = type.to(IsBoundedType.class).getMinValue();
                minimums.add(min);
            }
            index++;
        }
        if (minimums.isEmpty()) {
            this.nonMonotone(expansion);
            return operator;
        }

        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPOperator replacement = operator.withInputs(sources, this.force);
        replacement.setDerivedFrom(operator.id);
        this.addOperator(replacement);

        // The waterline operator will compute the *minimum legal value* of all the
        // inputs that have a lateness attached.  The output signature contains only
        // the columns that have lateness.
        DBSPTupleExpression min = new DBSPTupleExpression(minimums, false);
        DBSPTupleExpression timestamp = new DBSPTupleExpression(timestamps, false);
        DBSPClosureExpression max = this.timestampMax(operator.getNode(), min.getTupleType());

        DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(
                operator.getNode(), min.closure(),
                // second parameter unused for timestamp
                timestamp.closure(t.asParameter(), new DBSPTypeRawTuple().ref().var().asParameter()),
                max, replacement);
        this.addOperator(waterline);
        this.markBound(replacement, waterline);
        if (operator != replacement)
            this.markBound(operator, waterline);
        if (operator != expansion)
            this.markBound(expansion, waterline);

        // Waterline fed through a delay
        DBSPDelayOperator delay = new DBSPDelayOperator(operator.getNode(), min, waterline);
        this.addOperator(delay);
        this.markBound(delay, waterline);
        return DBSPControlledFilterOperator.create(
                operator.getNode(), replacement, Monotonicity.getBodyType(expression), delay);
    }

    DBSPExpression wrapTypedBox(DBSPExpression expression, boolean typed) {
        DBSPType type = new DBSPTypeTypedBox(expression.getType(), typed);
        return new DBSPUnaryExpression(expression.getNode(), type, DBSPOpcode.TYPEDBOX, expression);
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
        ReplacementExpansion replacementExpansion = Objects.requireNonNull(this.getReplacement(operator));
        DBSPOperator expansion = this.processLateness(operator, replacementExpansion.replacement);

        // Process watermark annotations.  Very similar to lateness annotations.
        int index = 0;
        DBSPType dataType = operator.getOutputZSetType().elementType;
        DBSPVariablePath t = dataType.ref().var();
        List<DBSPExpression> fields = new ArrayList<>();
        List<DBSPExpression> timestamps = new ArrayList<>();
        List<DBSPExpression> minimums = new ArrayList<>();
        for (IHasWatermark column: operator.to(IHasColumnsMetadata.class).getWatermarks()) {
            DBSPExpression lateness = column.getWatermark();

            if (lateness != null) {
                DBSPExpression field = t.deref().field(index);
                fields.add(field);
                DBSPType type = field.getType();
                field = ExpressionCompiler.makeBinaryExpression(operator.getNode(), field.getType(),
                        DBSPOpcode.SUB, field.deepCopy(), lateness);
                timestamps.add(field);
                DBSPExpression min = type.to(IsBoundedType.class).getMinValue();
                minimums.add(min);
            }
            index++;
        }

        // Currently we only support at most 1 watermark column per table.
        // TODO: support multiple fields.
        if (minimums.size() > 1) {
            throw new UnimplementedException("More than 1 watermark per table not yet supported", operator.getNode());
        }

        if (!minimums.isEmpty()) {
            assert fields.size() == 1;
            this.addOperator(expansion);

            DBSPTupleExpression min = new DBSPTupleExpression(minimums, false);
            DBSPTupleExpression timestamp = new DBSPTupleExpression(timestamps, false);
            DBSPParameter parameter = t.asParameter();
            DBSPClosureExpression max = this.timestampMax(operator.getNode(), min.getTupleType());
            DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(
                    operator.getNode(), min.closure(),
                    // Second parameter unused for timestamp
                    timestamp.closure(parameter, new DBSPTypeRawTuple().ref().var().asParameter()),
                    max, operator);
            this.addOperator(waterline);

            DBSPVariablePath var = timestamp.getType().ref().var();
            DBSPExpression makePair = new DBSPRawTupleExpression(
                    this.wrapTypedBox(minimums.get(0), false),
                    this.wrapTypedBox(var.deref().field(0), false));
            DBSPApplyOperator apply = new DBSPApplyOperator(
                    operator.getNode(), makePair.closure(var.asParameter()), makePair.getType(), waterline,
                    "(" + operator.getDerivedFrom() + ")");
            this.addOperator(apply);

            // Window requires data to be indexed
            DBSPOperator ix = new DBSPMapIndexOperator(operator.getNode(),
                    new DBSPRawTupleExpression(fields.get(0), t.deref()).closure(t.asParameter()),
                    new DBSPTypeIndexedZSet(operator.getNode(),
                            fields.get(0).getType(), dataType), true, expansion);
            this.addOperator(ix);
            DBSPWindowOperator window = new DBSPWindowOperator(operator.getNode(), ix, apply);
            this.addOperator(window);
            expansion = new DBSPDeindexOperator(operator.getNode(), window);
        }

        if (expansion == operator) {
            this.replace(operator);
        } else {
            this.map(operator, expansion);
        }
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        // Treat like an identity function
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null) {
            DBSPOperator bound = this.processSum(expanded.replacement.to(DBSPSumOperator.class));
            if (bound != null && expanded.replacement != operator)
                this.markBound(operator, bound);
        } else {
            this.nonMonotone(operator);
        }
        super.postorder(operator);
    }

    /** Compute an expression which projects a source expression into a subset of
     * its fields.  For example, the source may be v: (i32), while the sourceProjection
     * is (NonMonotone(i64), Monotone(i32)).  The destination may be (Monotone(i32)).
     * In this case the result will be just (v.0).
     * @param source                  Expression to project.
     * @param sourceProjection        Describes the type of source.
     *                                Source is the projection of some other expression.
     * @param destinationProjection   Projection desired for result.
     *                                The fields of destinationProjection are always
     *                                a subset of the fields in the sourceProjection.
     */
    DBSPExpression project(DBSPExpression source,
                           IMaybeMonotoneType sourceProjection,
                           IMaybeMonotoneType destinationProjection) {
        if (destinationProjection.is(ScalarMonotoneType.class)) {
            assert sourceProjection.is(ScalarMonotoneType.class);
            return source;
        } else if (destinationProjection.is(PartiallyMonotoneTuple.class)) {
            assert sourceProjection.is(PartiallyMonotoneTuple.class);
            PartiallyMonotoneTuple src = sourceProjection.to(PartiallyMonotoneTuple.class);
            PartiallyMonotoneTuple dest = destinationProjection.to(PartiallyMonotoneTuple.class);
            assert src.size() == dest.size();
            List<DBSPExpression> fields = new ArrayList<>();
            int currentIndex = 0;
            for (int i = 0; i < dest.size(); i++) {
                if (dest.getField(i).mayBeMonotone()) {
                    fields.add(source.field(currentIndex));
                }
                if (src.getField(i).mayBeMonotone()) {
                    currentIndex++;
                }
            }
            return new DBSPTupleExpression(source.getNode(), fields);
        }
        throw new UnimplementedException(source.getNode());
    }

    /** Apply MIN pointwise to two expressions */
    DBSPExpression min(DBSPExpression left,
                       DBSPExpression right) {
        assert left.getType().sameType(right.getType());
        if (left.getType().is(DBSPTypeBaseType.class)) {
            return ExpressionCompiler.makeBinaryExpression(
                    left.getNode(), left.getType(), DBSPOpcode.MIN, left, right);
        } else if (left.getType().is(DBSPTypeTupleBase.class)) {
            DBSPTypeTupleBase lt = left.getType().to(DBSPTypeTupleBase.class);
            DBSPExpression[] mins = new DBSPExpression[lt.size()];
            for (int i = 0; i < lt.size(); i++) {
                mins[i] = min(left.field(i), right.field(i));
            }
            return lt.makeTuple(mins);
        }
        throw new UnimplementedException(left.getNode());
    }

    /** Create and insert an operator which projects the output of limit.
     * @param node         Original program node.
     * @param limit        The output of this operator is being projected.
     * @param source       Monotonicity information about the output of limit.
     * @param destination  Monotonicity information for the desired output.
     * @param comment      Comment to insert in new operator.
     * @return             The operator performing the projection.
     *                     The operator is inserted in the graph.
     */
    DBSPApplyOperator project(
            CalciteObject node,
            DBSPOperator limit, IMaybeMonotoneType source,
            IMaybeMonotoneType destination, String comment) {
        DBSPVariablePath var = limit.outputType.ref().var();
        DBSPExpression proj = this.project(var.deref(), source, destination);
        DBSPApplyOperator result = new DBSPApplyOperator(node,
                proj.closure(var.asParameter()), Objects.requireNonNull(destination.getProjectedType()),
                limit, comment);
        this.addOperator(result);
        return result;
    }

    @Nullable
    DBSPOperator processSum(DBSPSumOperator expanded) {
        String comment = "(" + expanded.getDerivedFrom() + ")";
        MonotoneExpression monotoneValue = this.expansionMonotoneValues.get(expanded);
        if (monotoneValue == null || !monotoneValue.mayBeMonotone()) {
            this.nonMonotone(expanded);
            return null;
        }

        // Create a projection for each input
        // Collect input limits
        List<DBSPOperator> limiters = new ArrayList<>();
        List<IMaybeMonotoneType> mono = new ArrayList<>();
        for (DBSPOperator input: expanded.inputs) {
            DBSPOperator limiter = this.bound.get(input);
            if (limiter == null) {
                this.nonMonotone(expanded);
                return null;
            }
            limiters.add(limiter);
            MonotoneExpression me = this.expansionMonotoneValues.get(input);
            mono.add(Objects.requireNonNull(Monotonicity.getBodyType(Objects.requireNonNull(me))));
        }

        IMaybeMonotoneType out = Monotonicity.getBodyType(monotoneValue);
        DBSPType outputType = out.getProjectedType();
        assert outputType != null;

        // Same function everywhere
        DBSPVariablePath l = new DBSPVariablePath(outputType.ref());
        DBSPVariablePath r = new DBSPVariablePath(outputType.ref());
        DBSPClosureExpression min = this.min(l.deref(), r.deref())
                .closure(l.asParameter(), r.asParameter());

        // expand into a binary unbalanced tree
        DBSPOperator current = this.project(expanded.getNode(), limiters.get(0), mono.get(0), out, comment);
        for (int i = 1; i < expanded.inputs.size(); i++) {
            DBSPOperator next = this.project(expanded.getNode(), limiters.get(i), mono.get(i), out, comment);
            current = new DBSPApply2Operator(
                    expanded.getNode(),
                    min,
                    outputType,
                    current, next);
            this.addOperator(current);
        }

        this.markBound(expanded, current);
        return current;
    }

    @Override
    public void postorder(DBSPViewOperator operator) {
        if (operator.hasLateness()) {
            ReplacementExpansion expanded = this.getReplacement(operator);
            // Treat like a source operator
            DBSPOperator replacement = this.processLateness(
                    operator, Objects.requireNonNull(expanded).replacement);
            if (replacement == operator) {
                super.postorder(operator);
            } else {
                this.map(operator, replacement);
            }
            return;
        }
        // Treat like an identity function
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null) {
            DBSPApplyOperator bound = this.addBounds(expanded.replacement, 0);
            if (bound != null && operator != expanded.replacement)
                this.markBound(operator, bound);
        } else {
            this.nonMonotone(operator);
        }
        super.postorder(operator);
    }
}
