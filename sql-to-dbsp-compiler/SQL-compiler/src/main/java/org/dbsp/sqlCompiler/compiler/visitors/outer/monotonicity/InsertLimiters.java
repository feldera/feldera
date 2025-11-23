package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApply2Operator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPControlledKeyFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPInputMapWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.IColumnMetadata;
import org.dbsp.sqlCompiler.compiler.IHasColumnsMetadata;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneClosureType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.ScalarMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.IndexedInputs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.AggregateExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.CommonJoinExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.DistinctExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.JoinExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.JoinFilterMapExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.JoinIndexExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.LeftJoinExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.OperatorExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.ReplacementExpansion;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.circuit.annotation.AlwaysMonotone;
import org.dbsp.sqlCompiler.circuit.annotation.NoIntegrator;
import org.dbsp.sqlCompiler.circuit.annotation.Waterline;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.IsBoundedType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeTypedBox;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWeight;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.NullableFunction;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** As a result of the Monotonicity analysis, this pass inserts new operators:
 * - apply operators that compute the bounds that drive the controlled filters
 * - {@link DBSPControlledKeyFilterOperator} operators to throw away tuples that are not "useful"
 * - {@link DBSPWaterlineOperator} operators near sources with lateness information
 * - {@link DBSPIntegrateTraceRetainKeysOperator} to prune data from integral operators
 * - {@link DBSPPartitionedRollingAggregateWithWaterlineOperator} operators
 * - {@link DBSPIntegrateTraceRetainValuesOperator} to prune data from integral operators
 * This also inserts WINDOWS before views that have "emit_final" annotations.
 * It also converts {@link DBSPSourceMultisetOperator} into {@link DBSPInputMapWithWaterlineOperator}
 * when appropriate.
 *
 * <P>This visitor is tricky because it operates on a circuit, but takes the information
 * required to rewrite the graph from a different circuit, the expandedCircuit and
 * the expandedInto map.  Moreover, the current circuit is being rewritten by the
 * visitor while it is being processed.
 *
 * <p>Each apply operator has the signature (bool, arg) -> (bool, result), where the
 * boolean field indicates whether this is the first step.  In the first step the
 * apply operators do not compute, since the inputs may cause exceptions.
 **/
public class InsertLimiters extends CircuitCloneVisitor {
    /** For each operator in the expansion of the operators of this circuit
     * the list of its monotone output columns */
    public final Monotonicity.MonotonicityInformation expansionMonotoneValues;
    /** Circuit that contains the expansion of the circuit we are modifying */
    public final DBSPCircuit expandedCircuit;
    /** Maps each original operator to the set of operators it was expanded to */
    public final Map<DBSPSimpleOperator, OperatorExpansion> expandedInto;
    /** Maps each operator to the one that computes its lower bound.
     * The keys in this map can be both operators from the previous version of this
     * circuit and from the expanded circuit. */
    public final Map<OutputPort, OutputPort> bound;
    /** Information about joins */
    final NullableFunction<DBSPBinaryOperator, KeyPropagation.JoinDescription> joinInformation;
    // Debugging aid, normally 'true'
    static final boolean INSERT_RETAIN_VALUES = true;
    // Debugging aid, normally 'true'
    static final boolean INSERT_RETAIN_KEYS = true;
    final List<OutputPort> errorStreams;
    /** These operators use the error view as a source */
    final Set<DBSPOperator> reachableFromError;

    public InsertLimiters(DBSPCompiler compiler,
                          DBSPCircuit expandedCircuit,
                          Monotonicity.MonotonicityInformation expansionMonotoneValues,
                          Map<DBSPSimpleOperator, OperatorExpansion> expandedInto,
                          NullableFunction<DBSPBinaryOperator, KeyPropagation.JoinDescription> joinInformation,
                          Set<DBSPOperator> reachableFromError) {
        super(compiler, false);
        this.expandedCircuit = expandedCircuit;
        this.expansionMonotoneValues = expansionMonotoneValues;
        this.expandedInto = expandedInto;
        this.joinInformation = joinInformation;
        this.bound = new HashMap<>();
        this.errorStreams = new ArrayList<>();
        this.reachableFromError = reachableFromError;
    }

    void markBound(OutputPort operator, OutputPort bound) {
        Utilities.enforce(this.circuit != null);
        Utilities.enforce(this.circuit.contains(operator.node()) ||
                this.expandedCircuit.contains(operator.node()));
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Bound for ")
                .appendSupplier(operator::toString)
                .append(" computed by ")
                .appendSupplier(bound::toString)
                .newline();
        Utilities.putNew(this.bound, operator, bound);
    }

    /** Given a function for an apply operator, synthesizes an operator that performs
     * the following computation:
     * <p>
     * |param| (
     *    param.0,
     *    if param.0 {
     *       function(param.1)
     *    } else {
     *       min
     *    })
     * where 'min' is the minimum constant value with the appropriate type.
     * Inserts the operator in the circuit.  'param.0' is true when param.1 is
     * not the minimum legal value - i.e., the waterline has seen some data.
     *
     * @param source   Input operator.
     * @param represented  Operator whose waterline is represented.
     * @param function Function to apply to the data.
     */
    OutputPort createApply(OutputPort source, @Nullable DBSPSimpleOperator represented,
                           DBSPClosureExpression function) {
        if (represented != null) {
            Utilities.enforce(this.circuit != null);
            Utilities.enforce(this.circuit.contains(represented));
        }
        DBSPVariablePath var = source.outputType().ref().var();
        DBSPExpression v0 = var.deref().field(0);
        DBSPExpression v1 = var.deref().field(1);
        DBSPExpression min = function.getResultType().minimumValue();
        DBSPExpression call = function.call(v1.borrow()).reduce(this.compiler);
        DBSPExpression cond = new DBSPTupleExpression(v0,
                new DBSPIfExpression(source.node().getNode(), v0, call, min));
        DBSPApplyOperator result = new DBSPApplyOperator(source.node().getRelNode(), cond.closure(var),
                source.simpleNode().outputPort(), "(" + source.node().getDerivedFrom() + ")");
        this.addOperator(result);
        result.addAnnotation(Waterline.INSTANCE, DBSPApplyOperator.class);
        return result.outputPort();
    }

    /** Given a function for an apply2 operator, synthesizes an operator that performs
     * the following computation:
     * <p>
     * |param0, param1|
     *    (param0.0 && param1.0,
     *     if param0.0 && param1.0 {
     *       function(param0.1, param1.1)
     *     } else {
     *       min
     *     })
     * where 'min' is the minimum constant value with the appropriate type.
     * Inserts the operator in the circuit.  'param0.0' is true when param0.1 is
     * not the minimum legal value - i.e., the waterline has seen some data
     * (same on the other side).
     *
     * @param left     Left input operator.
     * @param right    Right input operator
     * @param function Function to apply to the data.
     */
    OutputPort createApply2(OutputPort left, OutputPort right, DBSPClosureExpression function) {
        DBSPVariablePath leftVar = left.outputType().ref().var();
        DBSPVariablePath rightVar = right.outputType().ref().var();
        DBSPExpression l0 = leftVar.deref().field(0);
        DBSPExpression r0 = rightVar.deref().field(0);
        DBSPExpression l1 = leftVar.deref().field(1);
        DBSPExpression r1 = rightVar.deref().field(1);
        DBSPExpression and = ExpressionCompiler.makeBinaryExpression(left.node().getNode(),
                l0.getType(), DBSPOpcode.AND, l0, r0);
        DBSPExpression min = function.getResultType().minimumValue();
        DBSPExpression cond = new DBSPTupleExpression(and,
                new DBSPIfExpression(left.node().getNode(), and,
                        function.call(l1.borrow(), r1.borrow()), min).reduce(this.compiler));
        DBSPApply2Operator result = new DBSPApply2Operator(
                left.node().getRelNode(), cond.closure(leftVar, rightVar), left, right);
        result.addAnnotation(Waterline.INSTANCE, DBSPApply2Operator.class);
        this.addOperator(result);
        return result.outputPort();
    }

    /**
     * Add an operator which computes the waterline for the output of an operator.
     *
     * @param represented           Operator that was expanded, in case we want to assign the bounds to it.
     * @param operatorFromExpansion Operator produced as the expansion of represented.
     * @param input                 Input of the operatorFromExpansion which is used.
     * */
    @SuppressWarnings("SameParameterValue")
    @Nullable
    OutputPort addBounds(@Nullable DBSPSimpleOperator represented,
                         @Nullable DBSPOperator operatorFromExpansion, int input) {
        Utilities.enforce(this.circuit != null);
        if (operatorFromExpansion == null)
            return null;
        Utilities.enforce(!this.circuit.contains(operatorFromExpansion));
        MonotoneExpression monotone = this.expansionMonotoneValues.get(
                operatorFromExpansion.to(DBSPSimpleOperator.class));
        if (monotone == null)
            return null;
        OutputPort source = operatorFromExpansion.inputs.get(input);  // Even for binary operators
        OutputPort boundSource = Utilities.getExists(this.bound, source);
        DBSPClosureExpression function = monotone.getReducedExpression().to(DBSPClosureExpression.class);

        OutputPort bound = this.createApply(boundSource, represented, function);
        this.markBound(operatorFromExpansion.to(DBSPSimpleOperator.class).outputPort(), bound);
        if (represented != null) {
            Utilities.enforce(this.circuit.contains(represented));
            this.markBound(represented.outputPort(), bound);
        }
        return bound;
    }

    void nonMonotone(DBSPSimpleOperator operator) {
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Not monotone: ")
                .appendSupplier(operator::toString)
                .newline();
    }

    @Nullable
    ReplacementExpansion getReplacement(DBSPSimpleOperator operator) {
        OperatorExpansion expanded = this.expandedInto.get(operator);
        if (expanded == null)
            return null;
        return expanded.to(ReplacementExpansion.class);
    }

    /** Add bounds for an operator that expands to itself */
    void addBoundsForNonExpandedOperator(DBSPSimpleOperator represented) {
        ReplacementExpansion expanded = this.getReplacement(represented);
        if (expanded == null)
            this.nonMonotone(represented);
        else
            this.addBounds(represented, expanded.replacement, 0);
    }


    @Override
    public void postorder(DBSPStreamAntiJoinOperator operator) {
        this.addBoundsForNonExpandedOperator(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPAntiJoinOperator operator) {
        this.addBoundsForNonExpandedOperator(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPHopOperator operator) {
        this.addBoundsForNonExpandedOperator(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) {
        this.addBoundsForNonExpandedOperator(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        this.addBoundsForNonExpandedOperator(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPNoopOperator operator) {
        this.addBoundsForNonExpandedOperator(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPNegateOperator operator) {
        this.addBoundsForNonExpandedOperator(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPFlatMapOperator operator) {
        this.addBoundsForNonExpandedOperator(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPWindowOperator operator) {
        // Treat as an identity function for the left input
        this.addBoundsForNonExpandedOperator(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null) {
            OutputPort bound = this.processFilter(expanded.replacement.to(DBSPFilterOperator.class));
            if (operator != expanded.replacement && bound != null) {
                this.markBound(operator.outputPort(), bound);
            }
        } else {
            this.nonMonotone(operator);
        }
        super.postorder(operator);
    }

    @Nullable
    OutputPort processFilter(@Nullable DBSPFilterOperator expansion) {
        if (expansion == null)
            return null;
        return this.addBounds(null, expansion, 0);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        this.addBoundsForNonExpandedOperator(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPChainAggregateOperator aggregator) {
        OutputPort source = this.mapped(aggregator.input());
        OperatorExpansion expanded = this.expandedInto.get(aggregator);
        if (expanded == null) {
            this.nonMonotone(aggregator);
            super.postorder(aggregator);
            return;
        }

        ReplacementExpansion ae = expanded.to(ReplacementExpansion.class);
        if (aggregator.annotations.first(AlwaysMonotone.class) != null) {
            // This operator computes its own waterline.
            // This treatment is similar to the ImplementNow.scalarNow function.

            DBSPSimpleOperator filteredAggregator = aggregator
                    .withInputs(Linq.list(source), false)
                    .to(DBSPSimpleOperator.class);
            this.map(aggregator.outputPort(), filteredAggregator.outputPort());
            DBSPDeindexOperator deindex = new DBSPDeindexOperator(aggregator.getRelNode(), aggregator.getFunctionNode(),
                    filteredAggregator.outputPort());
            this.addOperator(deindex);
            DBSPTypeTuple tuple = deindex.getOutputZSetElementType().to(DBSPTypeTuple.class);
            DBSPVariablePath t = tuple.ref().var();

            DBSPTupleExpression min = new DBSPTupleExpression(
                    Linq.map(tuple.tupFields, type -> type.to(IsBoundedType.class).getMinValue(), DBSPExpression.class));
            DBSPExpression timestampTuple = ExpressionCompiler.expandTuple(aggregator.getNode(), t.deref());
            DBSPClosureExpression max = InsertLimiters.timestampMax(aggregator.getNode(), min.getTypeAsTupleBase());
            DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(
                    aggregator.getRelNode(), min.closure(),
                    timestampTuple.closure(t, DBSPTypeRawTuple.EMPTY.ref().var()),
                    max, deindex.outputPort());
            this.addOperator(waterline);

            // An apply operator to add a Boolean bit to the waterline.
            // This bit is 'true' when the waterline produces a value
            // that is not 'minimum'.
            DBSPVariablePath var = t.getType().var();
            DBSPExpression eq = eq(min, var.deref());
            DBSPSimpleOperator extend = new DBSPApplyOperator(aggregator.getRelNode(),
                    new DBSPTupleExpression(
                            eq.not(),
                            new DBSPRawTupleExpression(
                                    new DBSPTupleExpression(),
                                    var.deref().applyClone())).closure(var),
                    waterline.outputPort(), null);
            extend.addAnnotation(Waterline.INSTANCE, DBSPSimpleOperator.class);
            this.addOperator(extend);

            this.markBound(aggregator.outputPort(), extend.outputPort());
            this.markBound(ae.replacement.outputPort(), extend.outputPort());
            return;
        }

        OutputPort limiter = this.bound.get(aggregator.input());
        if (limiter == null) {
            super.postorder(aggregator);
            this.nonMonotone(aggregator);
            return;
        }

        DBSPSimpleOperator filteredAggregator = aggregator
                .withInputs(Linq.list(source), false)
                .to(DBSPSimpleOperator.class);
        OutputPort limiter2 = this.addBounds(aggregator, ae.replacement, 0);
        if (limiter2 == null) {
            this.map(aggregator, filteredAggregator);
            return;
        }

        MonotoneExpression monotoneValue2 = this.expansionMonotoneValues.get(ae.replacement);
        if (monotoneValue2 == null) {
            this.map(aggregator, filteredAggregator);
            return;
        }
        IMaybeMonotoneType projection2 = Monotonicity.getBodyType(Objects.requireNonNull(monotoneValue2));
        this.addOperator(filteredAggregator);
        this.createRetainKeys(aggregator.getRelNode(), filteredAggregator.outputPort(), projection2, limiter2);
        this.map(aggregator.outputPort(), filteredAggregator.outputPort(), false);
    }

    void createRetainKeys(CalciteRelNode node, OutputPort data,
                          IMaybeMonotoneType dataProjection, OutputPort control) {
        if (!INSERT_RETAIN_KEYS)
            return;
        DBSPSimpleOperator itrk = DBSPIntegrateTraceRetainKeysOperator.create(
                node, data, dataProjection, this.createDelay(control));
        if (itrk != null) {
            this.addOperator(itrk);
        }
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessOperator aggregator) {
        OutputPort source = this.mapped(aggregator.input());
        OperatorExpansion expanded = this.expandedInto.get(aggregator);
        if (expanded == null) {
            this.nonMonotone(aggregator);
            super.postorder(aggregator);
            return;
        }

        ReplacementExpansion ae = expanded.to(ReplacementExpansion.class);
        OutputPort limiter = this.bound.get(aggregator.input());
        if (limiter == null) {
            super.postorder(aggregator);
            this.nonMonotone(aggregator);
            return;
        }

        DBSPSimpleOperator filteredAggregator = aggregator
                .withInputs(Linq.list(source), false)
                .to(DBSPSimpleOperator.class);
        OutputPort limiter2 = this.addBounds(aggregator, ae.replacement, 0);
        if (limiter2 == null) {
            this.map(aggregator, filteredAggregator);
            return;
        }

        this.addOperator(filteredAggregator);
        MonotoneExpression monotoneValue2 = this.expansionMonotoneValues.get(ae.replacement);
        IMaybeMonotoneType projection2 = Monotonicity.getBodyType(Objects.requireNonNull(monotoneValue2));
        this.createRetainKeys(aggregator.getRelNode(), filteredAggregator.outputPort(), projection2, limiter2);
        this.map(aggregator, filteredAggregator, false);
    }

    OutputPort createDelay(OutputPort source) {
        DBSPExpression initial = source.outputType().minimumValue();
        DBSPDelayOperator result = new DBSPDelayOperator(source.node().getRelNode(), initial, source);
        this.addOperator(result);
        return result.outputPort();
    }

    @Override
    public void postorder(DBSPAggregateOperator aggregator) {
        OutputPort source = this.mapped(aggregator.input());
        OperatorExpansion expanded = this.expandedInto.get(aggregator);
        if (expanded == null) {
            this.nonMonotone(aggregator);
            super.postorder(aggregator);
            return;
        }

        AggregateExpansion ae = expanded.to(AggregateExpansion.class);
        OutputPort limiter = this.bound.get(aggregator.input());
        if (limiter == null) {
            super.postorder(aggregator);
            this.nonMonotone(aggregator);
            return;
        }

        DBSPSimpleOperator filteredAggregator = aggregator
                .withInputs(Linq.list(source), false)
                .to(DBSPSimpleOperator.class);
        // We use the input 0; input 1 comes from the integrator
        OutputPort limiter2 = this.addBounds(null, ae.aggregator, 0);
        if (limiter2 == null) {
            this.map(aggregator, filteredAggregator);
            return;
        }

        this.addOperator(filteredAggregator);
        MonotoneExpression monotoneValue2 = this.expansionMonotoneValues.get(ae.aggregator);
        IMaybeMonotoneType projection2 = Monotonicity.getBodyType(Objects.requireNonNull(monotoneValue2));
        this.createRetainKeys(aggregator.getRelNode(), source, projection2, limiter2);
        this.createRetainKeys(aggregator.getRelNode(), filteredAggregator.outputPort(), projection2, limiter2);

        this.addBounds(aggregator, ae.upsert, 0);
        this.map(aggregator, filteredAggregator, false);
    }

    @Override
    public void postorder(DBSPLagOperator aggregator) {
        OutputPort source = this.mapped(aggregator.input());
        OperatorExpansion expanded = this.expandedInto.get(aggregator);
        if (expanded == null) {
            this.nonMonotone(aggregator);
            super.postorder(aggregator);
            return;
        }

        OutputPort limiter = this.bound.get(aggregator.input());
        if (limiter == null) {
            super.postorder(aggregator);
            this.nonMonotone(aggregator);
            return;
        }

        ReplacementExpansion ae = expanded.to(ReplacementExpansion.class);
        OutputPort expandedSource = ae.replacement.inputs.get(0);
        MonotoneExpression inputValue = this.expansionMonotoneValues.get(expandedSource);
        if (inputValue == null) {
            super.postorder(aggregator);
            this.nonMonotone(aggregator);
            return;
        }

        IMaybeMonotoneType projection = Monotonicity.getBodyType(inputValue);
        MonotoneExpression monotoneValue2 = this.expansionMonotoneValues.get(ae.replacement);
        if (monotoneValue2 == null) {
            super.postorder(aggregator);
            this.nonMonotone(aggregator);
            return;
        }

        this.createRetainKeys(aggregator.getRelNode(), source, projection, limiter);
        IMaybeMonotoneType projection2 = Monotonicity.getBodyType(Objects.requireNonNull(monotoneValue2));
        OutputPort aggLimiter = this.addBounds(aggregator, ae.replacement, 0);
        if (aggLimiter == null) {
            super.postorder(aggregator);
            this.nonMonotone(aggregator);
            return;
        }

        DBSPSimpleOperator filteredAggregator = aggregator
                .withInputs(Linq.list(source), false)
                .to(DBSPSimpleOperator.class);
        this.addOperator(filteredAggregator);
        this.createRetainKeys(aggregator.getRelNode(), filteredAggregator.outputPort(), projection2, aggLimiter);
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

        OutputPort source = expanded.replacement.inputs.get(0);
        MonotoneExpression inputValue = this.expansionMonotoneValues.get(source);
        if (inputValue == null) {
            super.postorder(operator);
            this.nonMonotone(operator);
            return;
        }

        OutputPort boundSource = this.bound.get(source);
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
        DBSPTypeTupleBase finalVarType = varType;
        Utilities.enforce(varType.size() == 2, () -> "Expected a pair, got " + finalVarType);
        varType = new DBSPTypeRawTuple(varType.tupFields[0].ref(), varType.tupFields[1].ref());
        final DBSPVariablePath var = varType.var();
        DBSPExpression body = var.field(0).deref();
        body = DBSPTypeTypedBox.wrapTypedBox(body, true);
        DBSPClosureExpression closure = body.closure(var);
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.compiler(), operator, MonotoneTransferFunctions.ArgumentKind.IndexedZSet, projection);
        MonotoneExpression monotone = analyzer.applyAnalysis(closure);
        Objects.requireNonNull(monotone);

        DBSPClosureExpression function = monotone.getReducedExpression().to(DBSPClosureExpression.class);
        OutputPort waterline = this.createApply(boundSource, null, function);
        Logger.INSTANCE.belowLevel(this, 2)
                .append("WATERLINE FUNCTION: ")
                .appendSupplier(function::toString)
                .newline();

        // The bound for the output is different from the waterline
        DBSPExpression body0;
        DBSPType keyType = operator.getOutputIndexedZSetType().keyType;
        if (keyType.is(DBSPTypeTupleBase.class) && keyType.to(DBSPTypeTupleBase.class).size() == 0) {
            // Special case when the key of the operator is an empty tuple.
            // The Tup0 tuple is always monotone, so we have to include it in the output of the bound.
            body0 = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(),
                    new DBSPTupleExpression(var.field(0).deref()));
        } else {
            body0 = new DBSPRawTupleExpression(new DBSPTupleExpression(var.field(0).deref()));
        }
        DBSPClosureExpression closure0 = body0.closure(var);
        analyzer = new MonotoneTransferFunctions(
                this.compiler(), operator, MonotoneTransferFunctions.ArgumentKind.IndexedZSet, projection);
        MonotoneExpression monotone0 = analyzer.applyAnalysis(closure0);
        Objects.requireNonNull(monotone0);

        DBSPClosureExpression function0 = monotone0.getReducedExpression().to(DBSPClosureExpression.class);
        Logger.INSTANCE.belowLevel(this, 2)
                .append("BOUND FUNCTION: ")
                .appendSupplier(function0::toString)
                .newline();

        OutputPort bound = this.createApply(boundSource, operator, function0);
        this.markBound(expanded.replacement.outputPort(), bound);

        // Drop the boolean flag from the waterline
        DBSPVariablePath tmp = waterline.outputType().ref().var();
        DBSPClosureExpression drop = tmp.deref().field(1).applyCloneIfNeeded().closure(tmp);
        // Must insert the delay on the waterline, if we try to insert the delay
        // on the output of the dropApply Rust will be upset, since Delays
        // cannot have types that include TypedBox.
        DBSPApplyOperator dropApply = new DBSPApplyOperator(
                operator.getRelNode(), drop, this.createDelay(waterline), null);
        this.addOperator(dropApply);

        DBSPPartitionedRollingAggregateWithWaterlineOperator replacement =
                new DBSPPartitionedRollingAggregateWithWaterlineOperator(operator.getRelNode(),
                        operator.partitioningFunction, operator.function, operator.aggregateList,
                        operator.lower, operator.upper, operator.getOutputIndexedZSetType(),
                        this.mapped(operator.input()), dropApply.outputPort());
        this.map(operator, replacement);
    }

    void addJoinAnnotations(DBSPBinaryOperator operator) {
        KeyPropagation.JoinDescription info = this.joinInformation.apply(operator);
        if (info != null)
            operator.addAnnotation(new NoIntegrator(info.leftIsKey(), !info.leftIsKey()), DBSPBinaryOperator.class);
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null)
            this.processJoin(expanded.replacement.to(DBSPStreamJoinIndexOperator.class));
        else
            this.nonMonotone(operator);
        this.addJoinAnnotations(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null)
            this.processJoin(expanded.replacement.to(DBSPStreamJoinOperator.class));
        else
            this.nonMonotone(operator);
        this.addJoinAnnotations(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        OutputPort source = this.mapped(operator.input());
        OperatorExpansion expanded = this.expandedInto.get(operator);
        if (expanded == null) {
            super.postorder(operator);
            this.nonMonotone(operator);
            return;
        }
        DistinctExpansion expansion = expanded.to(DistinctExpansion.class);
        OutputPort sourceLimiter = this.bound.get(operator.input());
        if (sourceLimiter == null) {
            super.postorder(operator);
            this.nonMonotone(operator);
            return;
        }

        DBSPSimpleOperator result = operator.withInputs(Linq.list(source), false)
                .to(DBSPSimpleOperator.class);
        MonotoneExpression sourceMonotone = this.expansionMonotoneValues.get(expansion.distinct.right());
        IMaybeMonotoneType projection = Monotonicity.getBodyType(Objects.requireNonNull(sourceMonotone));
        this.createRetainKeys(operator.getRelNode(), source, projection, sourceLimiter);
        // Same limiter as the source
        this.markBound(expansion.distinct.outputPort(), sourceLimiter);
        this.markBound(operator.outputPort(), sourceLimiter);
        this.map(operator, result, true);
    }

    @Nullable
    DBSPSimpleOperator gcJoin(DBSPJoinBaseOperator join, CommonJoinExpansion expansion) {
        OutputPort leftLimiter = this.bound.get(join.left());
        OutputPort rightLimiter = this.bound.get(join.right());
        if (leftLimiter == null && rightLimiter == null) {
            return null;
        }

        OutputPort left = this.mapped(join.left());
        OutputPort right = this.mapped(join.right());
        DBSPJoinBaseOperator result = join.withInputs(Linq.list(left, right), false)
                .to(DBSPJoinBaseOperator.class);
        if (leftLimiter != null && expansion.getLeftIntegrator() != null) {
            MonotoneExpression leftMonotone = this.expansionMonotoneValues.get(
                    expansion.getLeftIntegrator().input());
            // Yes, the limit of the left input is applied to the right one.
            IMaybeMonotoneType leftProjection = Monotonicity.getBodyType(Objects.requireNonNull(leftMonotone));
            // Check if the "key" field is monotone
            if (leftProjection.to(PartiallyMonotoneTuple.class).getField(0).mayBeMonotone()) {
                this.createRetainKeys(join.getRelNode(), right, leftProjection, leftLimiter);
            }
        }

        if (rightLimiter != null && expansion.getRightIntegrator() != null) {
            MonotoneExpression rightMonotone = this.expansionMonotoneValues.get(
                    expansion.getRightIntegrator().input());
            // Yes, the limit of the right input is applied to the left one.
            IMaybeMonotoneType rightProjection = Monotonicity.getBodyType(Objects.requireNonNull(rightMonotone));
            // Check if the "key" field is monotone
            if (rightProjection.to(PartiallyMonotoneTuple.class).getField(0).mayBeMonotone()) {
                this.createRetainKeys(join.getRelNode(), left, rightProjection, rightLimiter);
            }
        }
        return result;
    }

    @Override
    public void postorder(DBSPLeftJoinOperator join) {
        OperatorExpansion expanded = this.expandedInto.get(join);
        if (expanded == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

        this.addJoinAnnotations(join);
        LeftJoinExpansion expansion = expanded.to(LeftJoinExpansion.class);
        DBSPSimpleOperator result = this.gcJoin(join, expansion);
        if (result == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

        this.processIntegral(expansion.leftIntegrator);
        this.processIntegral(expansion.rightIntegrator);
        this.processJoin(expansion.leftDelta);
        this.processJoin(expansion.rightDelta);
        this.processJoin(expansion.join);
        this.addBounds(null, expansion.antiJoin, 0);
        this.addBounds(null, expansion.map, 0);
        OutputPort limiter = this.processSumOrDiff(expansion.sum);
        if (limiter != null)
            this.markBound(join.outputPort(), limiter);
        this.map(join, result, true);
    }

    @Override
    public void postorder(DBSPJoinOperator join) {
        OperatorExpansion expanded = this.expandedInto.get(join);
        if (expanded == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

        this.addJoinAnnotations(join);
        JoinExpansion expansion = expanded.to(JoinExpansion.class);
        DBSPSimpleOperator result = this.gcJoin(join, expansion);
        if (result == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

        this.processIntegral(expansion.leftIntegrator);
        this.processIntegral(expansion.rightIntegrator);
        this.processJoin(expansion.leftDelta);
        this.processJoin(expansion.rightDelta);
        this.processJoin(expansion.both);
        OutputPort limiter = this.processSumOrDiff(expansion.sum);
        if (limiter != null)
            this.markBound(join.outputPort(), limiter);
        this.map(join, result, true);
    }

    @Override
    public void postorder(DBSPJoinIndexOperator join) {
        OperatorExpansion expanded = this.expandedInto.get(join);
        if (expanded == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

        this.addJoinAnnotations(join);
        JoinIndexExpansion expansion = expanded.to(JoinIndexExpansion.class);
        DBSPSimpleOperator result = this.gcJoin(join, expansion);
        if (result == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

        this.processIntegral(expansion.leftIntegrator);
        this.processIntegral(expansion.rightIntegrator);
        this.processJoin(expansion.leftDelta);
        this.processJoin(expansion.rightDelta);
        this.processJoin(expansion.both);
        OutputPort limiter = this.processSumOrDiff(expansion.sum);
        if (limiter != null)
            this.markBound(join.outputPort(), limiter);
        this.map(join, result, true);
    }

    private void processIntegral(@Nullable DBSPDelayedIntegralOperator replacement) {
        if (replacement == null)
            return;
        if (replacement.hasAnnotation(a -> a.is(AlwaysMonotone.class))) {
            OutputPort limiter = this.bound.get(replacement.input());
            if (limiter != null) {
                this.markBound(replacement.outputPort(), limiter);
            } else {
                this.nonMonotone(replacement);
            }
        }
    }

    OutputPort extractTimestamp(PartiallyMonotoneTuple sourceType, int tsIndex, OutputPort source) {
        // First index to apply to the limiter
        int outerIndex = sourceType.getField(0).mayBeMonotone() ? 1 : 0;
        PartiallyMonotoneTuple sourceTypeValue = sourceType.getField(1).to(PartiallyMonotoneTuple.class);
        // Second index to apply to the limiter
        int innerIndex = 0;
        for (int i = 0; i < tsIndex; i++) {
            if (sourceTypeValue.getField(i).mayBeMonotone())
                innerIndex++;
        }

        DBSPVariablePath var = this.getLimiterDataOutputType(source).ref().var();
        DBSPClosureExpression tsFunction = var
                .deref()
                .field(outerIndex)
                .field(innerIndex)
                .closure(var);
        return this.createApply(source, null, tsFunction);
    }

    @Override
    public void postorder(DBSPAsofJoinOperator join) {
        OperatorExpansion expansion = this.expandedInto.get(join);
        if (expansion == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }
        ReplacementExpansion repl = expansion.to(ReplacementExpansion.class);
        DBSPAsofJoinOperator expanded = repl.replacement.to(DBSPAsofJoinOperator.class);
        this.processJoin(expanded);

        OutputPort leftLimiter = this.bound.get(join.left());
        OutputPort rightLimiter = this.bound.get(join.right());
        if (leftLimiter == null || rightLimiter == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

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

        if (leftMono == null || rightMono == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

        // Extract the value part from the key-value tuple
        IMaybeMonotoneType leftValue = leftMono.getField(1);
        IMaybeMonotoneType rightValue = rightMono.getField(1);
        if (!leftValue.mayBeMonotone() || !rightValue.mayBeMonotone()) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

        PartiallyMonotoneTuple leftValueTuple = leftValue.to(PartiallyMonotoneTuple.class);
        PartiallyMonotoneTuple rightValueTuple = rightValue.to(PartiallyMonotoneTuple.class);
        IMaybeMonotoneType leftTS = leftValueTuple.getField(join.leftTimestampIndex);
        IMaybeMonotoneType rightTS = rightValueTuple.getField(join.rightTimestampIndex);
        if (!leftTS.mayBeMonotone() || !rightTS.mayBeMonotone()) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

        // Extract the timestamps from the limiters
        OutputPort extractLeftTS = this.extractTimestamp(leftMono, join.leftTimestampIndex, leftLimiter);
        OutputPort extractRightTS = this.extractTimestamp(rightMono, join.rightTimestampIndex, rightLimiter);

        // Compute the min of the timestamps
        DBSPVariablePath leftVar = this.getLimiterDataOutputType(extractLeftTS).ref().var();
        DBSPVariablePath rightVar = this.getLimiterDataOutputType(extractRightTS).ref().var();
        DBSPExpression min = new DBSPTupleExpression(this.min(leftVar.deref(), rightVar.deref()));
        OutputPort minOperator = this.createApply2(extractLeftTS, extractRightTS, min.closure(leftVar, rightVar));

        DBSPTypeTuple keyType = join.getKeyType().to(DBSPTypeTuple.class);
        PartiallyMonotoneTuple keyPart = PartiallyMonotoneTuple.noMonotoneFields(keyType);

        DBSPTypeTupleBase leftValueType = join.getLeftInputValueType().to(DBSPTypeTupleBase.class);
        List<IMaybeMonotoneType> value = new ArrayList<>();
        for (int i = 0; i < leftValueType.size(); i++) {
            DBSPType field = leftValueType.getFieldType(i);
            IMaybeMonotoneType mono;
            if (i == join.leftTimestampIndex) {
                mono = new MonotoneType(field);
            } else {
                mono = NonMonotoneType.nonMonotone(field);
            }
            value.add(mono);
        }
        PartiallyMonotoneTuple valuePart = new PartiallyMonotoneTuple(value, false, false);
        PartiallyMonotoneTuple dataProjection = new PartiallyMonotoneTuple(
                Linq.list(keyPart, valuePart), true, false);

        if (INSERT_RETAIN_VALUES) {
            DBSPSimpleOperator retain = DBSPIntegrateTraceRetainValuesOperator.create(
                    join.getRelNode(), this.mapped(join.left()), dataProjection, this.createDelay(minOperator));
            this.addOperator(retain);
        }

        super.postorder(join);
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

        this.addJoinAnnotations(join);
        DBSPSimpleOperator result = this.gcJoin(join, expansion);
        if (result == null) {
            super.postorder(join);
            this.nonMonotone(join);
            return;
        }

        this.processIntegral(expansion.leftIntegrator);
        this.processIntegral(expansion.rightIntegrator);
        this.processJoin(expansion.leftDelta);
        this.processJoin(expansion.rightDelta);
        this.processJoin(expansion.both);
        this.processFilter(expansion.filter);
        this.processFilter(expansion.leftFilter);
        this.processFilter(expansion.rightFilter);
        OutputPort limiter = this.processSumOrDiff(expansion.sum);
        if (limiter != null)
            this.markBound(join.outputPort(), limiter);

        // If any of the filters leftFilter or rightFilter has monotone outputs,
        // we can use these to GC the other input of the join using
        // DBSPIntegrateTraceRetainValuesOperator.

        Projection proj = new Projection(this.compiler(), true, false);
        proj.apply(join.getFunction());
        Utilities.enforce((proj.isProjection));
        Projection.IOMap iomap = proj.getIoMap();
        // This will look something like.  Ordered by output field number.
        // [input#, field#]
        // [0, 0]
        // [1, 0]
        // [1, 2]
        // [0, 1]
        // [2, 0]
        // [2, 1]
        // input# is 0 = key, 1 = left, 2 = right
        int leftValueSize = join.left().getOutputIndexedZSetType().getElementTypeTuple().size();
        int rightValueSize = join.right().getOutputIndexedZSetType().getElementTypeTuple().size();

        DBSPTypeTuple keyType = join.getKeyType().to(DBSPTypeTuple.class);
        PartiallyMonotoneTuple keyPart = PartiallyMonotoneTuple.noMonotoneFields(keyType);

        // Check the left side and insert a GC operator if possible
        if (expansion.leftFilter != null) {
            OutputPort leftLimiter = this.bound.get(expansion.leftFilter.outputPort());
            if (leftLimiter != null) {
                MonotoneExpression monotone = this.expansionMonotoneValues.get(expansion.leftFilter);
                IMaybeMonotoneType projection = Monotonicity.getBodyType(Objects.requireNonNull(monotone));
                if (projection.mayBeMonotone()) {
                    PartiallyMonotoneTuple tuple = projection.to(PartiallyMonotoneTuple.class);
                    Utilities.enforce(tuple.size() == iomap.size());

                    List<IMaybeMonotoneType> value = new ArrayList<>();
                    DBSPVariablePath var = Objects.requireNonNull(tuple.getProjectedType()).ref().var();
                    List<DBSPExpression> monotoneFields = new ArrayList<>();
                    for (int index = 0, field = 0; field < leftValueSize; field++) {
                        int firstOutputField = iomap.firstOutputField(1, field);
                        // We assume that every left input field is used as an output
                        Utilities.enforce(firstOutputField >= 0);
                        IMaybeMonotoneType compareField = tuple.getField(firstOutputField);
                        value.add(compareField);
                        if (compareField.mayBeMonotone()) {
                            monotoneFields.add(var.deref().field(index++));
                        }
                    }
                    PartiallyMonotoneTuple valuePart = new PartiallyMonotoneTuple(value, false, false);

                    // Put the fields together
                    PartiallyMonotoneTuple together = new PartiallyMonotoneTuple(
                            Linq.list(keyPart, valuePart), true, false);

                    DBSPExpression func = new DBSPTupleExpression(monotoneFields, false);
                    OutputPort extractLeft = this.createApply(leftLimiter, join, func.closure(var));

                    if (INSERT_RETAIN_VALUES) {
                        DBSPSimpleOperator l = DBSPIntegrateTraceRetainValuesOperator.create(
                                join.getRelNode(), this.mapped(join.left()), together,
                                this.createDelay(extractLeft));
                        this.addOperator(l);
                    }
                }
            }
        }

        // Exact same procedure on the right hand side
        if (expansion.rightFilter != null) {
            OutputPort rightLimiter = this.bound.get(expansion.rightFilter.outputPort());
            if (rightLimiter != null) {
                MonotoneExpression monotone = this.expansionMonotoneValues.get(expansion.rightFilter);
                IMaybeMonotoneType projection = Monotonicity.getBodyType(Objects.requireNonNull(monotone));
                if (projection.mayBeMonotone()) {
                    PartiallyMonotoneTuple tuple = projection.to(PartiallyMonotoneTuple.class);
                    Utilities.enforce(tuple.size() == iomap.size());

                    List<IMaybeMonotoneType> value = new ArrayList<>();
                    DBSPVariablePath var = Objects.requireNonNull(tuple.getProjectedType()).ref().var();
                    List<DBSPExpression> monotoneFields = new ArrayList<>();
                    for (int field = 0, index = 0; field < rightValueSize; field++) {
                        int firstOutputField = iomap.firstOutputField(2, field);
                        // We assume that every left input field is used as an output
                        Utilities.enforce(firstOutputField >= 0);
                        IMaybeMonotoneType compareField = tuple.getField(firstOutputField);
                        value.add(compareField);
                        if (compareField.mayBeMonotone()) {
                            monotoneFields.add(var.deref().field(index++));
                        }
                    }
                    PartiallyMonotoneTuple valuePart = new PartiallyMonotoneTuple(value, false, false);

                    // Put the fields together
                    PartiallyMonotoneTuple together = new PartiallyMonotoneTuple(
                            Linq.list(keyPart, valuePart), true, false);

                    DBSPExpression func = new DBSPTupleExpression(monotoneFields, false);
                    OutputPort extractRight = this.createApply(rightLimiter, join, func.closure(var));

                    if (INSERT_RETAIN_VALUES) {
                        DBSPSimpleOperator r = DBSPIntegrateTraceRetainValuesOperator.create(
                                join.getRelNode(), this.mapped(join.right()), together,
                                this.createDelay(extractRight));
                        this.addOperator(r);
                    }
                }
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
                        left.getType(), DBSPOpcode.AGG_MAX, left, right);
            }
        } else if (leftProjection.is(PartiallyMonotoneTuple.class)) {
            PartiallyMonotoneTuple l = leftProjection.to(PartiallyMonotoneTuple.class);
            PartiallyMonotoneTuple r = rightProjection.to(PartiallyMonotoneTuple.class);
            Utilities.enforce(r.size() == l.size());
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
        throw new InternalCompilerError("Not yet handlex: max of type " + leftProjection,
                left.getNode());
    }

    /** Utility function for Apply operators that are introduced by the {@link InsertLimiters}
     * pass, converting their output type into a tuple.
     * This tuple always has a boolean on the first position. */
    DBSPTypeTupleBase getLimiterOutputType(OutputPort operator) {
        return operator.outputType().to(DBSPTypeTupleBase.class);
    }

    DBSPType getLimiterDataOutputType(OutputPort operator) {
        return this.getLimiterOutputType(operator).tupFields[1];
    }

    void processJoin(@Nullable DBSPJoinBaseOperator expanded) {
        if (expanded == null)
            return;
        MonotoneExpression monotoneValue = this.expansionMonotoneValues.get(expanded);
        if (monotoneValue == null || !monotoneValue.mayBeMonotone()) {
            this.nonMonotone(expanded);
            return;
        }
        OutputPort leftLimiter = this.bound.get(expanded.left());
        OutputPort rightLimiter = this.bound.get(expanded.right());
        if (leftLimiter == null && rightLimiter == null) {
            this.nonMonotone(expanded);
            return;
        }

        boolean rightMayBeNull = expanded.getClosureFunction().parameters[2].getType().deref().mayBeNull;
        PartiallyMonotoneTuple out = Monotonicity.getBodyType(monotoneValue).to(PartiallyMonotoneTuple.class);
        DBSPType outputType = out.getProjectedType();
        Utilities.enforce(outputType != null);
        OutputPort merger;
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
            Utilities.enforce(leftMono != null);
            Utilities.enforce(rightMono != null);
            DBSPVariablePath l = new DBSPVariablePath(this.getLimiterDataOutputType(leftLimiter).ref());
            DBSPVariablePath r = new DBSPVariablePath(this.getLimiterDataOutputType(rightLimiter).ref());
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
                fields[2] = new DBSPTupleExpression(rightMayBeNull);

            DBSPClosureExpression closure =
                    new DBSPRawTupleExpression(fields)
                            .closure(l, r);
            merger = this.createApply2(leftLimiter, rightLimiter, closure);
        } else if (leftLimiter != null) {
            // (k, l) -> (k, l, Tup0<>)
            Utilities.enforce(leftMono != null);
            DBSPVariablePath var = new DBSPVariablePath(this.getLimiterDataOutputType(leftLimiter).ref());
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
                            new DBSPTupleExpression(rightMayBeNull))
                            .closure(var);
            merger = this.createApply(leftLimiter, null, closure);
        } else {
            // (k, r) -> (k, Tup0<>, r)
            Utilities.enforce(rightMono != null);
            DBSPVariablePath var = new DBSPVariablePath(this.getLimiterDataOutputType(rightLimiter).ref());
            DBSPExpression k = new DBSPTupleExpression();
            DBSPExpression r = new DBSPTupleExpression(rightMayBeNull);
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
                            .closure(var);
            merger = this.createApply(rightLimiter, null, closure);
        }

        DBSPClosureExpression clo = monotoneValue.getReducedExpression().to(DBSPClosureExpression.class);
        OutputPort limiter = this.createApply(merger, null, clo);
        this.markBound(expanded.outputPort(), limiter);
    }

    public static DBSPClosureExpression timestampMax(CalciteObject node, DBSPTypeTupleBase type) {
        return type.pairwiseOperation(node, DBSPOpcode.AGG_MAX);
    }

    /** Generate an expression that compares two other expressions for equality */
    DBSPExpression eq(DBSPExpression left, DBSPExpression right) {
        DBSPType type = left.getType();
        if (type.is(DBSPTypeBaseType.class)) {
            boolean mayBeNull = type.mayBeNull || right.getType().mayBeNull;
            DBSPExpression compare = ExpressionCompiler.makeBinaryExpression(left.getNode(),
                    DBSPTypeBool.create(mayBeNull), DBSPOpcode.EQ, left, right);
            return compare.wrapBoolIfNeeded();
        } else if (type.is(DBSPTypeTupleBase.class)) {
            DBSPTypeTupleBase tuple = type.to(DBSPTypeTupleBase.class);
            DBSPExpression result = new DBSPBoolLiteral(true);
            for (int i = 0; i < tuple.size(); i++) {
                DBSPExpression compare = eq(left.field(i).simplify(), right.field(i).simplify());
                result = ExpressionCompiler.makeBinaryExpression(left.getNode(),
                        DBSPTypeBool.create(false), DBSPOpcode.AND, result, compare);
            }
            return result;
        } else {
            throw new InternalCompilerError("Not handled equality of type " + type.asSqlString(), type);
        }
    }

    /** Process LATENESS annotations.
     * @return Return the original operator if there aren't any annotations, or
     * the operator that produces the result of the input filtered otherwise.
     * The replacement depends on whether there are primary keys:
     * - for no primary keys, we insert a chain {@link DBSPWaterlineOperator}, {@link DBSPDelayOperator},
     * {@link DBSPControlledKeyFilterOperator}.
     * - for primary keys we use a single operator which does all of these in one shot:
     * {@link DBSPInputMapWithWaterlineOperator}*/
    DBSPOperator processLateness(
            ProgramIdentifier viewOrTable, DBSPSimpleOperator operator, DBSPSimpleOperator expansion) {
        MonotoneExpression me = this.expansionMonotoneValues.get(expansion);
        if (me == null) {
            this.nonMonotone(expansion);
            return operator;
        }
        DBSPSourceMultisetOperator multisetInput = operator.as(DBSPSourceMultisetOperator.class);
        final DBSPTypeIndexedZSet indexedOutputType = (multisetInput != null) ?
                IndexedInputs.getIndexedType(multisetInput) : null;

        List<DBSPExpression> timestamps = new ArrayList<>();
        int index = 0;
        DBSPVariablePath t = operator.getOutputZSetType().elementType.ref().var();
        for (IColumnMetadata column: operator.to(IHasColumnsMetadata.class).getColumnsMetadata()) {
            DBSPExpression lateness = column.getLateness();
            if (lateness != null) {
                DBSPExpression field = t.deref().field(index);
                field = ExpressionCompiler.makeBinaryExpression(operator.getRelNode(), field.getType(),
                        DBSPOpcode.SUB, field, lateness);
                timestamps.add(field);
            }
            index++;
        }

        if (timestamps.isEmpty()) {
            this.nonMonotone(expansion);
            return operator;
        }

        boolean replaceIndexedInput = indexedOutputType != null;

        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        DBSPSimpleOperator replacement = operator.withInputs(sources, this.force)
                .to(DBSPSimpleOperator.class);
        replacement.setDerivedFrom(operator);
        if (!replaceIndexedInput)
            this.addOperator(replacement);

        // The waterline operator will compute the *minimum legal value* of all the
        // inputs that have a lateness attached.  The output signature contains only
        // the columns that have lateness.
        DBSPTupleExpression timestamp = new DBSPTupleExpression(timestamps, false);
        DBSPExpression min = timestamp.getType().minimumValue();
        DBSPClosureExpression max = timestampMax(operator.getRelNode(), min.getType().to(DBSPTypeTupleBase.class));

        DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(
                operator.getRelNode(), min.closure(),
                // second parameter unused for timestamp
                timestamp.closure(t, DBSPTypeRawTuple.EMPTY.ref().var()),
                max, replacement.outputPort());
        OutputPort waterlineOutputPort = waterline.outputPort();
        if (!replaceIndexedInput)
            this.addOperator(waterline);

        // Waterline fed through a delay
        DBSPDelayOperator delay = new DBSPDelayOperator(operator.getRelNode(), min, waterline.outputPort());
        if (!replaceIndexedInput)
            this.addOperator(delay);

        DBSPControlledKeyFilterOperator filter = this.createControlledKeyFilter(
                operator.getRelNode(), viewOrTable, replacement.outputPort(),
                Monotonicity.getBodyType(me), delay.outputPort());
        DBSPOperator result = filter;
        DBSPInputMapWithWaterlineOperator newSource = null;

        if (replaceIndexedInput) {
            List<Integer> keyFields = IndexedInputs.getKeyFields(multisetInput);
            // Many of these functions take the key as a parameter, although
            // it is a subset of the value fields...
            DBSPVariablePath k = indexedOutputType.keyType.ref().var();
            Utilities.enforce(filter.function.parameters.length == 2);
            DBSPClosureExpression ff = filter.function.body.closure(
                    filter.function.parameters[0], k.asParameter(), filter.function.parameters[1]);
            Utilities.enforce(filter.error.parameters.length == 4);
            DBSPClosureExpression error = filter.error.body.closure(
                    filter.error.parameters[0], k.asParameter(), filter.error.parameters[1], filter.error.parameters[3]);
            newSource = new DBSPInputMapWithWaterlineOperator(
                    multisetInput.getRelNode(), multisetInput.sourceName, keyFields,
                    indexedOutputType, multisetInput.originalRowType, multisetInput.metadata, multisetInput.tableName,
                    min.closure(), timestamp.closure(k, t), max, ff, error);
            this.errorStreams.add(newSource.getOutput(1));
            waterlineOutputPort = newSource.getOutput(2);
            this.addOperator(newSource);

            result = new DBSPDeindexOperator(multisetInput.getRelNode(), multisetInput.getNode(),
                    newSource.getOutput(0));
        } else {
            OutputPort errorPort = result.getOutput(1);
            if (!this.reachableFromError.contains(result.inputs.get(0).operator)) {
                // This would create a cycle, so skip.
                this.errorStreams.add(errorPort);
                // TODO: connect through a delay.
            }
        }

        // An apply operator to add a Boolean bit to the waterline.
        // This bit is 'true' when the waterline produces a value
        // that is not 'minimum'.
        DBSPVariablePath var = waterlineOutputPort.outputType().ref().var();
        // If the v is &TypedBox<T>, v.deref().deref() has type T
        DBSPExpression unwrapped = replaceIndexedInput ? var.deref().deref() : var.deref();
        DBSPExpression eq = eq(min, unwrapped);
        DBSPSimpleOperator extend = new DBSPApplyOperator(operator.getRelNode(),
                new DBSPTupleExpression(
                        eq.not(),
                        unwrapped.applyClone()).closure(var),
                waterlineOutputPort, null);
        extend.addAnnotation(Waterline.INSTANCE, DBSPSimpleOperator.class);
        this.addOperator(extend);
        this.markBound(operator.outputPort(), extend.outputPort());
        if (operator != expansion)
            this.markBound(expansion.outputPort(), extend.outputPort());

        if (INSERT_RETAIN_VALUES && replaceIndexedInput &&
                multisetInput != null &&
                !multisetInput.getMetadata().materialized) {
            // Do not GC materialized tables
            IMaybeMonotoneType projection = me.getMonotoneType().to(MonotoneClosureType.class).getBodyType();
            // The new input operator produces an indexed Zset, need to adjust the projection
            projection = new PartiallyMonotoneTuple(
                    List.of(NonMonotoneType.nonMonotone(indexedOutputType.keyType), projection), true, false);
            DBSPSimpleOperator retain = DBSPIntegrateTraceRetainValuesOperator.create(
                    operator.getRelNode(), newSource.getOutput(0),
                    projection, extend.outputPort(), false);
            this.addOperator(retain);
        }

        return result;
    }

    DBSPControlledKeyFilterOperator createControlledKeyFilter(
            CalciteRelNode node, ProgramIdentifier tableOrViewName,
            OutputPort data, IMaybeMonotoneType monotoneType,
            OutputPort control) {
        DBSPType controlType = control.outputType();

        DBSPType leftSliceType = Objects.requireNonNull(monotoneType.getProjectedType());
        Utilities.enforce(leftSliceType.sameType(controlType),
                () -> "Projection type does not match control type " + leftSliceType + "/" + controlType);

        DBSPType rowType = data.getOutputRowType();
        DBSPVariablePath dataVar = rowType.ref().var();
        DBSPExpression projection = monotoneType.projectExpression(dataVar.deref());

        DBSPVariablePath controlArg = controlType.ref().var();
        DBSPExpression compare = DBSPControlledKeyFilterOperator.generateTupleCompare(
                projection, controlArg.deref(), DBSPOpcode.CONTROLLED_FILTER_GTE);
        DBSPClosureExpression closure = compare.closure(controlArg, dataVar);

        // The last parameter is not used
        DBSPVariablePath dataVar0 = dataVar.getType().var();
        DBSPVariablePath valArg = DBSPTypeRawTuple.EMPTY.ref().var();
        DBSPVariablePath weightArg = DBSPTypeWeight.INSTANCE.var();
        DBSPClosureExpression error = new DBSPTupleExpression(
                new DBSPStringLiteral(tableOrViewName.toString()),
                new DBSPStringLiteral(DBSPControlledKeyFilterOperator.LATE_ERROR),
                new DBSPApplyExpression("format!",
                        DBSPTypeAny.INSTANCE,
                        new DBSPStrLiteral("{:?}"),
                        dataVar0).applyMethod("into", DBSPTypeString.varchar(false)))
                .closure(controlArg, dataVar0, valArg, weightArg);
        return new DBSPControlledKeyFilterOperator(
                node, closure, error, data, control);
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
        ReplacementExpansion replacementExpansion = Objects.requireNonNull(this.getReplacement(operator));
        DBSPOperator replacement = this.processLateness(operator.tableName, operator, replacementExpansion.replacement);

        // Process watermark annotations.  Very similar to lateness annotations.
        int index = 0;
        DBSPType dataType = operator.getOutputZSetType().elementType;
        DBSPVariablePath t = dataType.ref().var();
        List<DBSPExpression> fields = new ArrayList<>();
        List<DBSPExpression> timestamps = new ArrayList<>();
        List<DBSPExpression> minimums = new ArrayList<>();
        for (IColumnMetadata column: operator.to(IHasColumnsMetadata.class).getColumnsMetadata()) {
            DBSPExpression watermark = column.getWatermark();
            if (watermark != null) {
                DBSPExpression field = t.deref().field(index);
                fields.add(field);
                DBSPType type = field.getType();
                field = ExpressionCompiler.makeBinaryExpression(operator.getRelNode(), field.getType(),
                        DBSPOpcode.SUB, field, watermark);
                timestamps.add(field);
                DBSPExpression min = type.to(IsBoundedType.class).getMinValue();
                minimums.add(min);
            }
            index++;
        }

        // Currently we only support at most 1 watermark column per table.
        // TODO: support multiple fields.
        if (minimums.size() > 1) {
            throw new UnimplementedException("More than 1 watermark per table not yet supported",
                    2734, operator.getRelNode());
        }

        if (!minimums.isEmpty()) {
            Utilities.enforce(fields.size() == 1);
            this.addOperator(replacement);

            DBSPTupleExpression min = new DBSPTupleExpression(minimums, false);
            DBSPTupleExpression timestamp = new DBSPTupleExpression(timestamps, false);
            DBSPParameter parameter = t.asParameter();
            DBSPClosureExpression max = timestampMax(operator.getRelNode(), min.getTypeAsTupleBase());
            DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(
                    operator.getRelNode(), min.closure(),
                    // Second parameter unused for timestamp
                    timestamp.closure(parameter, DBSPTypeRawTuple.EMPTY.ref().var().asParameter()),
                    max, operator.outputPort());
            this.addOperator(waterline);

            DBSPVariablePath var = timestamp.getType().ref().var();
            DBSPExpression makePair = new DBSPRawTupleExpression(
                    DBSPTypeTypedBox.wrapTypedBox(minimums.get(0), false),
                    DBSPTypeTypedBox.wrapTypedBox(var.deref().field(0), false));
            DBSPApplyOperator apply = new DBSPApplyOperator(
                    operator.getRelNode(), makePair.closure(var), waterline.outputPort(),
                    "(" + operator.getDerivedFrom() + ")");
            this.addOperator(apply);

            // Window requires data to be indexed
            DBSPSimpleOperator ix = new DBSPMapIndexOperator(operator.getRelNode(),
                    new DBSPRawTupleExpression(
                            fields.get(0).applyCloneIfNeeded(),
                            t.deref().applyCloneIfNeeded()).closure(t),
                    new DBSPTypeIndexedZSet(operator.getRelNode(),
                            fields.get(0).getType(), dataType), true, replacement.getOutput(0));
            this.addOperator(ix);
            DBSPWindowOperator window = new DBSPWindowOperator(
                    operator.getRelNode(), t.getNode(),true, true, ix.outputPort(), apply.outputPort());
            this.addOperator(window);
            replacement = new DBSPDeindexOperator(operator.getRelNode(), operator.getNode(), window.outputPort());
        }

        if (replacement == operator) {
            this.replace(operator);
        } else {
            this.map(operator.getOutput(0), replacement.getOutput(0));
        }
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        // Treat like an identity function
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null) {
            OutputPort bound = this.processSumOrDiff(expanded.replacement);
            if (bound != null && expanded.replacement != operator)
                this.markBound(operator.outputPort(), bound);
        } else {
            this.nonMonotone(operator);
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSubtractOperator operator) {
        // Similar to sum
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null) {
            OutputPort bound = this.processSumOrDiff(expanded.replacement);
            if (bound != null && expanded.replacement != operator)
                this.markBound(operator.outputPort(), bound);
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
            Utilities.enforce(sourceProjection.is(ScalarMonotoneType.class));
            return source;
        } else if (destinationProjection.is(PartiallyMonotoneTuple.class)) {
            Utilities.enforce(sourceProjection.is(PartiallyMonotoneTuple.class));
            PartiallyMonotoneTuple src = sourceProjection.to(PartiallyMonotoneTuple.class);
            PartiallyMonotoneTuple dest = destinationProjection.to(PartiallyMonotoneTuple.class);
            Utilities.enforce(src.size() == dest.size());
            List<DBSPExpression> fields = new ArrayList<>();
            int currentIndex = 0;
            for (int i = 0; i < dest.size(); i++) {
                if (dest.getField(i).mayBeMonotone()) {
                    fields.add(this.project(source.field(currentIndex), src.getField(i), dest.getField(i)));
                }
                if (src.getField(i).mayBeMonotone()) {
                    currentIndex++;
                }
            }
            if (dest.raw)
                return new DBSPRawTupleExpression(source.getNode(), fields);
            else
                return new DBSPTupleExpression(source.getNode(), fields);
        }
        throw new InternalCompilerError("Not yet handled " + destinationProjection, source.getNode());
    }

    /** Apply MIN pointwise to two expressions */
    DBSPExpression min(DBSPExpression left,
                       DBSPExpression right) {
        Utilities.enforce(left.getType().sameTypeIgnoringNullability(right.getType()));
        if (left.getType().is(DBSPTypeBaseType.class)) {
            return ExpressionCompiler.makeBinaryExpression(
                    left.getNode(), left.getType(), DBSPOpcode.AGG_MIN, left, right);
        } else if (left.getType().is(DBSPTypeTupleBase.class)) {
            DBSPTypeTupleBase lt = left.getType().to(DBSPTypeTupleBase.class);
            DBSPExpression[] mins = new DBSPExpression[lt.size()];
            for (int i = 0; i < lt.size(); i++) {
                mins[i] = min(left.field(i), right.field(i));
            }
            return lt.makeTuple(mins);
        }
        throw new InternalCompilerError("MIN of expressions of type " + left.getType() + " not yet supported",
                left.getNode());
    }

    /**
     * Create and insert an operator which projects the output of limit.
     *
     * @param limit       The output of this operator is being projected.
     * @param source      Monotonicity information about the output of limit.
     * @param destination Monotonicity information for the desired output.
     * @return The operator performing the projection.
     * The operator is inserted in the graph.
     */
    OutputPort project(
            OutputPort limit, IMaybeMonotoneType source,
            IMaybeMonotoneType destination) {
        DBSPVariablePath var = this.getLimiterDataOutputType(limit).ref().var();
        DBSPExpression proj = this.project(var.deref(), source, destination);
        return this.createApply(limit, null, proj.closure(var));
    }

    @Nullable
    private OutputPort processSumOrDiff(DBSPSimpleOperator expanded) {
        // Handles sum and subtract operators
        MonotoneExpression monotoneValue = this.expansionMonotoneValues.get(expanded);
        if (monotoneValue == null || !monotoneValue.mayBeMonotone()) {
            this.nonMonotone(expanded);
            return null;
        }

        // Create a projection for each input
        // Collect input limits
        List<OutputPort> limiters = new ArrayList<>();
        List<IMaybeMonotoneType> mono = new ArrayList<>();
        for (OutputPort input: expanded.inputs) {
            OutputPort limiter = this.bound.get(input);
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
        Utilities.enforce(outputType != null);

        // Same function everywhere
        DBSPVariablePath l = new DBSPVariablePath(outputType.ref());
        DBSPVariablePath r = new DBSPVariablePath(outputType.ref());
        DBSPClosureExpression min = this.min(l.deref(), r.deref())
                .closure(l, r);

        // expand into a binary unbalanced tree (this could be a star if we had an applyN operator).
        OutputPort current = this.project(limiters.get(0), mono.get(0), out);
        for (int i = 1; i < expanded.inputs.size(); i++) {
            OutputPort next = this.project(limiters.get(i), mono.get(i), out);
            current = this.createApply2(current, next, min);
        }

        this.markBound(expanded.outputPort(), current);
        return current;
    }

    @Override
    public void postorder(DBSPViewOperator operator) {
        if (operator.viewName.equals(DBSPCompiler.ERROR_VIEW_NAME)) {
            // Unhook from the error table and hook input to all the
            // error streams generated so far.
            // Since in a prior pass we have reordered the operators
            // such that this view comes after all other operators,
            // we know that all possible error streams at this point have
            // been already computed.
            OutputPort collected;
            if (this.errorStreams.isEmpty()) {
                DBSPSimpleOperator c = new DBSPConstantOperator(operator.getRelNode(),
                        DBSPZSetExpression.emptyWithElementType(operator.getOutputZSetElementType()), false);
                this.addOperator(c);
                collected = c.outputPort();
            } else if (this.errorStreams.size() > 1) {
                DBSPSumOperator sum = new DBSPSumOperator(operator.getRelNode(), this.errorStreams);
                this.addOperator(sum);
                collected = sum.outputPort();
            } else {
                collected = this.errorStreams.get(0);
            }
            DBSPSimpleOperator newView = operator.withInputs(Linq.list(collected), false)
                    .to(DBSPSimpleOperator.class);
            this.map(operator, newView);
            // No lateness to propagate for error view
            return;
        }

        if (operator.hasLateness()) {
            ReplacementExpansion expanded = this.getReplacement(operator);
            // Treat like a source operator
            DBSPOperator replacement = this.processLateness(
                    operator.viewName, operator, Objects.requireNonNull(expanded).replacement);
            if (replacement == operator) {
                super.postorder(operator);
            } else {
                this.map(operator.getOutput(0), replacement.getOutput(0));
            }
            return;
        }
        // Treat like an identity function
        this.addBoundsForNonExpandedOperator(operator);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        int monotoneFieldIndex = operator.metadata.emitFinalColumn;
        if (monotoneFieldIndex >= 0) {
            ReplacementExpansion expanded = this.getReplacement(operator);
            if (expanded != null) {
                DBSPSimpleOperator operatorFromExpansion = expanded.replacement;
                MonotoneExpression monotone = this.expansionMonotoneValues.get(operatorFromExpansion);
                if (monotone != null && monotone.mayBeMonotone()) {
                    OutputPort source = operatorFromExpansion.inputs.get(0);
                    OutputPort boundSource = Utilities.getExists(this.bound, source);

                    MonotoneClosureType monoClosure = monotone.getMonotoneType().to(MonotoneClosureType.class);
                    IMaybeMonotoneType bodyType = monoClosure.getBodyType();
                    Utilities.enforce(bodyType.mayBeMonotone());
                    PartiallyMonotoneTuple tuple = bodyType.to(PartiallyMonotoneTuple.class);
                    if (!tuple.getField(monotoneFieldIndex).mayBeMonotone()) {
                        var col = operator.metadata.columns.get(monotoneFieldIndex);
                        throw new CompilationError("Compiler could not infer a waterline for column " +
                                operator.viewName.singleQuote() + "." +
                                col.columnName.singleQuote(),
                                col.getPositionRange());
                    }
                    int controlFieldIndex = 0;
                    Utilities.enforce(monotoneFieldIndex < tuple.size());
                    for (int i = 0; i < monotoneFieldIndex; i++) {
                        IMaybeMonotoneType field = tuple.getField(i);
                        if (field.mayBeMonotone())
                            controlFieldIndex++;
                    }

                    DBSPTypeTupleBase dataType = operator.getOutputZSetType().elementType.to(DBSPTypeTupleBase.class);
                    DBSPVariablePath t = dataType.ref().var();
                    DBSPType tsType = dataType.getFieldType(monotoneFieldIndex);
                    DBSPExpression minimum = tsType.to(IsBoundedType.class).getMinValue();

                    // boundSource has a type (boolean, TupN), extract the corresponding
                    // value in the second tuple
                    DBSPVariablePath var = boundSource.outputType().ref().var();
                    DBSPExpression makePair = new DBSPRawTupleExpression(
                            DBSPTypeTypedBox.wrapTypedBox(minimum, false),
                            DBSPTypeTypedBox.wrapTypedBox(var.deref().field(1).field(controlFieldIndex), false));
                    DBSPApplyOperator apply = new DBSPApplyOperator(
                            operator.getRelNode(), makePair.closure(var.asParameter()), boundSource,
                            "(" + operator.getDerivedFrom() + ")");
                    this.addOperator(apply);

                    // Window requires data to be indexed
                    DBSPExpression field = t.deref().field(monotoneFieldIndex);
                    DBSPSimpleOperator ix = new DBSPMapIndexOperator(operator.getRelNode(),
                            new DBSPRawTupleExpression(
                                    field.applyCloneIfNeeded(),
                                    t.deref().applyCloneIfNeeded()).closure(t.asParameter()),
                            new DBSPTypeIndexedZSet(operator.getRelNode(),
                                    field.getType(), dataType), true,
                            this.mapped(operator.input()));
                    this.addOperator(ix);
                    // The upper bound must be exclusive
                    DBSPWindowOperator window = new DBSPWindowOperator(
                            operator.getRelNode(), t.getNode(), true, false, ix.outputPort(), apply.outputPort());
                    this.addOperator(window);
                    // GC for window: the waterline delayed
                    PartiallyMonotoneTuple projection = new PartiallyMonotoneTuple(
                            // We project the key of the index node, which is always the first field.
                            Linq.list(new MonotoneType(tsType)), false, false);

                    // The boundSource may have more than 1 monotone field, keep only the one we care about.
                    DBSPTypeTuple monotoneFields = boundSource.outputType()
                            .to(DBSPTypeTuple.class)
                            .getFieldType(1)
                            .to(DBSPTypeTuple.class);
                    if (monotoneFields.size() > 1) {
                        DBSPVariablePath v = boundSource.outputType().ref().var();
                        DBSPExpression dropFields = new DBSPTupleExpression(
                                v.deref().field(0),
                                new DBSPTupleExpression(v.deref().field(1).field(controlFieldIndex)));
                        DBSPApplyOperator dropOp = new DBSPApplyOperator(
                                operator.getRelNode(), dropFields.closure(v.asParameter()), boundSource, "");
                        this.addOperator(dropOp);
                        boundSource = dropOp.outputPort();
                    }
                    this.createRetainKeys(operator.getRelNode(), ix.outputPort(), projection, boundSource);

                    DBSPSimpleOperator deindex = new DBSPDeindexOperator(
                            operator.getRelNode(), window.getFunctionNode(), window.outputPort());
                    this.addOperator(deindex);
                    DBSPSimpleOperator sink = operator.withInputs(Linq.list(deindex.outputPort()), false)
                            .to(DBSPSimpleOperator.class);
                    this.map(operator, sink);
                    return;
                }
            }
            var col = operator.metadata.columns.get(monotoneFieldIndex);
            throw new CompilationError("Could not infer a waterline for column " +
                    operator.viewName.singleQuote() + "." +
                    operator.metadata.columns.get(monotoneFieldIndex).columnName.singleQuote() +
                    " which has an 'emit_final' annotation",
                    col.getPositionRange());
        }
        super.postorder(operator);
    }
}
