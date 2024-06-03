package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPControlledFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.IHasColumnsMetadata;
import org.dbsp.sqlCompiler.compiler.IHasLateness;
import org.dbsp.sqlCompiler.compiler.IHasWatermark;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.AggregateExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.JoinExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.OperatorExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.ReplacementExpansion;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.IsBoundedType;
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
    public final Map<DBSPOperator, MonotoneExpression> expansionMonotoneValues;
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
                          Map<DBSPOperator, MonotoneExpression> expansionMonotoneValues,
                          Map<DBSPOperator, OperatorExpansion> expandedInto) {
        super(reporter, false);
        this.expandedCircuit = expandedCircuit;
        this.expansionMonotoneValues = expansionMonotoneValues;
        this.expandedInto = expandedInto;
        this.bound = new HashMap<>();
    }

    void markBound(DBSPOperator operator, DBSPOperator bound) {
        Logger.INSTANCE.belowLevel(this, 2)
                .append("Bound for ")
                .append(operator.getIdString())
                .append(" computed by ")
                .append(bound.getIdString())
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
    @Nullable
    DBSPOperator addBounds(@Nullable DBSPOperator operatorFromExpansion, int input) {
        if (operatorFromExpansion == null)
            return null;
        MonotoneExpression monotone = this.expansionMonotoneValues.get(operatorFromExpansion);
        if (monotone == null)
            return null;
        DBSPOperator source = operatorFromExpansion.inputs.get(input);  // Even for binary operators
        DBSPOperator boundSource = Utilities.getExists(this.bound, source);
        DBSPClosureExpression function = monotone.getReducedExpression().to(DBSPClosureExpression.class);
        DBSPOperator bound = new DBSPApplyOperator(operatorFromExpansion.getNode(), function,
                function.getFunctionType().resultType, boundSource,
                "(" + operatorFromExpansion.getDerivedFrom() + ")");
        this.getResult().addOperator(bound);  // insert directly into circuit
        this.markBound(operatorFromExpansion, bound);
        return bound;
    }

    @Nullable
    ReplacementExpansion getReplacement(DBSPOperator operator) {
        OperatorExpansion expanded = this.expandedInto.get(operator);
        if (expanded == null)
            return null;
        return expanded.to(ReplacementExpansion.class);
    }

    @Override
    public void postorder(DBSPDifferentiateOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null)
            this.addBounds(expanded.replacement, 0);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPIntegrateOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null)
            this.addBounds(expanded.replacement, 0);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null)
            this.addBounds(expanded.replacement, 0);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null)
            this.addBounds(expanded.replacement, 0);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null)
            this.addBounds(expanded.replacement, 0);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPAggregateOperator aggregator) {
        DBSPOperator source = this.mapped(aggregator.input());
        OperatorExpansion expanded = this.expandedInto.get(aggregator);
        if (expanded == null) {
            super.postorder(aggregator);
            return;
        }

        AggregateExpansion ae = expanded.to(AggregateExpansion.class);
        DBSPOperator limiter = this.addBounds(ae.integrator, 0);
        if (limiter == null) {
            super.postorder(aggregator);
            return;
        }

        MonotoneExpression expression = this.expansionMonotoneValues.get(ae.integrator);
        DBSPOperator filteredAggregator;
        if (false) {
            DBSPControlledFilterOperator filter =
                    DBSPControlledFilterOperator.create(
                            aggregator.getNode(), source, Monotonicity.getBodyType(expression), limiter);
            this.addOperator(filter);
            filteredAggregator = aggregator.withInputs(Linq.list(filter), false);
        } else {
            filteredAggregator = aggregator.withInputs(Linq.list(source), false);
        }

        // We use the input 1, coming from the integrator
        DBSPOperator limiter2 = this.addBounds(ae.aggregator, 1);
        if (limiter2 == null) {
            this.map(aggregator, filteredAggregator);
            return;
        }

        this.addOperator(filteredAggregator);
        MonotoneExpression monotoneValue2 = this.expansionMonotoneValues.get(ae.aggregator);
        IMaybeMonotoneType projection2 = Monotonicity.getBodyType(monotoneValue2);
        // A second controlled filter for the output of the aggregator
        if (false) {
            DBSPOperator filter2 = DBSPControlledFilterOperator.create(
                    aggregator.getNode(), filteredAggregator, projection2, limiter2);
            this.markBound(aggregator, filter2);
            this.map(aggregator, filter2);
        } else {
            // The before and after filters are actually identical for now.
            DBSPIntegrateTraceRetainKeysOperator before = DBSPIntegrateTraceRetainKeysOperator.create(
                    aggregator.getNode(), source, projection2, limiter2);
            this.addOperator(before);
            // output of 'before' is not used in the graph, but the DBSP Rust layer will use it

            DBSPIntegrateTraceRetainKeysOperator after = DBSPIntegrateTraceRetainKeysOperator.create(
                    aggregator.getNode(), filteredAggregator, projection2, limiter2);
            this.addOperator(after);
            // output of 'after'' is not used in the graph, but the DBSP Rust layer will use it

            this.map(aggregator, filteredAggregator, false);
        }
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) {
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded == null) {
            super.postorder(operator);
            return;
        }

        DBSPOperator source = expanded.replacement.inputs.get(0);
        MonotoneExpression inputValue = this.expansionMonotoneValues.get(source);
        if (inputValue == null) {
            super.postorder(operator);
            return;
        }

        DBSPOperator boundSource = this.bound.get(source);
        if (boundSource == null) {
            super.postorder(operator);
            return;
        }


        // Preserve the field that the data is indexed on from the source
        IMaybeMonotoneType projection = Monotonicity.getBodyType(inputValue);
        PartiallyMonotoneTuple tuple = projection.to(PartiallyMonotoneTuple.class);
        IMaybeMonotoneType tuple0 = tuple.getField(0);
        // Drop field 1 of the value projection.
        if (!tuple0.mayBeMonotone()) {
            super.postorder(operator);
            return;
        }

        DBSPTypeTupleBase varType = projection.getType().to(DBSPTypeTupleBase.class);
        assert varType.size() == 2 : "Expected a pair, got " + varType;
        varType = new DBSPTypeRawTuple(varType.tupFields[0].ref(), varType.tupFields[1].ref());
        DBSPVariablePath var = new DBSPVariablePath("t", varType);
        DBSPExpression body = var.field(0).deref();
        body = this.wrapTypedBox(body, true);
        DBSPClosureExpression closure = body.closure(var.asParameter());
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.errorReporter, operator, projection, true);
        MonotoneExpression monotone = analyzer.applyAnalysis(closure);
        Objects.requireNonNull(monotone);

        DBSPClosureExpression function = monotone.getReducedExpression().to(DBSPClosureExpression.class);
        DBSPOperator bound = new DBSPApplyOperator(operator.getNode(), function,
                function.getFunctionType().resultType, boundSource,
                "(" + operator.getDerivedFrom() + ")");
        this.addOperator(bound);

        this.markBound(expanded.replacement, bound);
        DBSPPartitionedRollingAggregateWithWaterlineOperator replacement =
                new DBSPPartitionedRollingAggregateWithWaterlineOperator(operator.getNode(),
                        operator.partitioningFunction, operator.function, operator.aggregate,
                        operator.window, operator.getOutputIndexedZSetType(), this.mapped(operator.input()), bound);
        this.map(operator, replacement);
    }

    @Override
    public void postorder(DBSPJoinOperator join) {
        DBSPOperator left = this.mapped(join.inputs.get(0));
        DBSPOperator right = this.mapped(join.inputs.get(1));
        OperatorExpansion expanded = this.expandedInto.get(join);
        if (expanded == null) {
            super.postorder(join);
            return;
        }

        JoinExpansion je = expanded.to(JoinExpansion.class);
        DBSPOperator leftLimiter = this.addBounds(je.left, 0);
        DBSPOperator rightLimiter = this.addBounds(je.right, 0);
        if (leftLimiter == null && rightLimiter == null) {
            super.postorder(join);
            return;
        }

        DBSPOperator result = join.withInputs(Linq.list(left, right), false);
        if (leftLimiter != null) {
            MonotoneExpression leftMonotone = this.expansionMonotoneValues.get(je.left);
            // Yes, the limit of the left input is applied to the right one.
            IMaybeMonotoneType leftProjection = Monotonicity.getBodyType(leftMonotone);
            // Check if the "key" field is monotone
            if (leftProjection.to(PartiallyMonotoneTuple.class).getField(0).mayBeMonotone()) {
                DBSPIntegrateTraceRetainKeysOperator r = DBSPIntegrateTraceRetainKeysOperator.create(
                        join.getNode(), right, leftProjection, leftLimiter);
                this.addOperator(r);
            }
        }

        if (rightLimiter != null) {
            MonotoneExpression rightMonotone = this.expansionMonotoneValues.get(je.right);
            // Yes, the limit of the right input is applied to the left one.
            IMaybeMonotoneType rightProjection = Monotonicity.getBodyType(rightMonotone);
            // Check if the "key" field is monotone
            if (rightProjection.to(PartiallyMonotoneTuple.class).getField(0).mayBeMonotone()) {
                DBSPIntegrateTraceRetainKeysOperator l = DBSPIntegrateTraceRetainKeysOperator.create(
                        join.getNode(), left, rightProjection, rightLimiter);
                this.addOperator(l);
            }
        }

        this.map(join, result, true);
    }

    /** Process LATENESS annotations.
     * @return Return the original operator if there aren't any annotations, or
     * the operator that produces the result of the input filtered otherwise. */
    DBSPOperator processLateness(DBSPOperator operator, DBSPOperator expansion) {
        MonotoneExpression expression = this.expansionMonotoneValues.get(expansion);
        if (expression == null) {
            return operator;
        }
        List<DBSPExpression> bounds = new ArrayList<>();
        List<DBSPExpression> minimums = new ArrayList<>();
        int index = 0;
        DBSPVariablePath t = new DBSPVariablePath("t", operator.getOutputZSetType().elementType.ref());
        for (IHasLateness column: operator.to(IHasColumnsMetadata.class).getLateness()) {
            DBSPExpression lateness = column.getLateness();
            if (lateness != null) {
                DBSPExpression field = t.deref().field(index);
                DBSPType type = field.getType();
                field = ExpressionCompiler.makeBinaryExpression(operator.getNode(), field.getType(),
                        DBSPOpcode.SUB, field, lateness);
                bounds.add(field);
                DBSPExpression min = type.to(IsBoundedType.class).getMinValue();
                minimums.add(min);
            }
            index++;
        }
        if (minimums.isEmpty()) {
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
        DBSPTupleExpression bound = new DBSPTupleExpression(bounds, false);
        DBSPParameter parameter = t.asParameter();
        DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(
                operator.getNode(), min.closure(), bound.closure(parameter), replacement);
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
        DBSPOperator replacement = this.processLateness(operator, operator);

        // Process watermark annotations.  Very similar to lateness annotations.
        int index = 0;
        DBSPType dataType = operator.getOutputZSetType().elementType;
        DBSPVariablePath t = new DBSPVariablePath("t", dataType.ref());
        List<DBSPExpression> fields = new ArrayList<>();
        List<DBSPExpression> bounds = new ArrayList<>();
        List<DBSPExpression> minimums = new ArrayList<>();
        for (IHasWatermark column: operator.to(IHasColumnsMetadata.class).getWatermarks()) {
            DBSPExpression lateness = column.getWatermark();

            if (lateness != null) {
                DBSPExpression field = t.deref().field(index);
                fields.add(field);
                DBSPType type = field.getType();
                field = ExpressionCompiler.makeBinaryExpression(operator.getNode(), field.getType(),
                        DBSPOpcode.SUB, field.deepCopy(), lateness);
                bounds.add(field);
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
            this.addOperator(replacement);

            DBSPTupleExpression min = new DBSPTupleExpression(minimums, false);
            DBSPTupleExpression bound = new DBSPTupleExpression(bounds, false);
            DBSPParameter parameter = t.asParameter();
            DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(
                    operator.getNode(), min.closure(), bound.closure(parameter), operator);
            this.addOperator(waterline);

            DBSPVariablePath var = new DBSPVariablePath("t", bound.getType().ref());
            DBSPExpression makePair = new DBSPRawTupleExpression(
                    this.wrapTypedBox(minimums.get(0), false),
                    this.wrapTypedBox(var.deref().field(0), false));
            DBSPApplyOperator apply = new DBSPApplyOperator(
                    operator.getNode(), makePair.closure(var.asParameter()), makePair.getType(), waterline, null);
            this.addOperator(apply);

            // Window requires data to be indexed
            DBSPOperator ix = new DBSPMapIndexOperator(operator.getNode(),
                    new DBSPRawTupleExpression(fields.get(0), t.deref()).closure(t.asParameter()),
                    new DBSPTypeIndexedZSet(operator.getNode(),
                            fields.get(0).getType(), dataType), true, replacement);
            this.addOperator(ix);
            DBSPWindowOperator window = new DBSPWindowOperator(operator.getNode(), ix, apply);
            this.addOperator(window);
            replacement = new DBSPDeindexOperator(operator.getNode(), window);
        }

        if (replacement == operator) {
            this.replace(operator);
        } else {
            this.map(operator, replacement);
        }
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
                return;
            } else {
                this.map(operator, replacement);
                return;
            }
        }
        // Treat like an identity function
        ReplacementExpansion expanded = this.getReplacement(operator);
        if (expanded != null)
            this.addBounds(expanded.replacement, 0);
        super.postorder(operator);
    }
}
