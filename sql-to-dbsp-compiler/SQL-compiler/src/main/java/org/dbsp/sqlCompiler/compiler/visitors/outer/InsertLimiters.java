package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPControlledFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneValue;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.ValueProjection;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.AggregateExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.OperatorExpansion;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** As a result of the MonotoneOperators analysis, this pass inserts ControlledFilter
 * operators to throw away tuples that are not "useful", and some apply
 * operators that compute the bounds that drive the controlled filters. */
public class InsertLimiters extends CircuitCloneVisitor {
    /** For each operator in the expansion of the operators of this circuit
     * the list of its monotone output columns */
    public final Map<DBSPOperator, MonotoneValue> expansionMonotoneValues;
    /** Circuit that contains the expansion of the circuit we are modifying */
    public final DBSPCircuit expandedCircuit;
    /** Maps each original operator to the set of operators it was expanded to */
    public final Map<DBSPOperator, OperatorExpansion> expandedInto;
    /** Maps each operator to the one that computes its lower bound.
     * The keys in this map can be both operators from this circuit and from
     * the expanded circuit. */
    public final Map<DBSPOperator, DBSPOperator> bound;

    static final boolean useControlledFilters = false;

    public InsertLimiters(IErrorReporter reporter,
                          DBSPCircuit expandedCircuit,
                          Map<DBSPOperator, MonotoneValue> expansionMonotoneValues,
                          Map<DBSPOperator, OperatorExpansion> expandedInto) {
        super(reporter, false);
        this.expandedCircuit = expandedCircuit;
        this.expansionMonotoneValues = expansionMonotoneValues;
        this.expandedInto = expandedInto;
        this.bound = new HashMap<>();
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
        MonotoneValue monotone = this.expansionMonotoneValues.get(operatorFromExpansion);
        if (monotone == null)
            return null;
        DBSPOperator source = operatorFromExpansion.inputs.get(input);  // Even for binary operators
        DBSPOperator boundSource = Utilities.getExists(this.bound, source);
        DBSPClosureExpression function = monotone.getExpression().to(DBSPClosureExpression.class);
        DBSPOperator bound = new DBSPApplyOperator(operatorFromExpansion.getNode(), function,
                function.getFunctionType().resultType, boundSource);
        Logger.INSTANCE.belowLevel(this, 2)
                .append("Bound for " + operatorFromExpansion + " is " + bound)
                .newline();
        this.getResult().addOperator(bound);  // insert directly into circuit
        Utilities.putNew(this.bound, operatorFromExpansion, bound);
        return bound;
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        this.addBounds(operator, 0);
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPIndexOperator operator) {
        this.addBounds(operator, 0);
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

        MonotoneValue monotoneValue = this.expansionMonotoneValues.get(ae.integrator);
        ValueProjection projection = monotoneValue.getProjection();
        DBSPOperator filteredAggregator;
        if (useControlledFilters) {
            DBSPControlledFilterOperator filter =
                    DBSPControlledFilterOperator.create(aggregator.getNode(), source, projection, limiter);
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
        MonotoneValue monotoneValue2 = this.expansionMonotoneValues.get(ae.aggregator);
        ValueProjection projection2 = monotoneValue2.getProjection();
        // A second controlled filter for the output of the aggregator
        if (useControlledFilters) {
            DBSPOperator filter2 = DBSPControlledFilterOperator.create(
                    aggregator.getNode(), filteredAggregator, projection2, limiter2);
            Utilities.putNew(this.bound, aggregator, filter2);
            this.map(aggregator, filter2);
        } else {
            // The before and after filters are actually identical for now.
            DBSPIntegrateTraceRetainKeysOperator before = DBSPIntegrateTraceRetainKeysOperator.create(
                    aggregator.getNode(), source, projection2, limiter2);
            this.addOperator(before);
            // output of 'before' is never used

            DBSPIntegrateTraceRetainKeysOperator after = DBSPIntegrateTraceRetainKeysOperator.create(
                    aggregator.getNode(), filteredAggregator, projection2, limiter2);
            this.addOperator(after);
            // output of 'after'' is never used

            this.map(aggregator, filteredAggregator, false);
        }
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
        MonotoneValue monotoneValue = this.expansionMonotoneValues.get(operator);
        if (monotoneValue == null) {
            super.postorder(operator);
            return;
        }
        List<DBSPExpression> minimums = new ArrayList<>();
        List<DBSPExpression> zeros = new ArrayList<>();
        int index = 0;
        DBSPVariablePath t = new DBSPVariablePath("t", operator.getOutputZSetType().elementType.ref());
        for (InputColumnMetadata column: operator.metadata.getColumns()) {
            if (column.lateness != null) {
                DBSPExpression field = t.deref().field(index);
                DBSPType type = field.getType();
                field = new DBSPBinaryExpression(operator.getNode(), field.getType(),
                        DBSPOpcode.SUB, field, column.lateness);
                minimums.add(field);
                if (!type.is(IsNumericType.class)) {
                    throw new CompilationError("Column " + column.name + " has a type " + type +
                            " which does not support lateness", column.getNode());
                }
                DBSPExpression zero = type.to(IsNumericType.class).getZero();
                zeros.add(zero);
            }
            index++;
        }
        if (zeros.isEmpty()) {
            this.replace(operator);
            return;
        }

        // The waterline operator will compute the *minimum legal value* of all the
        // inputs that have a lateness attached.  The output signature contains only
        // the columns that have lateness.
        this.addOperator(operator);
        DBSPTupleExpression zero = new DBSPTupleExpression(zeros, false);
        DBSPTupleExpression min = new DBSPTupleExpression(minimums, false);
        DBSPType outputType = min.getType();
        DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(
                operator.getNode(), zero.closure(), min.closure(t.asParameter()), outputType, operator);
        this.addOperator(waterline);

        DBSPControlledFilterOperator filter = DBSPControlledFilterOperator.create(
                operator.getNode(), operator, monotoneValue.getProjection(), waterline);
        Utilities.putNew(this.bound, operator, waterline);
        this.map(operator, filter);
    }
}
