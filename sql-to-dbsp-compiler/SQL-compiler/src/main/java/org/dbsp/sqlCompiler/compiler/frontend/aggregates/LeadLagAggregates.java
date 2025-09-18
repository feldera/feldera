package org.dbsp.sqlCompiler.compiler.frontend.aggregates;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlKind;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A class representing a LEAD or LAG function
 */
public class LeadLagAggregates extends WindowAggregates {
    protected LeadLagAggregates(CalciteToDBSPCompiler compiler, Window window,
                                Window.Group group, int windowFieldIndex) {
        super(compiler, window, group, windowFieldIndex);
        if (group.exclude != RexWindowExclusion.EXCLUDE_NO_OTHER)
            throw new UnimplementedException("EXCLUDE in OVER", 457, node);
    }

    @Override
    public DBSPSimpleOperator implement(
            DBSPSimpleOperator unusedInput, DBSPSimpleOperator lastOperator, boolean isLast) {
        // All the aggregate calls have the same arguments by construction
        AggregateCall lastCall = Utilities.last(this.aggregateCalls);
        SqlKind kind = lastCall.getAggregation().getKind();
        int offset = kind == SqlKind.LEAD ? -1 : +1;
        OutputPort inputIndexed = this.indexInput(lastOperator);

        // This operator is always incremental, so create the non-incremental version
        // of it by adding a Differentiator and an Integrator around it.
        DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(this.node, inputIndexed);
        this.compiler.addOperator(diff);

        DBSPType inputRowType = lastOperator.getOutputZSetElementType();
        DBSPVariablePath inputVar = inputRowType.ref().var();
        DBSPExpression row = DBSPTupleExpression.flatten(
                inputVar.deref().applyClone());
        DBSPComparatorExpression comparator = CalciteToDBSPCompiler.generateComparator(
                this.node, this.group.orderKeys.getFieldCollations(), row.getType(), false);

        // Lag argument calls
        List<Integer> operands = lastCall.getArgList();
        if (operands.size() > 1) {
            int amountIndex = operands.get(1);
            RexInputRef ri = new RexInputRef(
                    amountIndex, this.window.getRowType().getFieldList().get(amountIndex).getType());
            DBSPExpression amount = this.eComp.compile(ri);
            if (!amount.is(DBSPI32Literal.class)) {
                throw new UnimplementedException("Currently LAG/LEAD amount must be a compile-time constant", 457, node);
            }
            Utilities.enforce(amount.is(DBSPI32Literal.class));
            offset *= Objects.requireNonNull(amount.to(DBSPI32Literal.class).value);
        }

        // Lag has this signature
        //     pub fn lag_custom_order<VL, OV, PF, CF, OF>(
        //        &self,
        //        offset: isize,
        //        project: PF,
        //        output: OF,
        //    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
        //    where
        //        VL: DBData,
        //        OV: DBData,
        //        CF: CmpFunc<V>,
        //        PF: Fn(Option<&V>) -> VL + 'static,
        //        OF: Fn(&V, &VL) -> OV + 'static,
        // Notice that the project function takes an Option<&V>.
        List<Integer> lagColumns = Linq.map(this.aggregateCalls, call -> call.getArgList().get(0));
        DBSPVariablePath var = new DBSPVariablePath(inputRowType.ref().withMayBeNull(true));
        List<DBSPExpression> lagColumnExpressions = new ArrayList<>();
        for (int i = 0; i < lagColumns.size(); i++) {
            int field = lagColumns.get(i);
            DBSPExpression expression = var.unwrap()
                    .deref()
                    .field(field)
                    // cast the results to whatever Calcite says they will be.
                    .applyCloneIfNeeded()
                    .cast(this.node, this.windowResultType.getFieldType(this.windowFieldIndex + i), false);
            lagColumnExpressions.add(expression);
        }
        DBSPTupleExpression lagTuple = new DBSPTupleExpression(
                lagColumnExpressions, false);
        DBSPExpression[] defaultValues = new DBSPExpression[lagTuple.size()];
        // Default value is NULL, or may be specified explicitly
        int i = 0;
        for (AggregateCall call : this.aggregateCalls) {
            List<Integer> args = call.getArgList();
            Utilities.enforce(lagTuple.fields != null);
            DBSPType resultType = lagTuple.fields[i].getType();
            if (args.size() > 2) {
                int defaultIndex = args.get(2);
                RexInputRef ri = new RexInputRef(defaultIndex,
                        // Same type as field i
                        window.getRowType().getFieldList().get(i).getType());
                // a default argument is present
                defaultValues[i] = eComp.compile(ri).cast(this.node, resultType, false);
            } else {
                defaultValues[i] = resultType.none();
            }
            i++;
        }
        // All fields of none are NULL
        DBSPExpression none = new DBSPTupleExpression(defaultValues);
        DBSPExpression conditional = new DBSPIfExpression(this.node,
                var.is_null(), none, lagTuple);

        DBSPExpression projection = conditional.closure(var);

        DBSPVariablePath origRow = new DBSPVariablePath(inputRowType.ref());
        DBSPVariablePath delayedRow = new DBSPVariablePath(lagTuple.getType().ref());
        DBSPExpression functionBody = DBSPTupleExpression.flatten(origRow.deref(), delayedRow.deref());
        DBSPExpression function = functionBody.closure(origRow, delayedRow);

        DBSPLagOperator lag = new DBSPLagOperator(
                node, offset, projection, function, comparator,
                TypeCompiler.makeIndexedZSet(
                        diff.getOutputIndexedZSetType().keyType, functionBody.getType()), diff.outputPort());
        this.compiler.addOperator(lag);

        DBSPIntegrateOperator integral = new DBSPIntegrateOperator(node, lag.outputPort());
        this.compiler.addOperator(integral);
        return new DBSPDeindexOperator(node.maybeFinal(isLast), integral.outputPort());
    }

    @Override
    public boolean isCompatible(AggregateCall call) {
        Utilities.enforce(!this.aggregateCalls.isEmpty());
        AggregateCall lastCall = Utilities.last(this.aggregateCalls);
        SqlKind kind = call.getAggregation().getKind();
        if (lastCall.getAggregation().getKind() != kind)
            return false;
        // A call is compatible if the first 2 arguments are the same
        List<Integer> args = call.getArgList();
        List<Integer> lastArgs = lastCall.getArgList();
        if (!Objects.equals(args.get(0), lastArgs.get(0)))
            return false;
        Integer arg1 = null;
        Integer lastArg1 = null;
        if (args.size() > 1)
            arg1 = args.get(1);
        if (lastArgs.size() > 1)
            lastArg1 = lastArgs.get(1);
        return Objects.equals(arg1, lastArg1);
        // Argument #3 may be different
    }
}
