package org.dbsp.sqlCompiler.compiler.frontend.aggregates;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.IntermediateRel;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.ICastable;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for window processing.
 * Calcite can sometimes use the same group for window computations
 * that we cannot perform in one operator, so we
 * divide some group/window combinations into multiple
 * combinations.
 */
public abstract class WindowAggregates implements ICastable {
    final CalciteToDBSPCompiler compiler;
    final IntermediateRel node;
    final Window window;
    final Window.Group group;
    final List<AggregateCall> aggregateCalls;
    public final int windowFieldIndex;
    final DBSPTypeTuple windowResultType;
    final DBSPTypeTuple inputRowType;
    final List<Integer> partitionKeys;
    final DBSPVariablePath inputRowRefVar;
    final ExpressionCompiler eComp;

    /**
     * Create a new window aggregate.
     *
     * @param compiler         Compiler.
     * @param window           Window being compiled.
     * @param group            Group within window being compiled.
     * @param windowFieldIndex Index of first field of aggregate within window.
     *                         The list aggregateCalls contains aggregates starting at this index.
     */
    WindowAggregates(CalciteToDBSPCompiler compiler, Window window, Window.Group group, int windowFieldIndex) {
        this.node = CalciteObject.create(window);
        this.compiler = compiler;
        this.window = window;
        this.group = group;
        this.aggregateCalls = new ArrayList<>();
        this.windowFieldIndex = windowFieldIndex;
        this.windowResultType = this.compiler.convertType(node.getPositionRange(),
                window.getRowType(), false).to(DBSPTypeTuple.class);
        this.inputRowType = this.compiler.convertType(node.getPositionRange(),
                window.getInput().getRowType(), false).to(DBSPTypeTuple.class);
        this.partitionKeys = this.group.keys.toList();
        this.inputRowRefVar = this.inputRowType.ref().var();
        this.eComp = new ExpressionCompiler(window, this.inputRowRefVar, window.constants, this.compiler.compiler());
    }

    public CalciteObject getNode() {
        return CalciteObject.create(this.window);
    }

    OutputPort indexInput(DBSPSimpleOperator lastOperator) {
        if (!lastOperator.is(DBSPDeindexOperator.class)) {
            DBSPType inputRowType = lastOperator.getOutputZSetElementType();
            DBSPVariablePath firstInputVar = inputRowType.ref().var();
            List<DBSPExpression> expressions = Linq.map(this.partitionKeys,
                    f -> firstInputVar.deref().field(f).applyCloneIfNeeded());
            DBSPTupleExpression partition = new DBSPTupleExpression(this.node, expressions);
            DBSPExpression row = DBSPTupleExpression.flatten(firstInputVar.deref());
            DBSPExpression mapExpr = new DBSPRawTupleExpression(partition, row);
            DBSPClosureExpression mapClo = mapExpr.closure(firstInputVar);
            DBSPSimpleOperator index = new DBSPMapIndexOperator(this.node, mapClo,
                    TypeCompiler.makeIndexedZSet(
                            partition.getType(), row.getType()), lastOperator.outputPort());
            this.compiler.addOperator(index);
            return index.outputPort();
        } else {
            // avoid a deindex->index chain which does nothing
            return lastOperator.inputs.get(0);
        }
    }

    public abstract DBSPSimpleOperator implement(DBSPSimpleOperator input, DBSPSimpleOperator lastOperator, boolean isLast);

    public void addAggregate(AggregateCall call) {
        this.aggregateCalls.add(call);
    }

    public DBSPTupleExpression partitionKeys() {
        List<DBSPExpression> expressions = Linq.map(this.partitionKeys,
                f -> this.inputRowRefVar.deref().field(f).applyCloneIfNeeded());
        return new DBSPTupleExpression(node, expressions);
    }

    public abstract boolean isCompatible(AggregateCall call);

    static boolean isUnbounded(Window.Group group) {
        return group.lowerBound.isUnboundedPreceding() && group.upperBound.isUnboundedFollowing();
    }

    public static WindowAggregates newGroup(CalciteToDBSPCompiler compiler, Window window, Window.Group group,
                                            int windowFieldIndex, AggregateCall call) {
        WindowAggregates result = switch (call.getAggregation().getKind()) {
            case RANK, DENSE_RANK, ROW_NUMBER -> new RankAggregate(
                    compiler, window, group, windowFieldIndex, call);
            case FIRST_VALUE, LAST_VALUE -> new FirstLastAggregate(
                    compiler, window, group, windowFieldIndex, call);
            case LAG, LEAD -> {
                String agg = Utilities.singleQuote(call.getAggregation().getKind().toString());
                if (group.isRows)
                    throw new UnimplementedException(agg + " with ROWS not yet implemented",
                            457, CalciteObject.create(window));
                if (call.ignoreNulls())
                    throw new UnimplementedException(agg + " with IGNORE NULLS not yet implemented",
                            CalciteObject.create(window));
                yield new LeadLagAggregates(compiler, window, group, windowFieldIndex);
            }
            default -> (isUnbounded(group) && group.orderKeys.getFieldCollations().isEmpty()) ?
                    new SimpleAggregates(compiler, window, group, windowFieldIndex) :
                    new RangeAggregates(compiler, window, group, windowFieldIndex);
        };
        result.addAggregate(call);
        return result;
    }

    @Override
    public String toString() {
        return this.aggregateCalls.toString();
    }
}
