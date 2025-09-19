package org.dbsp.sqlCompiler.compiler.frontend.aggregates;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.sql.SqlKind;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.IntermediateRel;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEqualityComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.List;
import java.util.Objects;

import static org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator.TopKNumbering.ROW_NUMBER;
import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.INT64;

/**
 * FIRST_VALUE or LAST_VALUE aggregates
 */
public class FirstLastAggregate extends WindowAggregates {
    public final AggregateCall call;

    protected FirstLastAggregate(
            CalciteToDBSPCompiler compiler, Window window, Window.Group group,
            int windowFieldIndex, AggregateCall call) {
        super(compiler, window, group, windowFieldIndex);
        this.call = call;
        String agg = call.getAggregation().getKind().toString();
        boolean canImplement = false;
        // If the range is unbounded, we can do it
        if (isUnbounded(group)) {
            canImplement = true;
        } else if (call.getAggregation().kind == SqlKind.FIRST_VALUE) {
            if (group.lowerBound.isUnboundedPreceding() &&
                    group.upperBound.isCurrentRow()) {
                // These combinations are equivalent to unbounded with RANGE or ROWS
                canImplement = true;
            }
        } else if (call.getAggregation().kind == SqlKind.LAST_VALUE) {
            if (group.upperBound.isUnboundedFollowing() &&
                    group.lowerBound.isCurrentRow()) {
                // These combinations are equivalent to unbounded with RANGE or ROWS
                canImplement = true;
            }
        }
        if (!canImplement)
            throw new UnimplementedException(agg + " with bounded range",
                    CalciteObject.create(window));
        if (group.orderKeys.getFieldCollations().isEmpty()) {
            this.compiler.compiler().reportWarning(
                    new SourcePositionRange(call.getParserPosition()),
                    "Underspecified aggregate",
                    this.getKind() + " should be used with ORDER BY to produce a deterministic result");
        }
    }

    public final SqlKind getKind() {
        return this.call.getAggregation().kind;
    }

    @Override
    public boolean isCompatible(AggregateCall call) {
        return false;
    }

    DBSPSimpleOperator createTopK() {
        // A specialized version of the function generateNestedTopK.
        // That function generates a row with the rank on the last position, but
        // this function generates directly the aggregated value
        IntermediateRel node = CalciteObject.create(this.window);
        DBSPSimpleOperator index = this.compiler.indexWindow(this.window, this.group);
        RelNode input = this.window.getInput();
        DBSPType inputRowType = this.compiler.convertType(this.node.getPositionRange(), input.getRowType(), false);

        // Generate comparison function for sorting the vector
        boolean reverse = this.getKind() == SqlKind.LAST_VALUE;
        DBSPComparatorExpression comparator = CalciteToDBSPCompiler.generateComparator(
                node, this.group.orderKeys.getFieldCollations(), inputRowType, reverse);

        // TopK expects a function (index, row) -> row; the index is ignored in this implementation
        DBSPVariablePath ignored = DBSPTypeInteger.getType(node, INT64, false).var();
        DBSPVariablePath right = inputRowType.ref().var();

        // The field to aggregate is the single output we produce
        List<Integer> args = this.call.getArgList();
        Utilities.enforce(args.size() == 1);
        int fieldIndex = args.get(0);
        if (this.call.ignoreNulls() && inputRowType.to(DBSPTypeTuple.class).getFieldType(fieldIndex).mayBeNull) {
            String agg = call.getAggregation().getKind().toString();
            throw new UnimplementedException(agg + " with IGNORE NULLS ",
                    CalciteObject.create(window));
        }
        DBSPTupleExpression tuple = new DBSPTupleExpression(right.deref().field(fieldIndex).applyCloneIfNeeded());
        DBSPClosureExpression outputProducer = tuple.closure(ignored, right);

        // TopK operator.
        // Since TopK is always incremental we have to wrap it into a D-I pair
        DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, index.outputPort());
        this.compiler.addOperator(diff);
        DBSPUSizeLiteral limitValue = new DBSPUSizeLiteral(1);
        DBSPEqualityComparatorExpression eq = new DBSPEqualityComparatorExpression(node, comparator);
        DBSPIndexedTopKOperator topK = new DBSPIndexedTopKOperator(
                node, ROW_NUMBER, comparator, limitValue, eq, outputProducer, diff.outputPort());
        this.compiler.addOperator(topK);
        DBSPIntegrateOperator integral = new DBSPIntegrateOperator(node, topK.outputPort());
        this.compiler.addOperator(integral);
        return integral;
    }

    @Override
    public DBSPSimpleOperator implement(DBSPSimpleOperator unusedInput, DBSPSimpleOperator lastOperator, boolean isLast) {
        // Similar to SimpleAggregates.implement
        DBSPType groupKeyType = this.partitionKeys().getType();
        DBSPType inputType = lastOperator.getOutputZSetElementType();

        // Index the previous input using the group keys
        DBSPTypeIndexedZSet localGroupAndInput = TypeCompiler.makeIndexedZSet(groupKeyType, inputType);
        DBSPVariablePath rowVar = inputType.ref().var();
        DBSPExpression[] expressions = new DBSPExpression[]{rowVar.deref()};
        DBSPTupleExpression flattened = DBSPTupleExpression.flatten(expressions);
        DBSPClosureExpression makeKeys =
                new DBSPRawTupleExpression(
                        new DBSPTupleExpression(
                                Linq.map(this.partitionKeys,
                                        p -> rowVar.deref().field(p).applyCloneIfNeeded()), false),
                        new DBSPTupleExpression(this.node,
                                lastOperator.getOutputZSetElementType().to(DBSPTypeTuple.class),
                                Objects.requireNonNull(flattened.fields)))
                        .closure(rowVar);
        DBSPSimpleOperator indexedInput = new DBSPMapIndexOperator(
                node, makeKeys, localGroupAndInput, lastOperator.outputPort());
        this.compiler.addOperator(indexedInput);

        DBSPSimpleOperator topK = this.createTopK();

        // Join with the indexed input
        DBSPVariablePath key = groupKeyType.ref().var();
        DBSPVariablePath left = flattened.getType().ref().var();
        DBSPVariablePath right = topK.getOutputIndexedZSetType().elementType.ref().var();
        DBSPClosureExpression append =
                DBSPTupleExpression.flatten(left.deref(), right.deref()).closure(
                        key, left, right);
        CalciteRelNode n = this.node;
        // Do not insert the last operator
        return new DBSPStreamJoinOperator(this.node.maybeFinal(isLast), TypeCompiler.makeZSet(append.getResultType()),
                append, true, indexedInput.outputPort(), topK.outputPort());
    }
}
