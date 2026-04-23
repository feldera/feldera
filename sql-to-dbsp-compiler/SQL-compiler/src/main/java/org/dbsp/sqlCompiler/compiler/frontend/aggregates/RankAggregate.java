package org.dbsp.sqlCompiler.compiler.frontend.aggregates;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.sql.SqlKind;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRankOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.IntermediateRel;
import org.dbsp.sqlCompiler.ir.expression.DBSPAsymmetricFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEqualityComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;

import java.util.ArrayList;
import java.util.List;

import static org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator.Numbering.*;
import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.INT64;

/** A RANK aggregate.
 * If followed by a filter on the rank it is implemented as a TopK aggregate in DBSP,
 * otherwise it is implemented using a primitive Rank DBSP aggregate. */
public class RankAggregate extends WindowAggregates {
    public final AggregateCall call;

    /**
     * Create a new {@link RankAggregate} aggregate.
     *
     * @param compiler         Compiler.
     * @param window           Window being compiled.
     * @param group            Group within window being compiled.
     * @param windowFieldIndex Index of first field of aggregate within window.
     *                         The list aggregateCalls contains aggregates starting at this index.
     */
    RankAggregate(CalciteToDBSPCompiler compiler, Window window, Window.Group group,
                  int windowFieldIndex, AggregateCall call) {
        super(compiler, window, group, windowFieldIndex);
        this.call = call;
    }

    @Override
    public DBSPSimpleOperator implement(DBSPSimpleOperator input, DBSPSimpleOperator lastOperator, boolean isLast) {
        SqlKind kind = this.call.getAggregation().kind;
        IntermediateRel node = CalciteObject.create(window, new SourcePositionRange(this.call.getParserPosition()));
        DBSPIndexedTopKOperator.Numbering numbering = switch (kind) {
            case RANK -> RANK;
            case DENSE_RANK -> DENSE_RANK;
            // case ROW_NUMBER -> ROW_NUMBER;
            default -> throw new UnimplementedException(
                    "Ranking function " + kind + " not yet implemented in a WINDOW aggregate",
                    node);
        };

        OutputPort inputIndex = this.indexInput(lastOperator);

        // Generate comparison function for sorting the vector
        DBSPType inputRowType = lastOperator.getOutputZSetElementType();
        DBSPComparatorExpression comparator = CalciteToDBSPCompiler.generateComparator(
                node, this.group.orderKeys.getFieldCollations(), inputRowType, false);

        // The rank must be added at the end of the input collection (that's how Calcite expects it).
        DBSPVariablePath left = DBSPTypeInteger.getType(node, INT64, false).var();
        DBSPVariablePath right = inputRowType.ref().var();
        List<DBSPExpression> flattened = DBSPTypeTupleBase.flatten(right.deref());
        flattened.add(left);
        DBSPTupleExpression tuple = new DBSPTupleExpression(flattened, false);
        DBSPClosureExpression outputProducer = tuple.closure(left, right);

        // Since Rank is always incremental we have to wrap it into a D-I pair
        DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, inputIndex);
        this.compiler.addOperator(diff);

        DBSPVariablePath o = inputRowType.ref().var();
        List<DBSPExpression> orderFields = new ArrayList<>();
        for (RelFieldCollation col: this.group.orderKeys.getFieldCollations()) {
            orderFields.add(o.deref().field(col.getFieldIndex()));
        }

        DBSPExpression projectionTuple = new DBSPTupleExpression(node, orderFields);
        DBSPClosureExpression projectionFunc = projectionTuple.closure(o);

        List<DBSPAsymmetricFieldComparatorExpression.Collation> collations = new ArrayList<>();
        int index = 0;
        for (RelFieldCollation col: this.group.orderKeys.getFieldCollations()) {
            var collation = new DBSPAsymmetricFieldComparatorExpression.Collation(
                    col.getFieldIndex(), index,
                    CalciteToDBSPCompiler.ascending(col), !CalciteToDBSPCompiler.nullsLast(col));
            collations.add(collation);
            index++;
        }
        DBSPAsymmetricFieldComparatorExpression rankCmpFunc =
                new DBSPAsymmetricFieldComparatorExpression(node, inputRowType, projectionTuple.type, collations);

        DBSPRankOperator topK = new DBSPRankOperator(
                node, numbering, comparator, rankCmpFunc,
                projectionFunc, outputProducer, diff.outputPort());
        this.compiler.addOperator(topK);
        DBSPIntegrateOperator integral = new DBSPIntegrateOperator(node, topK.outputPort());
        this.compiler.addOperator(integral);
        // We must drop the index we built.
        return new DBSPDeindexOperator(node.maybeFinal(isLast), node, integral.outputPort());
    }

    // Implement RankAggregate followed by limit as TopK
    public DBSPSimpleOperator implementAsTopK(int limit, DBSPSimpleOperator lastOperator, boolean isLast) {
        SqlKind kind = this.call.getAggregation().kind;
        IntermediateRel node = CalciteObject.create(window, new SourcePositionRange(this.call.getParserPosition()));
        DBSPIndexedTopKOperator.Numbering numbering = switch (kind) {
            case RANK -> RANK;
            case DENSE_RANK -> DENSE_RANK;
            case ROW_NUMBER -> ROW_NUMBER;
            default -> throw new UnimplementedException(
                    "Ranking function " + kind + " not yet implemented in a WINDOW aggregate",
                    node);
        };

        OutputPort inputIndex = this.indexInput(lastOperator);

        // Generate comparison function for sorting the vector
        DBSPType inputRowType = lastOperator.getOutputZSetElementType();
        DBSPComparatorExpression comparator = CalciteToDBSPCompiler.generateComparator(
                node, this.group.orderKeys.getFieldCollations(), inputRowType, false);

        // The rank must be added at the end of the input collection (that's how Calcite expects it).
        DBSPVariablePath left = DBSPTypeInteger.getType(node, INT64, false).var();
        DBSPVariablePath right = inputRowType.ref().var();
        List<DBSPExpression> flattened = DBSPTypeTupleBase.flatten(right.deref());
        flattened.add(left);
        DBSPTupleExpression tuple = new DBSPTupleExpression(flattened, false);
        DBSPClosureExpression outputProducer = tuple.closure(left, right);

        // TopK operator.
        // Since TopK is always incremental we have to wrap it into a D-I pair
        DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, inputIndex);
        this.compiler.addOperator(diff);
        DBSPUSizeLiteral limitValue = new DBSPUSizeLiteral(limit);
        DBSPEqualityComparatorExpression eq = new DBSPEqualityComparatorExpression(node, comparator);
        DBSPIndexedTopKOperator topK = new DBSPIndexedTopKOperator(
                node, numbering, comparator, limitValue, eq, outputProducer, diff.outputPort());
        this.compiler.addOperator(topK);
        DBSPIntegrateOperator integral = new DBSPIntegrateOperator(node, topK.outputPort());
        this.compiler.addOperator(integral);
        // We must drop the index we built.
        return new DBSPDeindexOperator(node.maybeFinal(isLast), node, integral.outputPort());
    }

    @Override
    public boolean isCompatible(AggregateCall call) {
        return false;
    }
}
