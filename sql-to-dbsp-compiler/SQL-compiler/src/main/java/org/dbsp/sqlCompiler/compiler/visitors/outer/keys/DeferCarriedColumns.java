package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.LeftJoinChainsVisitor.Chain;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.CollectionShape;
import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;
import org.dbsp.sqlCompiler.ir.type.CollectionShape.Part;
import org.dbsp.sqlCompiler.ir.type.IndexedShape;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** A chain of left joins applied to a collection may "carry" a set of columns from the
 * input collection without modifying them.  In this case state can be reduced
 * by having the chain only carry the columns that some operator of the chain
 * reads, and adding an extra join afterwards to "stick" back the unused columns.
 * The new join will have the output of the chain as left input and the
 * original chain input as the right input.
 * This can be done as long as
 * - the collection the chain starts from has a key, as found by {@link KeyAnalysis}
 * - the key is copied to the output as well
 */
public class DeferCarriedColumns extends CircuitCloneVisitor {
    /** For each chain output where a lookup belongs, the chain it puts the columns back for. */
    final Map<OutputPort, Chain> chains;

    public DeferCarriedColumns(DBSPCompiler compiler, Map<OutputPort, Chain> chains) {
        super(compiler, false);
        this.chains = chains;
    }

    @Override
    public void postorder(DBSPIntegrateOperator endNode) {
        // Every chain the analysis hands over ends at an integrator
        Chain chain = this.chains.get(endNode.outputPort());
        if (chain == null) {
            super.postorder(endNode);
            return;
        }
        CollectionShape resultShape = endNode.outputPort().getShape();
        CollectionShape startShape = chain.start().getShape();
        // When the start operator is indexed by the key, use it as an input for the new join we are inserting
        boolean reuseStart = chain.startIsIndexedByKey();
        List<Column> key = reuseStart ? chain.keyInIndexOrder() : chain.keyColumns();
        List<Column> deferred = new ArrayList<>(chain.deferrable());

        Utilities.enforce(key != null && !deferred.isEmpty());
        DBSPVariablePath outputVar = resultShape.rowVariable(endNode.outputPort().outputType());
        List<Column> resultColumns = new ArrayList<>(resultShape.columns());
        List<DBSPExpression> carried = new ArrayList<>();
        for (Column column : resultColumns)
            carried.add(resultShape.field(outputVar, column));

        OutputPort delta = this.mapped(endNode.input());
        DBSPSimpleOperator integrate = endNode.withInputs(List.of(delta), false).to(DBSPSimpleOperator.class);
        this.addOperator(integrate);
        OutputPort start = this.mapped(chain.start());
        List<DBSPExpression> rowKeyFields = new ArrayList<>();
        final OutputPort rightInput;
        if (reuseStart) {
            // Key the result rows by the start's key type
            DBSPTypeTupleBase startKeyType = chain.start().getOutputIndexedZSetType().getKeyTypeTuple();
            for (int i = 0; i < key.size(); i++) {
                Column inResult = chain.inputToOutputRemap().get(key.get(i));
                DBSPExpression outputField = resultShape.field(outputVar, inResult);
                rowKeyFields.add(outputField.nullabilityCast(startKeyType.getFieldType(i), DBSPCastExpression.CastType.SqlUnsafe));
            }
            rightInput = start;
        } else {
            // Index the start by the key, keeping only the deferred columns
            DBSPVariablePath sourceVar = startShape.rowVariable(chain.start().outputType());
            List<DBSPExpression> sourceKeyFields = new ArrayList<>();
            for (Column column : key) {
                Column inResult = chain.inputToOutputRemap().get(column);
                DBSPExpression outputField = resultShape.field(outputVar, inResult);
                DBSPExpression sourceField = startShape.field(sourceVar, column);
                // The key has the start's nullability on both sides
                DBSPType commonType = outputField.getType().withMayBeNull(sourceField.getType().mayBeNull);
                rowKeyFields.add(outputField.nullabilityCast(commonType, DBSPCastExpression.CastType.SqlUnsafe));
                sourceKeyFields.add(sourceField.nullabilityCast(commonType, DBSPCastExpression.CastType.SqlUnsafe));
            }
            List<DBSPExpression> sourceValue = new ArrayList<>();
            for (Column column : deferred) {
                // A deferred column takes the type of the result column it fills
                Column inResult = chain.inputToOutputRemap().get(column);
                DBSPType resultType = carried.get(resultColumns.indexOf(inResult)).getType();
                DBSPExpression sourceField = startShape.field(sourceVar, column);
                sourceValue.add(sourceField.nullabilityCast(resultType, DBSPCastExpression.CastType.SqlUnsafe));
            }
            DBSPExpression reindexBody = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(sourceKeyFields, false),
                    new DBSPTupleExpression(sourceValue, false));
            DBSPSimpleOperator joinIndex = new DBSPMapIndexOperator(
                    endNode.getRelNode(), reindexBody.closure(sourceVar), start);
            this.addOperator(joinIndex);
            rightInput = joinIndex.outputPort();
        }
        DBSPExpression rowByKey = new DBSPRawTupleExpression(
                new DBSPTupleExpression(rowKeyFields, false), new DBSPTupleExpression(carried, false));
        DBSPSimpleOperator indexed = new DBSPMapIndexOperator(
                endNode.getRelNode(), rowByKey.closure(outputVar), integrate.outputPort());
        this.addOperator(indexed);
        OutputPort leftInput = indexed.outputPort();

        // Build the join function
        DBSPVariablePath keyVar = indexed.getOutputIndexedZSetType().keyType.ref().var();
        DBSPVariablePath leftVar = indexed.getOutputIndexedZSetType().elementType.ref().var();
        DBSPVariablePath rightVar = rightInput.getOutputIndexedZSetType().elementType.ref().var();
        List<DBSPExpression> joinIndexFields = new ArrayList<>();
        List<DBSPExpression> joinOutputFields = new ArrayList<>();
        for (int i = 0; i < resultColumns.size(); i++) {
            Column fromStart = chain.outputToInputRemap().get(resultColumns.get(i));
            int from = deferred.indexOf(fromStart);
            DBSPExpression field;
            if (from < 0) {
                field = leftVar.deref().field(i).applyCloneIfNeeded();
            } else if (reuseStart) {
                DBSPExpression startField = rightVar.deref().field(fromStart.field()).applyCloneIfNeeded();
                field = startField.nullabilityCast(carried.get(i).getType(), DBSPCastExpression.CastType.SqlUnsafe);
            } else {
                field = rightVar.deref().field(from).applyCloneIfNeeded();
            }
            if (resultColumns.get(i).part() == Part.INDEX)
                joinIndexFields.add(field);
            else
                joinOutputFields.add(field);
        }
        DBSPSimpleOperator join;
        if (resultShape instanceof IndexedShape) {
            DBSPExpression body = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(joinIndexFields, false),
                    new DBSPTupleExpression(joinOutputFields, false));
            join = new DBSPStreamJoinIndexOperator(endNode.getRelNode(),
                    endNode.outputPort().getOutputIndexedZSetType(),
                    body.closure(keyVar, leftVar, rightVar), endNode.isMultiset,
                    leftInput, rightInput, false);
        } else {
            DBSPExpression body = new DBSPTupleExpression(joinOutputFields, false);
            join = new DBSPStreamJoinOperator(endNode.getRelNode(),
                    endNode.outputPort().getOutputZSetType(),
                    body.closure(keyVar, leftVar, rightVar), endNode.isMultiset,
                    leftInput, rightInput, false);
        }
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Deferred ")
                .append(deferred.size())
                .append(" columns across ")
                .append(chain.length())
                .append(" left joins")
                .append(reuseStart ? ", reusing the start's index" : "")
                .append("; the result has ")
                .append(resultShape.width())
                .append(" columns, ")
                .append(key.size())
                .append(" in the key")
                .newline();
        this.map(endNode.outputPort(), join.outputPort());
    }
}
