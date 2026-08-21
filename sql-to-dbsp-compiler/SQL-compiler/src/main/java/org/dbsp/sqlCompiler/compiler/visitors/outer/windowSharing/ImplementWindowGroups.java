package org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import java.util.LinkedHashMap;
import java.util.Map;

/** Rebuilds the windows of each {@link WindowGroup} on one shared index. */
public class ImplementWindowGroups extends CircuitCloneVisitor {
    final FindWindowGroups analysis;
    /** Output of MapIndex for each group. */
    final Map<WindowGroup, OutputPort> sharedIndex;

    public ImplementWindowGroups(DBSPCompiler compiler, FindWindowGroups analysis) {
        super(compiler, false);
        this.analysis = analysis;
        this.sharedIndex = new LinkedHashMap<>();
    }

    /** The left input port for every member of `group`. */
    OutputPort leftInputPort(WindowGroup group) {
        OutputPort existing = this.sharedIndex.get(group);
        if (existing != null)
            return existing;

        OutputPort source = this.mapped(group.source);
        DBSPVariablePath row = group.key.parameters[0].getType().var();
        DBSPExpression[] values = new DBSPExpression[group.leftValueFields.size()];
        for (int i = 0; i < values.length; i++)
            values[i] = group.leftValueFields.get(i).call(row);
        DBSPClosureExpression indexFunction = new DBSPRawTupleExpression(
                group.key.call(row),
                new DBSPTupleExpression(values))
                .closure(row)
                .reduce(this.compiler())
                .to(DBSPClosureExpression.class);
        DBSPMapIndexOperator index = new DBSPMapIndexOperator(
                group.members.get(0).window().getRelNode(), indexFunction, source);
        this.addOperator(index);
        OutputPort result = index.outputPort();
        Utilities.putNew(this.sharedIndex, group, result);
        return result;
    }

    /** Builds a closure which extracts only the fields needed from the Window's output after sharing inputs. */
    DBSPClosureExpression projectBack(WindowGroup.Member member, OutputPort sharedIndex) {
        DBSPTypeRawTuple kvRef = sharedIndex.outputType()
                .to(DBSPTypeIndexedZSet.class).getKVRefType();
        DBSPVariablePath keyValue = kvRef.var();
        DBSPExpression[] fields = new DBSPExpression[member.fieldsUsed().length];
        for (int i = 0; i < fields.length; i++)
            fields[i] = keyValue.field(1).deref().field(member.fieldsUsed()[i]).applyCloneIfNeeded();
        return new DBSPRawTupleExpression(
                keyValue.field(0).deref().applyCloneIfNeeded(),
                new DBSPTupleExpression(fields))
                .closure(keyValue);
    }

    @Override
    public void postorder(DBSPWindowOperator window) {
        WindowGroup group = this.analysis.groups.get(window);
        if (group == null) {
            super.postorder(window);
            return;
        }
        WindowGroup.Member member = group.member(window);
        OutputPort leftInput = this.leftInputPort(group);
        DBSPWindowOperator replacement = new DBSPWindowOperator(
                window.getRelNode(), window.lowerInclusive, window.upperInclusive,
                window.lowerUnbounded, leftInput, this.mapped(window.right()));
        this.addOperator(replacement);

        // Keep only needed fields
        DBSPMapIndexOperator narrow = new DBSPMapIndexOperator(
                window.getRelNode(), this.projectBack(member, leftInput),
                window.getOutputIndexedZSetType(), replacement.outputPort());
        this.map(window, narrow);
    }
}
