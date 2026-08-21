package org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/** A set of {@link DBSPWindowOperator}s that can read one shared input. */
public class WindowGroup {
    /** One member window, and the list of fields it is using from the input value.
     *
     * @param fieldsUsed Position in {@link WindowGroup#leftValueFields} of each field of the
     *                   window's own value. */
    public record Member(DBSPWindowOperator window, int[] fieldsUsed) {}

    /** Left input stream for all windows. */
    public final OutputPort source;
    /** Closure which computes the timestamp (key part of left MapIndex). */
    public final DBSPClosureExpression key;
    /** One closure for each field of the tuple produced by the left MapIndex. */
    public final List<DBSPClosureExpression> leftValueFields;
    /** All windows sharing an input */
    public final List<Member> members;

    WindowGroup(OutputPort source, DBSPClosureExpression key,
                List<DBSPClosureExpression> leftValueFields, List<Member> members) {
        this.source = source;
        this.key = key;
        this.leftValueFields = leftValueFields;
        this.members = members;
        DBSPType row = key.parameters[0].getType();
        for (DBSPClosureExpression value : leftValueFields)
            Utilities.enforce(value.parameters.length == 1
                            && value.parameters[0].getType().sameType(row),
                    () -> "Value " + value + " does not read a row of " + row);
        for (Member member : members)
            for (int field : member.fieldsUsed())
                Utilities.enforce(field < leftValueFields.size(),
                        () -> "Field " + field + " is outside the " + leftValueFields.size()
                                + " left value fields of " + this);
    }

    /** The member for `window`, which must belong to this group. */
    public Member member(DBSPWindowOperator window) {
        for (Member member : this.members)
            if (member.window() == window)
                return member;
        throw new InternalCompilerError("Window " + window + " is not a member of " + this);
    }

    @Override
    public String toString() {
        List<String> windows = new ArrayList<>();
        for (Member m : this.members)
            windows.add(Long.toString(m.window().id));
        return "WindowGroup(" + windows + " on " + this.source.node().id
                + ", " + this.leftValueFields.size() + " shared fields)";
    }
}
