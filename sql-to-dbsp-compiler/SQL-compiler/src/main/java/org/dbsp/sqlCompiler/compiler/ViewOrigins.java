package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

/** Records, for every Calcite relational operator, the view whose definition it belongs to. */
public class ViewOrigins {
    /** The view whose definition contains an operator, and the position of the view's statement */
    public record ViewSourcePosition(ProgramIdentifier view, SourcePositionRange position) { }

    /** Maps each Calcite relational operator of a view's plan to that view and the
     * position of the view's statement; operators are compared by identity */
    final Map<RelNode, ViewSourcePosition> origins = new IdentityHashMap<>();
    /** Maps the name of each view to the root of the view's plan */
    final Map<ProgramIdentifier, RelNode> plans = new HashMap<>();

    /** Record every operator of the plan rooted at {@code plan} as belonging to {@code view} */
    public void add(RelNode plan, ProgramIdentifier view, SourcePositionRange position) {
        this.plans.putIfAbsent(view, plan);
        this.add(plan, new ViewSourcePosition(view, position));
    }

    private void add(RelNode rel, ViewSourcePosition origin) {
        // Plans are trees; a node reached twice keeps its first origin
        if (this.origins.putIfAbsent(rel, origin) != null)
            return;
        for (RelNode input : rel.getInputs())
            this.add(input, origin);
    }

    /** The view whose definition contains {@code rel}, or null if the compiler synthesized it */
    @Nullable
    public ViewSourcePosition get(RelNode rel) {
        return this.origins.get(rel);
    }

    /** The root of the plan of {@code view}, or null for a view the compiler did not plan */
    @Nullable
    public RelNode getPlan(ProgramIdentifier view) {
        return this.plans.get(view);
    }
}
