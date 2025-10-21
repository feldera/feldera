package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Conditional;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Fail;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveTable;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Repeat;

/** Implements the "now" operator.
 * This requires:
 * - using the input stream called now
 * - rewriting map operators that have calls to "now()" into a join followed by a map
 * - rewriting the invocations to the now() function in the map function to references to the input variable */
public class ImplementNow extends Passes {
    public ImplementNow(DBSPCompiler compiler) {
        super("ImplementNow", compiler);
        boolean removeTable = !compiler.compiler().options.ioOptions.nowStream;
        RewriteNow rewriteNow = new RewriteNow(compiler);
        CircuitContainsNow cn = new CircuitContainsNow(compiler);
        CircuitTransform breakFilters = new Repeat(compiler, new BreakFilters(compiler));
        // Check if there is any operator containing NOW
        this.passes.add(cn);
        // If there is, break filters that contain NOW into simpler filters
        this.passes.add(new Conditional(compiler, breakFilters, cn::found));
        // Rewrite MAP and FILTER operators
        this.passes.add(new Conditional(compiler, rewriteNow, cn::found));
        // Remove the NOW table
        this.passes.add(new Conditional(compiler,
                new RemoveTable(compiler, DBSPCompiler.NOW_TABLE_NAME), () -> !cn.found() || removeTable));
        Passes check = new Passes("CheckNow", compiler);
        // Check that no instances of NOW are left in the circuit
        CircuitContainsNow cn0 = new CircuitContainsNow(compiler);
        check.add(cn0);
        check.add(new Conditional(compiler,
                new Fail(compiler, "Instances of 'now' have not been replaced"), cn0::found));
        // Only if we found now previously we check that it's been removed
        this.passes.add(new Conditional(compiler, check, cn::found));
    }
}

