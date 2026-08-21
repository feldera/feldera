package org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;

/** If multiple windows share the same timestamp from the same source, ensure that
 * they share the same input integral by making all window share the same left
 * MapIndex operator.  This is done by widening the operator to contain the
 * union of all fields needed by all inputs, and by inserting corresponding
 * projections after each of the widened windows. */
public class ShareWindowIntegrals extends Passes {
    public ShareWindowIntegrals(DBSPCompiler compiler) {
        super("WindowSharing", compiler);
        FindWindowGroups find = new FindWindowGroups(compiler);
        this.add(find);
        this.add(new ImplementWindowGroups(compiler, find));
    }
}
