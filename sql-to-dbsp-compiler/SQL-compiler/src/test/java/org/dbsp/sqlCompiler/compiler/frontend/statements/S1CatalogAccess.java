package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.plan.RelOptTable;

/** Fixed access to the compiler catalog; never reflective construction. */
public final class S1CatalogAccess {
    private S1CatalogAccess() { }
    public static IHasSchema schema(RelOptTable table) {
        var description = table.unwrap(CalciteTableDescription.class);
        if (description == null) throw new IllegalArgumentException("S1 scan has no compiler catalog schema");
        return description.schema;
    }
}
