package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlIdentifier;

public final class S1ForeignKeyIdentifier {
    private S1ForeignKeyIdentifier() { }
    public static SqlIdentifier get(SqlFragmentIdentifier fragment) { return fragment.identifier; }
}
