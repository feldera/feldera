package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

/** Test-only access to the exact constructor. RexBuilder's public/protected
 * factories normalize types and can clone NULL collection types. S1 import must
 * retain the already validated arena object and canonical value representation. */
public final class FelderaS1LiteralFactory {
    private FelderaS1LiteralFactory() { }

    public static RexLiteral create(Comparable<?> value, RelDataType type, SqlTypeName storage) {
        return new RexLiteral(value, type, storage);
    }
}
