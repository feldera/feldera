package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;
import java.util.List;

/** Test-only access to the optional component positions. The public getter
 * substitutes the overall position when this list is absent. */
public final class FelderaS1Identifiers {
    private FelderaS1Identifiers() { }

    public static @Nullable List<SqlParserPos> components(SqlIdentifier identifier) {
        return identifier.componentPositions == null ? null : List.copyOf(identifier.componentPositions);
    }
}
