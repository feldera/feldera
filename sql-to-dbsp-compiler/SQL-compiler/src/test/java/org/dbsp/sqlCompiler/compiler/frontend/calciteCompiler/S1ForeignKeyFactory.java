package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragmentIdentifier;
import java.util.List;

public final class S1ForeignKeyFactory {
    private S1ForeignKeyFactory() { }
    public static ForeignKey.TableAndColumns create(SourcePositionRange position, SqlFragmentIdentifier table,
                                                     List<SqlFragmentIdentifier> columns) {
        return new ForeignKey.TableAndColumns(position, table, List.copyOf(columns));
    }
}
