package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions;

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.util.ListSqlOperatorTable;

import java.util.List;
import java.util.function.Consumer;

/** An operator table which does name matching always in a case-insensitive way.
 * As a consequence, all function name lookups are case-insensitive in SQL. */
public class CaseInsensitiveOperatorTable extends ListSqlOperatorTable {
    @SuppressWarnings("deprecation")
    public CaseInsensitiveOperatorTable(List<? extends SqlOperator> operators) {
        //noinspection unchecked
        super((List<SqlOperator>) operators);
    }

    @Override
    protected void lookUpOperators(String name, boolean caseSensitive, Consumer<SqlOperator> consumer) {
        // always case-insensitive
        super.lookUpOperators(name, false, consumer);
    }
}
