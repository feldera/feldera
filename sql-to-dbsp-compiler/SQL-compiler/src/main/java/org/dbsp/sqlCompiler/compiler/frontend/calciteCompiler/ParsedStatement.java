package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.calcite.sql.SqlNode;

/** A SQL statement obtained after parsing.  Some statements are not
 * <code>visible</code> because they are generated by the compiler internally.
 * These are not supposed to ever show up in error messages, for example. */
public record ParsedStatement(SqlNode statement, boolean visible) {
    @Override
    public String toString() {
        try {
            return this.statement.toString();
        } catch (Exception e) {
            // This can happen if the statement is syntactically incorrect
            // See https://issues.apache.org/jira/browse/CALCITE-6708
            return this.statement.getKind().toString();
        }
    }
}
