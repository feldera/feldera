package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.util.Utilities;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code DROP VIEW} statement.
 */
public class SqlDropView extends SqlDropObject {
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DROP VIEW", SqlKind.DROP_VIEW) {
                @Override
                public SqlCall createCall(
                        @Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
                    Utilities.enforce(operands.length == 1);
                    return new SqlDropView(pos, false,
                            (SqlIdentifier) Objects.requireNonNull(operands[0]));
                }
            };

    /** Creates a SqlDropView. */
    public SqlDropView(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
        super(OPERATOR, pos, ifExists, name);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(this.name);
    }
}
