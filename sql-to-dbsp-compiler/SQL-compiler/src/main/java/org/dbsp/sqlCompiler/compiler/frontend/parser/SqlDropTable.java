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

import static java.util.Objects.requireNonNull;

/**
 * Parse tree for {@code DROP TABLE} statement.
 */
public class SqlDropTable extends SqlDropObject {
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE) {
                @Override
                public SqlCall createCall(
                        @Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
                    Utilities.enforce(operands.length == 2);
                    return new SqlDropTable(pos,
                            ((SqlLiteral) requireNonNull(operands[0], "ifExists")).booleanValue(),
                            (SqlIdentifier) Objects.requireNonNull(operands[1]));
                }
            };

    /** Creates a SqlDropTable. */
    public SqlDropTable(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
        super(OPERATOR, pos, ifExists, name);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(SqlLiteral.createBoolean(ifExists, SqlParserPos.ZERO), this.name);
    }
}
