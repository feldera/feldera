package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.util.Utilities;

import java.util.List;
import java.util.Objects;

/** Parse tree for {@code PRIMARY KEY} constraints. */
public class SqlPrimaryKey extends SqlCall {
    protected static final SqlSpecialOperator PRIMARY =
            new SqlSpecialOperator("PRIMARY KEY", SqlKind.PRIMARY_KEY) {
                @Override
                public SqlCall createCall(
                        @Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
                    Utilities.enforce(operands.length == 2);
                    return new SqlPrimaryKey(pos,
                            (SqlIdentifier) operands[0],
                            (SqlNodeList) Objects.requireNonNull(operands[1]));
                }
            };

    private final @Nullable SqlIdentifier name;
    private final SqlNodeList columnList;

    /** Creates a SqlKeyConstraint. */
    public SqlPrimaryKey(SqlParserPos pos, @Nullable SqlIdentifier name,
                         SqlNodeList columnList) {
        super(pos);
        this.name = name;
        this.columnList = columnList;
    }

    @Override public SqlOperator getOperator() {
        return PRIMARY;
    }

    @SuppressWarnings("nullness")
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList);
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (name != null) {
            writer.keyword("CONSTRAINT");
            name.unparse(writer, 0, 0);
        }
        writer.keyword(getOperator().getName()); // "PRIMARY KEY"
        columnList.unparse(writer, 1, 1);
    }
}

