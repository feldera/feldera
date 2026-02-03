package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.util.Utilities;

import java.util.List;
import java.util.Objects;

/** An Attribute attached to an argument in a user-defined function declaration. */
// The implementation in Calcite DDL is incorrect, so this is a clone of a Calcite class with the same name.
public class SqlAttributeDefinition extends SqlCall {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ATTRIBUTE_DEF", SqlKind.ATTRIBUTE_DEF) {
                @Override
                public SqlCall createCall(
                        @Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
                    Utilities.enforce(operands.length == 3);
                    return new SqlAttributeDefinition(pos,
                            (SqlIdentifier) Objects.requireNonNull(operands[0]),
                            (SqlDataTypeSpec) Objects.requireNonNull(operands[1]),
                            operands[2]);
                }
            };
    public final SqlIdentifier name;
    public final SqlDataTypeSpec dataType;
    final @Nullable SqlNode expression;

    public SqlAttributeDefinition(
            SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, @Nullable SqlNode expression) {
        super(pos);
        this.name = name;
        this.dataType = dataType;
        this.expression = expression;
    }

    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(this.name, this.dataType, this.expression);
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        this.name.unparse(writer, 0, 0);
        this.dataType.unparse(writer, 0, 0);

        if (Boolean.FALSE.equals(this.dataType.getNullable())) {
            writer.keyword("NOT NULL");
        }

        SqlNode expression = this.expression;
        if (expression != null) {
            writer.keyword("DEFAULT");
            if (writer.isAlwaysUseParentheses()) {
                expression.unparse(writer, 0, 0);
            } else {
                writer.sep("(");
                expression.unparse(writer, 0, 0);
                writer.sep(")");
            }
        }
    }
}
