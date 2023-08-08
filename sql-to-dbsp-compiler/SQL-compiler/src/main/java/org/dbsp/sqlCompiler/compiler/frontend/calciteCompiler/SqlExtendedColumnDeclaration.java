package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * This class is based on SqlColumnDeclaration from Calcite.
 * It should be an extension of that class, but that class doesn't have
 * a public constructor, so we have copied here the code.
 */
public class SqlExtendedColumnDeclaration extends SqlCall {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

    public final SqlIdentifier name;
    public final SqlDataTypeSpec dataType;
    public final @Nullable SqlNode expression;
    public final ColumnStrategy strategy;
    public final @Nullable SqlNode lateness;
    public final @Nullable SqlIdentifier foreignKeyTable;
    public final @Nullable SqlIdentifier foreignKeyColumn;
    public final boolean primaryKey;

    public SqlExtendedColumnDeclaration(
            SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType,
            @Nullable SqlNode expression, ColumnStrategy strategy,
            @Nullable SqlIdentifier foreignKeyTable, @Nullable SqlIdentifier foreignKeyColumn,
            boolean primaryKey, @Nullable SqlNode lateness) {
        super(pos);
        this.name = name;
        this.dataType = dataType;
        this.expression = expression;
        this.strategy = strategy;
        this.foreignKeyTable = foreignKeyTable;
        this.foreignKeyColumn = foreignKeyColumn;
        this.primaryKey = primaryKey;
        this.lateness = lateness;
    }

    @Override public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override public List<SqlNode> getOperandList() {
        return ImmutableList.of(name, dataType);
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, 0, 0);
        dataType.unparse(writer, 0, 0);
        if (Boolean.FALSE.equals(dataType.getNullable())) {
            writer.keyword("NOT NULL");
        }
        SqlNode expression = this.expression;
        if (expression != null) {
            switch (strategy) {
                case VIRTUAL:
                case STORED:
                    writer.keyword("AS");
                    exp(writer, expression);
                    writer.keyword(strategy.name());
                    break;
                case DEFAULT:
                    writer.keyword("DEFAULT");
                    exp(writer, expression);
                    break;
                default:
                    throw new AssertionError("unexpected: " + strategy);
            }
        }
        if (this.lateness != null) {
            writer.keyword("LATENESS");
            exp(writer, this.lateness);
        }
    }

    static void exp(SqlWriter writer, SqlNode expression) {
        if (writer.isAlwaysUseParentheses()) {
            expression.unparse(writer, 0, 0);
        } else {
            writer.sep("(");
            expression.unparse(writer, 0, 0);
            writer.sep(")");
        }
    }
}
