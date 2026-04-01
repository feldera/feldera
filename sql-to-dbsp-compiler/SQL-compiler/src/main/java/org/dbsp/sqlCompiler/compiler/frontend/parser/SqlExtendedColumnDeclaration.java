package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** This class is based on SqlColumnDeclaration from Calcite.
 * It should be an extension of that class, but that class doesn't have
 * a public constructor, so we have copied here the code. */
public class SqlExtendedColumnDeclaration extends SqlCall {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL) {
                static List<SqlIdentifier> extract(SqlNodeList list) {
                    List<SqlIdentifier> result = new ArrayList<>();
                    for (SqlNode node: list) {
                        result.add((SqlIdentifier) node);
                    }
                    return result;
                }

                @Override
                public SqlCall createCall(
                        @Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
                    Utilities.enforce(operands.length == 11);
                    return new SqlExtendedColumnDeclaration(pos,
                            (SqlIdentifier) Objects.requireNonNull(operands[0]),
                            (SqlDataTypeSpec) Objects.requireNonNull(operands[1]),
                            operands[2],
                            ColumnStrategy.valueOf(((SqlIdentifier) Objects.requireNonNull(operands[3])).getSimple()),
                            extract((SqlNodeList) Objects.requireNonNull(operands[4])),
                            extract((SqlNodeList) Objects.requireNonNull(operands[5])),
                            ((SqlLiteral) Objects.requireNonNull(operands[6])).booleanValue(),
                            operands[7],
                            operands[8],
                            operands[9],
                            ((SqlLiteral) Objects.requireNonNull(operands[10])).booleanValue());
                }
            };

    public final SqlIdentifier name;
    public final SqlDataTypeSpec dataType;
    public final @Nullable SqlNode expression;
    public final ColumnStrategy strategy;
    // The next two lists must have the same length.
    // There can be multiple foreign key columns for one column
    public final List<SqlIdentifier> foreignKeyTables;
    public final List<SqlIdentifier> foreignKeyColumns;
    // These can be mutated
    public boolean primaryKey;
    public @Nullable SqlNode lateness;
    public @Nullable SqlNode watermark;
    public @Nullable SqlNode defaultValue;
    public boolean interned;

    public SqlExtendedColumnDeclaration(
            SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType,
            @Nullable SqlNode expression, ColumnStrategy strategy,
            @Nullable SqlIdentifier foreignKeyTable, @Nullable SqlIdentifier foreignKeyColumn,
            boolean primaryKey, @Nullable SqlNode lateness, @Nullable SqlNode watermark, boolean interned) {
        super(pos);
        this.name = name;
        this.dataType = dataType;
        this.expression = expression;
        this.strategy = strategy;
        this.defaultValue = null;
        this.foreignKeyTables = new ArrayList<>();
        this.foreignKeyColumns = new ArrayList<>();
        if (foreignKeyTable != null)
            this.foreignKeyTables.add(foreignKeyTable);
        if (foreignKeyColumn != null)
            this.foreignKeyColumns.add(foreignKeyColumn);
        this.primaryKey = primaryKey;
        this.lateness = lateness;
        this.watermark = watermark;
        this.interned = interned;
    }

    public SqlExtendedColumnDeclaration(
            SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, @Nullable SqlNode expression,
            ColumnStrategy strategy, List<SqlIdentifier> foreignKeyTables, List<SqlIdentifier> foreignKeyColumns,
            boolean primaryKey, @Nullable SqlNode lateness, @Nullable SqlNode watermark, @Nullable SqlNode defaultValue,
            boolean interned) {
        super(pos);
        this.name = name;
        this.dataType = dataType;
        this.expression = expression;
        this.strategy = strategy;
        this.foreignKeyTables = foreignKeyTables;
        this.foreignKeyColumns = foreignKeyColumns;
        this.primaryKey = primaryKey;
        this.lateness = lateness;
        this.watermark = watermark;
        this.defaultValue = defaultValue;
        this.interned = interned;
    }

    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(this.name, this.dataType, this.expression,
                new SqlIdentifier(this.strategy.name(), SqlParserPos.ZERO),
                new SqlNodeList(this.foreignKeyTables, SqlParserPos.ZERO),
                new SqlNodeList(this.foreignKeyColumns, SqlParserPos.ZERO),
                SqlLiteral.createBoolean(this.primaryKey, SqlParserPos.ZERO),
                this.lateness, this.watermark, this.defaultValue,
                SqlLiteral.createBoolean(this.interned, SqlParserPos.ZERO));
    }

    public SqlExtendedColumnDeclaration setPrimaryKey(SqlParserPos pos) {
        if (this.primaryKey) {
            throw new CompilationError("Column " + Utilities.singleQuote(this.name.getSimple()) +
                    " already declared a primary key", CalciteObject.create(pos));
        }
        this.primaryKey = true;
        return this;
    }

    public SqlExtendedColumnDeclaration setDefault(SqlNode expression) {
        if (this.defaultValue != null){
            throw new CompilationError("Column " + Utilities.singleQuote(this.name.getSimple()) +
                    " already has a default value", CalciteObject.create(expression));
        }
        this.defaultValue = expression;
        return this;
    }

    public SqlExtendedColumnDeclaration setForeignKey(SqlIdentifier table, SqlIdentifier column) {
        this.foreignKeyTables.add(table);
        this.foreignKeyColumns.add(column);
        return this;
    }

    public SqlExtendedColumnDeclaration setLateness(SqlNode lateness) {
        if (this.lateness != null) {
            throw new CompilationError("Column " + Utilities.singleQuote(this.name.getSimple()) +
                    " already has lateness", CalciteObject.create(lateness));
        }
        this.lateness = lateness;
        return this;
    }

    public SqlExtendedColumnDeclaration setInterned() {
        if (this.interned) {
            throw new CompilationError("Column " + Utilities.singleQuote(this.name.getSimple()) +
                    " already marked as interned");
        }
        this.interned = true;
        return this;
    }

    public SqlExtendedColumnDeclaration setWatermark(SqlNode watermark) {
        if (this.watermark != null) {
            throw new CompilationError("Column " + Utilities.singleQuote(this.name.getSimple()) +
                    " already has a watermark", CalciteObject.create(watermark));
        }
        this.watermark = watermark;
        return this;
    }

    @Override public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        this.name.unparse(writer, 0, 0);
        this.dataType.unparse(writer, 0, 0);
        if (Boolean.FALSE.equals(dataType.getNullable())) {
            writer.keyword("NOT NULL");
        }
        SqlNode expression = this.expression;
        if (expression != null) {
            switch (this.strategy) {
                case VIRTUAL:
                case STORED:
                    writer.keyword("AS");
                    exp(writer, expression);
                    writer.keyword(this.strategy.name());
                    break;
                case DEFAULT:
                    writer.keyword("DEFAULT");
                    exp(writer, expression);
                    break;
                default:
                    throw new AssertionError("unexpected: " + strategy);
            }
        }
        if (this.primaryKey) {
            writer.keyword("PRIMARY");
            writer.keyword("KEY");
        }
        for (int i = 0; i < this.foreignKeyTables.size(); i++) {
            SqlIdentifier table = this.foreignKeyTables.get(i);
            SqlIdentifier column = this.foreignKeyColumns.get(i);
            writer.keyword("FOREIGN");
            writer.keyword("KEY");
            writer.keyword("REFERENCES");
            table.unparse(writer, 0, 0);
            SqlWriter.Frame frame = writer.startList("(", ")");
            column.unparse(writer, 0, 0);
            writer.endList(frame);
        }
        if (this.lateness != null) {
            writer.keyword("LATENESS");
            this.lateness.unparse(writer, 0, 0);
        }
        if (this.defaultValue != null) {
            writer.keyword("DEFAULT");
            this.defaultValue.unparse(writer, 0, 0);
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
