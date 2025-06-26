package org.dbsp.sqlCompiler.compiler.frontend.parser;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/** This class is based on {@link SqlColumnDeclaration} from Calcite.
 * It should be an extension of that class, but that class doesn't have
 * a public constructor, so we have copied here the code. */
public class SqlExtendedColumnDeclaration extends SqlCall {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

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
        this.interned = interned;
        if (foreignKeyTable != null)
            this.foreignKeyTables.add(foreignKeyTable);
        if (foreignKeyColumn != null)
            this.foreignKeyColumns.add(foreignKeyColumn);
        this.primaryKey = primaryKey;
        this.lateness = lateness;
        this.watermark = watermark;
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

    public SqlExtendedColumnDeclaration setLatenes(SqlNode lateness) {
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

    @Override public List<SqlNode> getOperandList() {
        return ImmutableList.of(this.name, this.dataType);
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
