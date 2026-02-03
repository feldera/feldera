package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreate;
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

/** Parse tree for {@code CREATE VIEW} statement. */
public class SqlCreateView extends SqlCreate {
    public enum ViewKind {
        /** For materialized views the DBSP program will keep the full contents */
        MATERIALIZED,
        /** Local views are not program outputs */
        LOCAL,
        /** Standard views only produce deltas */
        STANDARD,
    }

    public final SqlIdentifier name;
    public final ViewKind viewKind;
    public final @Nullable SqlNodeList columnList;
    public final SqlNode query;
    @Nullable public final SqlNodeList viewProperties;

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE VIEW", SqlKind.CREATE_VIEW) {
                @Override
                public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
                    Utilities.enforce(operands.length == 5);
                    return new SqlCreateView(pos, false,
                            ViewKind.valueOf(((SqlIdentifier) Objects.requireNonNull(operands[0])).getSimple()),
                            (SqlIdentifier) Objects.requireNonNull(operands[1]),
                            (SqlNodeList) operands[2],
                            (SqlNodeList) operands[3],
                            Objects.requireNonNull(operands[4]));
                }
            };

    public SqlCreateView(SqlParserPos pos, boolean replace, ViewKind viewKind, SqlIdentifier name,
                         @Nullable SqlNodeList columnList, @Nullable SqlNodeList viewProperties,
                         SqlNode query) {
        super(OPERATOR, pos, replace, false);
        this.name = Objects.requireNonNull(name, "name");
        this.columnList = columnList; // may be null
        this.query = Objects.requireNonNull(query, "query");
        this.viewKind = viewKind;
        this.viewProperties = viewProperties;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlCreateView(pos, this.getReplace(), this.viewKind, this.name, this.columnList,
                this.viewProperties, this.query);
    }

    @SuppressWarnings("nullness")
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                new SqlIdentifier(this.viewKind.name(), SqlParserPos.ZERO),
                name, columnList, viewProperties, query);
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (getReplace()) {
            writer.keyword("CREATE OR REPLACE");
        } else {
            writer.keyword("CREATE");
        }
        switch (this.viewKind) {
            case LOCAL:
                writer.keyword("LOCAL");
                break;
            case MATERIALIZED:
                writer.keyword("MATERIALIZED");
                break;
            default:
                break;
        }
        writer.keyword("VIEW");
        name.unparse(writer, leftPrec, rightPrec);
        if (columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : columnList) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }
        SqlCreateTable.writeProperties(writer, this.viewProperties);
        writer.keyword("AS");
        writer.newlineAndIndent();
        query.unparse(writer, 0, 0);
    }
}

