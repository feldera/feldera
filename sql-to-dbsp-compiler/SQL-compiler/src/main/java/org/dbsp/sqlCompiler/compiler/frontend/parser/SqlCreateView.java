package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/** Parse tree for {@code CREATE VIEW} statement.
    Ideally we would inherit {@link org.apache.calcite.sql.ddl.SqlCreateView}
    but the constructor isn't public. */
public class SqlCreateView extends SqlCreate {
    public enum ViewKind {
        /** For materialized views the DBSP program will keep the full contents */
        MATERIALIZED,
        /** Local views are not program outputs */
        LOCAL,
        /** Standard views only produce deltas */
        STANDARD,
        /** Automatic view maintained by the system */
        SYSTEM,
    }

    public final SqlIdentifier name;
    public final ViewKind viewKind;
    public final @Nullable SqlNodeList columnList;
    public final SqlNode query;
    @Nullable public final SqlNodeList viewProperties;

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE VIEW", SqlKind.CREATE_VIEW);

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

    @SuppressWarnings("nullness")
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, viewProperties, query);
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

