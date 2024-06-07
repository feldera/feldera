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

/** Parse tree for {@code CREATE VIEW} statement. */
// Ideally we would inherit org.apache.calcite.sql.ddl.SqlCreateView,
// but the constructor isn't public.
// We just need an extra 'local' field.
public class SqlCreateLocalView extends SqlCreate {
    public final SqlIdentifier name;
    public final boolean isLocal;
    public final @Nullable SqlNodeList columnList;
    public final SqlNode query;

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE VIEW", SqlKind.CREATE_VIEW);

    public SqlCreateLocalView(SqlParserPos pos, boolean replace, boolean local, SqlIdentifier name,
                        @Nullable SqlNodeList columnList, SqlNode query) {
        super(OPERATOR, pos, replace, false);
        this.name = Objects.requireNonNull(name, "name");
        this.columnList = columnList; // may be null
        this.query = Objects.requireNonNull(query, "query");
        this.isLocal = local;
    }

    @SuppressWarnings("nullness")
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, query);
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (getReplace()) {
            writer.keyword("CREATE OR REPLACE");
        } else {
            writer.keyword("CREATE");
        }
        if (this.isLocal) {
            writer.keyword("LOCAL");
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
        writer.keyword("AS");
        writer.newlineAndIndent();
        query.unparse(writer, 0, 0);
    }
}

