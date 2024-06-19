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

/** Parse tree for {@code CREATE TABLE} statement. */
public class SqlCreateTable extends SqlCreate {
    public final SqlIdentifier name;
    public final SqlNodeList columnList;
    /** Key-value list of string literals */
    public final @Nullable SqlNodeList connectorProperties;

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    public SqlCreateTable(SqlParserPos pos, boolean replace, boolean ifNotExists,
                             SqlIdentifier name, SqlNodeList columnList, @Nullable SqlNodeList connectorProperties) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.name = Objects.requireNonNull(name, "name");
        this.columnList = columnList;
        this.connectorProperties = connectorProperties;
        assert connectorProperties == null || connectorProperties.size() % 2 == 0;
    }

    @SuppressWarnings("nullness")
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, connectorProperties);
    }

    public static void writeProperties(SqlWriter writer, @Nullable SqlNodeList connectorProperties) {
        if (connectorProperties != null) {
            writer.keyword("WITH");
            SqlWriter.Frame frame = writer.startList("(", ")");
            boolean even = true;
            for (SqlNode c : connectorProperties) {
                if (even) {
                    writer.sep(",");
                } else {
                    writer.sep("=");
                }
                c.unparse(writer, 0, 0);
                even = !even;
            }
            writer.endList(frame);
            writer.newlineAndIndent();
        }
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("TABLE");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
        {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : columnList) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }
        writeProperties(writer, this.connectorProperties);
    }
}

