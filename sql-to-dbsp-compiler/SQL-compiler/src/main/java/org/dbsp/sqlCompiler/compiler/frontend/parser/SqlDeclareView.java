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

import java.util.List;
import java.util.Objects;

/** Parse tree for {@code DECLARE RECURSIVE VIEW} statement. */
public class SqlDeclareView extends SqlCreate {
    public final SqlIdentifier name;
    /** The column list can also contain foreign key declarations */
    public final SqlNodeList columns;

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DECLARE RECURSIVE VIEW", SqlKind.OTHER);

    public SqlDeclareView(SqlParserPos pos, SqlIdentifier name, SqlNodeList columns) {
        super(OPERATOR, pos, false, false);
        this.name = Objects.requireNonNull(name, "name");
        this.columns = columns;
    }

    @SuppressWarnings("nullness")
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columns);
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DECLARE");
        writer.keyword("RECURSIVE");
        writer.keyword("VIEW");
        name.unparse(writer, leftPrec, rightPrec);
        {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : columns) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }
    }
}
