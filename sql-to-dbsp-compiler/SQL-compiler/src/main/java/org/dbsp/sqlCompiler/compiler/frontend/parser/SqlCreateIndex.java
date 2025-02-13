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

/** Parse tree for {@code CREATE INDEX} statement. */
public class SqlCreateIndex extends SqlCreate {
    public final SqlIdentifier name;
    public final SqlIdentifier indexed;
    public final SqlNodeList columns;

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE INDEX", SqlKind.CREATE_INDEX);

    public SqlCreateIndex(SqlParserPos pos, SqlIdentifier name, SqlIdentifier indexed, SqlNodeList columns) {
        super(OPERATOR, pos, false, false);
        this.name = Objects.requireNonNull(name, "name");
        this.indexed = indexed;
        this.columns = columns;
    }

    @SuppressWarnings("nullness")
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(this.name, this.indexed, this.columns);
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("INDEX");
        this.name.unparse(writer, leftPrec, rightPrec);
        writer.keyword("ON");
        this.indexed.unparse(writer, leftPrec, rightPrec);
        {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : this.columns) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }
    }
}
