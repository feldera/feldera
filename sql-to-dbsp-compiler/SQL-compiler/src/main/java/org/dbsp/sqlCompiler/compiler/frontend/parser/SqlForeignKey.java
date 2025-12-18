package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.util.Utilities;

import java.util.List;
import java.util.Objects;

/** A FOREIGN KEY declaration that can appear in a table but not as an attribute
 * of a column.
 * E.g.:
 * CREATE TABLE T (
 *   id BIGINT NOT NULL PRIMARY KEY,
 *   FOREIGN KEY (id) REFERENCES inventoryitem_t (id))
 * */
public class SqlForeignKey extends SqlCall {
    private static final SqlSpecialOperator FOREIGN =
            new SqlSpecialOperator("FOREIGN", SqlKind.OTHER) {
                @Override
                public SqlCall createCall(
                        @Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
                    Utilities.enforce(operands.length == 3);
                    return new SqlForeignKey(pos,
                            (SqlNodeList) Objects.requireNonNull(operands[0]),
                            (SqlIdentifier) Objects.requireNonNull(operands[1]),
                            (SqlNodeList) Objects.requireNonNull(operands[2]));
                }
            };

    public final SqlNodeList columnList;
    public final SqlIdentifier otherTable;
    public final SqlNodeList otherColumnList;

    public SqlForeignKey(SqlParserPos pos, SqlNodeList columnList,
                         SqlIdentifier otherTable, SqlNodeList otherColumnList) {
        super(pos);
        this.columnList = columnList;
        this.otherTable = otherTable;
        this.otherColumnList = otherColumnList;
    }

    @Override
    public SqlOperator getOperator() {
        return FOREIGN;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(columnList, otherTable, otherColumnList);
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("FOREIGN KEY");
        SqlWriter.Frame frame = writer.startList("(", ")");
        for (SqlNode c : columnList) {
            writer.sep(",");
            c.unparse(writer, 0, 0);
        }
        writer.endList(frame);
        writer.keyword("REFERENCES");
        otherTable.unparse(writer, 0, 0);

        frame = writer.startList("(", ")");
        for (SqlNode c : otherColumnList) {
            writer.sep(",");
            c.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }
}
