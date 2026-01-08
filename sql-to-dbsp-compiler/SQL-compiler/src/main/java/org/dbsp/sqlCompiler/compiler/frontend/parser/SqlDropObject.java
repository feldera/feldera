package org.dbsp.sqlCompiler.compiler.frontend.parser;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Base class for parse trees of {@code DROP TABLE}, {@code DROP VIEW},
 * {@code DROP MATERIALIZED VIEW} and {@code DROP TYPE} statements.
 *
 * <p>We cannot use the Calcite DDL class because it's constructor is not protected.
 */
public abstract class SqlDropObject extends SqlDrop {
    public final SqlIdentifier name;

    /** Creates a SqlDropObject. */
    SqlDropObject(SqlOperator operator, SqlParserPos pos, boolean ifExists,
                  SqlIdentifier name) {
        super(operator, pos, ifExists);
        this.name = name;
    }

    @Override public List<SqlNode> getOperandList() {
        return ImmutableList.of(name);
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName()); // "DROP TABLE" etc.
        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
    }

    public void execute(CalcitePrepare.Context context) { }
}
