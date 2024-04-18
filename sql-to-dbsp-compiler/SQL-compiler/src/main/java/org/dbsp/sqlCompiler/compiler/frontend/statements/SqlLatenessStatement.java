package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

public class SqlLatenessStatement extends FrontEndStatement {
    public final SqlIdentifier view;
    public final SqlIdentifier column;
    public final RexNode value;

    public SqlLatenessStatement(SqlNode node, String statement,
                                SqlIdentifier view, SqlIdentifier column,
                                RexNode value) {
        super(node, statement, null);
        this.view = view;
        this.column = column;
        this.value = value;
    }
}
