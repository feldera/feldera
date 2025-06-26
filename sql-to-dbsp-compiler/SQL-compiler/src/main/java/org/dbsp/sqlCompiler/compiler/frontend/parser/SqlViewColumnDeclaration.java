package org.dbsp.sqlCompiler.compiler.frontend.parser;

import com.google.common.collect.ImmutableList;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Declaration of a column in a recursive view declaration */
public class SqlViewColumnDeclaration extends SqlCall {
    private static final SqlSpecialOperator OPERATOR;
    public final SqlIdentifier name;
    public final SqlDataTypeSpec dataType;
    public final boolean interned;

    public SqlViewColumnDeclaration(SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, boolean interned) {
        super(pos);
        this.name = name;
        this.dataType = dataType;
        this.interned = interned;
    }

    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableList.of(this.name, this.dataType);
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        this.name.unparse(writer, 0, 0);
        this.dataType.unparse(writer, 0, 0);
        if (Boolean.FALSE.equals(this.dataType.getNullable())) {
            writer.keyword("NOT NULL");
        }

        if (this.interned) {
            writer.keyword("INTERNED");
        }
    }

    static {
        OPERATOR = new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);
    }
}
