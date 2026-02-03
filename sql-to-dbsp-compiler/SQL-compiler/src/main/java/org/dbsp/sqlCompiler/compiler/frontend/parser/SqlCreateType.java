package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDataTypeSpec;
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

public class SqlCreateType extends SqlCreate {
    public final SqlIdentifier name;
    public final @Nullable SqlNodeList attributeDefs;
    public final @Nullable SqlDataTypeSpec dataType;
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE TYPE", SqlKind.CREATE_TYPE) {
        @Override
        public SqlCall createCall(
                @Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            Utilities.enforce(operands.length == 3);
            return new SqlCreateType(pos, false,
                    (SqlIdentifier) Objects.requireNonNull(operands[0]),
                    (SqlNodeList) operands[1],
                    (SqlDataTypeSpec) operands[2]);
        }
    };

    public SqlCreateType(SqlParserPos pos, boolean replace, SqlIdentifier name,
                         @Nullable SqlNodeList attributeDefs, @Nullable SqlDataTypeSpec dataType) {
        super(OPERATOR, pos, replace, false);
        this.name = Objects.requireNonNull(name, "name");
        this.attributeDefs = attributeDefs;
        this.dataType = dataType;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(this.name, this.attributeDefs, this.dataType);
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.getReplace()) {
            writer.keyword("CREATE OR REPLACE");
        } else {
            writer.keyword("CREATE");
        }

        writer.keyword("TYPE");
        this.name.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        if (this.attributeDefs != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");

            for(SqlNode a : this.attributeDefs) {
                writer.sep(",");
                a.unparse(writer, 0, 0);
            }

            writer.endList(frame);
        } else if (this.dataType != null) {
            this.dataType.unparse(writer, leftPrec, rightPrec);
        }
    }
}

