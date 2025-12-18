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
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Our own version of CREATE FUNCTION, different from Calcite. */
public class SqlCreateFunctionDeclaration extends SqlCreate {
    private final SqlIdentifier name;
    private final SqlNodeList parameters;
    private final SqlDataTypeSpec returnType;
    @Nullable private final SqlNode body;

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION) {
                @Override
                public SqlCall createCall(@org.checkerframework.checker.nullness.qual.Nullable SqlLiteral functionQualifier,
                                          SqlParserPos pos, @org.checkerframework.checker.nullness.qual.Nullable SqlNode... operands) {
                    Utilities.enforce(operands.length == 4);
                    return new SqlCreateFunctionDeclaration(pos, false, false,
                            (SqlIdentifier) Objects.requireNonNull(operands[0]),
                            (SqlNodeList) Objects.requireNonNull(operands[1]),
                            (SqlDataTypeSpec) Objects.requireNonNull(operands[2]),
                            operands[3]);
                }
            };

    public SqlCreateFunctionDeclaration(SqlParserPos pos, boolean replace,
                                        boolean ifNotExists, SqlIdentifier name,
                                        SqlNodeList parameters, SqlDataTypeSpec returnType,
                                        @Nullable SqlNode body) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.name = Objects.requireNonNull(name, "name");
        this.parameters = Objects.requireNonNull(parameters, "parameters");
        this.returnType = returnType;
        this.body = body;
    }

    @Override public void unparse(SqlWriter writer, int leftPrec,
                                  int rightPrec) {
        writer.keyword(getReplace() ? "CREATE OR REPLACE" : "CREATE");
        writer.keyword("FUNCTION");
        if (this.ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        this.name.unparse(writer, 0, 0);
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")");
        for (SqlNode parameter : this.parameters) {
            writer.sep(",");
            parameter.unparse(writer, 0, 0);
        }
        writer.endList(frame);
        writer.keyword("RETURNS");
        this.returnType.unparse(writer, 0, 0);
        if (this.body != null) {
            writer.keyword("AS");
            this.body.unparse(writer, 0, 0);
        }
    }

    @Override public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlNodeList getParameters() {
        return this.parameters;
    }

    public SqlDataTypeSpec getReturnType() {
        return this.returnType;
    }

    public SqlIdentifier getName() {
        return this.name;
    }

    @Override public List<SqlNode> getOperandList() {
        return Arrays.asList(this.name, this.parameters, this.returnType, this.body);
    }

    @Nullable public SqlNode getBody() { return this.body; }
}
