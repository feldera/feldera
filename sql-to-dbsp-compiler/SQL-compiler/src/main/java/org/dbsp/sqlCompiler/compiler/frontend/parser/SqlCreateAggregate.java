package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Result produced by a CREATE AGGREGATE statement */
public class SqlCreateAggregate extends SqlCreate {
    private final SqlIdentifier name;
    private final SqlNodeList parameters;
    private final SqlDataTypeSpec returnType;
    private final boolean linear;

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE AGGREGATE", SqlKind.OTHER);

    public SqlCreateAggregate(SqlParserPos pos, boolean replace,
                              boolean ifNotExists, boolean linear, SqlIdentifier name,
                              SqlNodeList parameters, SqlDataTypeSpec returnType) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.name = Objects.requireNonNull(name, "name");
        this.parameters = Objects.requireNonNull(parameters, "parameters");
        this.returnType = returnType;
        this.linear = linear;
    }

    public boolean isLinear() {
        return this.linear;
    }

    @Override public void unparse(SqlWriter writer, int leftPrec,
                                  int rightPrec) {
        writer.keyword(getReplace() ? "CREATE OR REPLACE" : "CREATE");
        if (this.linear)
            writer.keyword("LINEAR");
        writer.keyword("AGGREGATE");
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
        return Arrays.asList(this.name, this.parameters);
    }
}
