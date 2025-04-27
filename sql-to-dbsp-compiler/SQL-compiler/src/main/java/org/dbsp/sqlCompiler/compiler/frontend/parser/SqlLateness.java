package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.dbsp.util.Utilities;

import java.util.List;
import java.util.Objects;

/** The LATENESS statement is used to declare the lateness of a view's columns. */
public class SqlLateness extends SqlCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("LATENESS", SqlKind.OTHER) {
                @SuppressWarnings("argument.type.incompatible")
                @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                                                    SqlParserPos pos,
                                                    @Nullable SqlNode... operands) {
                    return new SqlLateness(
                            pos,
                            (SqlIdentifier) Objects.requireNonNull(operands[0]),
                            (SqlIdentifier) Objects.requireNonNull(operands[1]),
                            Objects.requireNonNull(operands[2]));
                }
            };

    SqlIdentifier view;
    SqlIdentifier column;
    SqlNode expression;

    public SqlLateness(SqlParserPos pos,
                       SqlIdentifier view,
                       SqlIdentifier column,
                       SqlNode expression) {
        super(pos);
        this.view = view;
        this.column = column;
        this.expression = expression;
    }

    @Override public SqlKind getKind() {
        return SqlKind.OTHER;
    }

    @Override public SqlOperator getOperator() {
        return OPERATOR;
    }

    @SuppressWarnings("nullness")
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(view, column, expression);
    }

    @SuppressWarnings("assignment.type.incompatible")
    @Override public void setOperand(int i, @Nullable SqlNode operand) {
        Objects.requireNonNull(operand);
        switch (i) {
            case 0:
                Utilities.enforce(operand instanceof SqlIdentifier);
                this.view = (SqlIdentifier) operand;
                break;
            case 1:
                Utilities.enforce(operand instanceof SqlIdentifier);
                this.column = (SqlIdentifier) operand;
                break;
            case 3:
                this.expression = operand;
                break;
            default:
                throw new AssertionError(i);
        }
    }

    public SqlIdentifier getView() {
        return this.view;
    }

    public SqlIdentifier getColumn() {
        return this.column;
    }

    @Pure
    public SqlNode getLateness() {
        return this.expression;
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.keyword("LATENESS");
        final int opLeft = getOperator().getLeftPrec();
        final int opRight = getOperator().getRightPrec();
        this.view.unparse(writer, opLeft, opRight);
        this.column.unparse(writer, opLeft, opRight);
        this.expression.unparse(writer, 0, 0);
        writer.newlineAndIndent();
    }

    @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
        // TODO
    }
}
