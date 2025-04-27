package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
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

/** This implements our own extension of SQL DML statement.
 * The REMOVE statement is almost like an INSERT statement,
 * but the effect is to remove a value from a table.
 * Unlike DELETE, the values are not specified by a query. */
public class SqlRemove extends SqlCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("REMOVE", SqlKind.DELETE) {
                @SuppressWarnings("argument.type.incompatible")
                @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                                                    SqlParserPos pos,
                                                    @Nullable SqlNode... operands) {
                    return new SqlRemove(
                            pos,
                            Objects.requireNonNull(operands[0]),
                            Objects.requireNonNull(operands[1]),
                            (SqlNodeList) operands[2]);
                }
            };

    SqlNode targetTable;
    SqlNode source;
    @Nullable SqlNodeList columnList;

    public SqlRemove(SqlParserPos pos,
                     SqlNode targetTable,
                     SqlNode source,
                     @Nullable SqlNodeList columnList) {
        super(pos);
        this.targetTable = targetTable;
        this.source = source;
        this.columnList = columnList;
    }

    @Override public SqlKind getKind() {
        return SqlKind.DELETE;
    }

    @Override public SqlOperator getOperator() {
        return OPERATOR;
    }

    @SuppressWarnings("nullness")
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(targetTable, source);
    }

    @SuppressWarnings("assignment.type.incompatible")
    @Override public void setOperand(int i, @Nullable SqlNode operand) {
        switch (i) {
            case 0:
                Utilities.enforce(operand instanceof SqlIdentifier);
                this.targetTable = operand;
                break;
            case 1:
                this.source = Objects.requireNonNull(operand);
                break;
            case 3:
                this.columnList = (SqlNodeList) operand;
                break;
            default:
                throw new AssertionError(i);
        }
    }

    /** Return the identifier for the target table of the removal. */
    public SqlNode getTargetTable() {
        return targetTable;
    }

    /** Returns the source expression for the data to be removed. */
    public SqlNode getSource() {
        return source;
    }

    public void setSource(SqlSelect source) {
        this.source = source;
    }

    @Pure
    public @Nullable SqlNodeList getTargetColumnList() {
        return this.columnList;
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("REMOVE FROM");
        final int opLeft = getOperator().getLeftPrec();
        final int opRight = getOperator().getRightPrec();
        this.targetTable.unparse(writer, opLeft, opRight);
        if (this.columnList != null) {
            this.columnList.unparse(writer, opLeft, opRight);
        }
        writer.newlineAndIndent();
        this.source.unparse(writer, 0, 0);
    }

    @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
        // validator.validateInsert(this);
        // TODO
    }
}
