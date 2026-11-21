package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.Objects;
import java.util.function.UnaryOperator;

/** Detached presentation metadata for a statement already bound at S1.
 * Semantic information lives in the typed RelStatement, not in this node.
 * Attempts to run front-end validation or an AST visitor fail explicitly. */
public final class FelderaS1StatementSource extends SqlNode {
    private final SqlKind kind;
    private final String statementText;
    private final String diagnosticText;

    public FelderaS1StatementSource(SqlParserPos position, SqlKind kind, String statementText, String diagnosticText) {
        super(position);
        this.kind = Objects.requireNonNull(kind);
        this.statementText = Objects.requireNonNull(statementText);
        this.diagnosticText = Objects.requireNonNull(diagnosticText);
    }
    @Override public SqlKind getKind() { return this.kind; }
    @Override public String toString() { return this.statementText; }
    @Override public SqlNode clone(SqlParserPos position) {
        return new FelderaS1StatementSource(position, this.kind, this.statementText, this.diagnosticText);
    }
    @Override public SqlString toSqlString(UnaryOperator<SqlWriterConfig> configure) {
        return new SqlString(SqlDialect.DatabaseProduct.POSTGRESQL.getDialect(), this.diagnosticText);
    }
    @Override public void unparse(SqlWriter writer, int left, int right) { writer.print(this.diagnosticText); }
    @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
        throw new UnsupportedOperationException("Detached S1 statement metadata cannot be validated as SQL");
    }
    @Override public <R> R accept(SqlVisitor<R> visitor) {
        throw new UnsupportedOperationException("Detached S1 statement metadata has no syntax tree");
    }
    @Override public boolean equalsDeep(SqlNode other, Litmus litmus) {
        boolean equal = other instanceof FelderaS1StatementSource source && this.kind == source.kind
                && this.pos.equals(source.pos) && this.statementText.equals(source.statementText)
                && this.diagnosticText.equals(source.diagnosticText);
        return equal ? litmus.succeed() : litmus.fail("Detached statement metadata differs");
    }
}
