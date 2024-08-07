package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlIdentifier;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

/** A fragment that contains a SqlIdentifier */
public class SqlFragmentIdentifier implements SqlFragment {
    final SqlIdentifier identifier;

    public SqlFragmentIdentifier(SqlIdentifier identifier) {
        this.identifier = identifier;
    }

    @Override
    public String getString() {
        return this.identifier.toString();
    }

    @Override
    public SourcePositionRange getSourcePosition() {
        return new SourcePositionRange(this.identifier.getParserPosition());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SqlFragmentIdentifier that = (SqlFragmentIdentifier) o;
        return this.identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return this.identifier.hashCode();
    }

    @Override
    public String toString() {
        return this.getString();
    }
}
