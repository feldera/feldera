package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.util.Utilities;

/** A fragment that contains a SqlIdentifier */
public class SqlFragmentIdentifier implements SqlFragment {
    final SqlIdentifier identifier;

    public SqlFragmentIdentifier(SqlIdentifier identifier) {
        this.identifier = identifier;
    }

    public SqlFragmentIdentifier(String str) {
        this.identifier = new SqlIdentifier(str, SqlParserPos.ZERO);
    }

    @Override
    public String getString() {
        return this.identifier.toString();
    }

    @Override
    public SqlParserPos getParserPosition() {
        return this.identifier.getParserPosition();
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

    public ProgramIdentifier toIdentifier() {
        return Utilities.toIdentifier(this.identifier);
    }
}
