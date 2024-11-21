package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.ICastable;

/** Represents a sequence of characters that is part of the SQL program */
public interface SqlFragment extends Comparable<SqlFragment>, ICastable {
    /** Fragment as a string */
    String getString();
    /** Source position of the fragment */
    SqlParserPos getParserPosition();
    /** Source position of the fragment */
    default SourcePositionRange getSourcePosition() {
        return new SourcePositionRange(this.getParserPosition());
    }

    @Override
    default int compareTo(SqlFragment sqlFragment) {
        return this.getString().compareTo(sqlFragment.getString());
    }
}
