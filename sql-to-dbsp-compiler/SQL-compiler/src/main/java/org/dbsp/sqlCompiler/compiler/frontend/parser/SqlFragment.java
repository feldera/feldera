package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

/** Represents a sequence of characters that is part of the SQL program */
public interface SqlFragment extends Comparable<SqlFragment> {
    /** Fragment as a string */
    String getString();
    /** Source position of the fragment */
    SourcePositionRange getSourcePosition();

    @Override
    default int compareTo(SqlFragment sqlFragment) {
        return this.getString().compareTo(sqlFragment.getString());
    }
}
