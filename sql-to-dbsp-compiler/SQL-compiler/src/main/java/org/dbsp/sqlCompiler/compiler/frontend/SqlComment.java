package org.dbsp.sqlCompiler.compiler.frontend;

import org.apache.calcite.sql.parser.SqlParserPos;

/** A SQL comment extracted from the original source text, with its source position. */
public class SqlComment {
    public enum Kind {
        /** {@code -- ...} to end of line (SQL standard). */
        LINE_DASH,
        /** {@code // ...} to end of line (C++ style). */
        LINE_SLASH,
        /** {@code /* ... *\/} possibly spanning multiple lines. */
        BLOCK
    }

    public final Kind kind;
    /** Full comment text, including delimiters (e.g. {@code -- foo\n} or {@code /* bar *\/}). */
    public final String text;
    /** Position of the first character of the comment in the original source. */
    public final SqlParserPos pos;

    public SqlComment(Kind kind, String text, SqlParserPos pos) {
        this.kind = kind;
        this.text = text;
        this.pos = pos;
    }

    @Override
    public String toString() {
        return pos + ": " + text.trim();
    }
}
