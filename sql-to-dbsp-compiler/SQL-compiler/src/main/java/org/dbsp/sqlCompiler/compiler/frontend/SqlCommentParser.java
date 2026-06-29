package org.dbsp.sqlCompiler.compiler.frontend;

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Scans SQL source text for comments, recording each comment's text and
 * position in the original source.
 *
 * <p>Correctly skips string literals ({@code '...'}), double-quoted
 * identifiers ({@code "..."}), and backtick-quoted identifiers so that
 * comment-like sequences inside them are not mistaken for real comments.
 *
 * <p>Recognised comment forms:
 * <ul>
 *   <li>{@code --} to end of line (SQL standard single-line comment)</li>
 *   <li>{@code //} to end of line (C++-style; also accepted by Calcite)</li>
 *   <li>{@code /* ... *\/} (block comment, may span multiple lines)</li>
 * </ul>
 *
 * <p>Calcite query hints ({@code /*+ ... *\/}) are <em>not</em> included in the
 * output: they are already represented in the {@code SqlNode} tree and handled
 * separately by {@link SqlPrettyPrinter}.
 *
 * <p>The input is assumed to be syntactically valid SQL (as accepted by
 * Calcite).  Unterminated block comments or string literals are consumed
 * silently to end-of-input; the resulting output may be malformed.
 */
public class SqlCommentParser {
    private final String sql; // source text being scanned
    private final int n;      // cached length of sql
    private int i;            // scan cursor (index into sql)
    private int line;         // current line number, 1-based
    private int col;          // current column number, 1-based

    private SqlCommentParser(final String sql) {
        this.sql = sql;
        this.n = sql.length();
        this.i = 0;
        this.line = 1;
        this.col = 1;
    }

    /** Extract all comments from {@code sql}, in source order. */
    public static List<SqlComment> parse(final String sql) {
        return new SqlCommentParser(sql).scan();
    }

    private List<SqlComment> scan() {
        final List<SqlComment> result = new ArrayList<>();

        while (this.i < this.n) {
            final char c = this.current();

            // single-line comment: -- or //
            if ((c == '-' && this.peekNext() == '-')
                    || (c == '/' && this.peekNext() == '/')) {
                final SqlComment.Kind kind = (c == '-') ? SqlComment.Kind.LINE_DASH : SqlComment.Kind.LINE_SLASH;
                final int start = this.i;
                final SqlParserPos pos = new SqlParserPos(this.line, this.col);
                this.nextChar(); // consume first marker char
                this.nextChar(); // consume second marker char
                while (this.i < this.n && this.current() != '\n' && this.current() != '\r') {
                    this.nextChar();
                }
                if (this.i < this.n) {
                    this.nextChar(); // consume line terminator; \r\n handled as one unit
                }
                result.add(new SqlComment(kind, requireNonNull(this.sql.substring(start, this.i)), pos));
                continue;
            }

            // block comment: /* ... */
            // Calcite query hints /*+ ... */ are not collected: they are already
            // represented in the SqlNode tree and emitted by SqlPrettyPrinter separately.
            if (c == '/' && this.peekNext() == '*') {
                final int start = this.i;
                final SqlParserPos pos = new SqlParserPos(this.line, this.col);
                this.nextChar(); // consume '/'
                this.nextChar(); // consume '*'
                final boolean isHint = this.i < this.n && this.current() == '+';
                while (this.i < this.n) {
                    if (this.current() == '*' && this.peekNext() == '/') {
                        this.nextChar(); // consume '*'
                        this.nextChar(); // consume '/'
                        break;
                    }
                    this.nextChar();
                }
                if (!isHint) {
                    result.add(new SqlComment(SqlComment.Kind.BLOCK, requireNonNull(this.sql.substring(start, this.i)), pos));
                }
                continue;
            }

            // single-quoted string literal: '...'
            if (c == '\'') {
                this.nextChar(); // consume opening '
                while (this.i < this.n) {
                    if (this.current() == '\'') {
                        this.nextChar(); // consume '
                        if (this.i < this.n && this.current() == '\'') {
                            this.nextChar(); // '' is an escaped single quote; keep scanning
                        } else {
                            break;
                        }
                    } else {
                        this.nextChar();
                    }
                }
                continue;
            }

            // double-quoted identifier: "..."
            if (c == '"') {
                this.nextChar(); // consume opening "
                while (this.i < this.n) {
                    if (this.current() == '"') {
                        this.nextChar(); // consume "
                        if (this.i < this.n && this.current() == '"') {
                            this.nextChar(); // "" is an escaped double-quote; keep scanning
                        } else {
                            break;
                        }
                    } else {
                        this.nextChar();
                    }
                }
                continue;
            }

            // backtick-quoted identifier: `...`
            if (c == '`') {
                this.nextChar(); // consume opening `
                while (this.i < this.n) {
                    if (this.current() == '`') {
                        this.nextChar(); // consume `
                        if (this.i < this.n && this.current() == '`') {
                            this.nextChar(); // `` is an escaped backtick; keep scanning
                        } else {
                            break;
                        }
                    } else {
                        this.nextChar();
                    }
                }
                continue;
            }

            // ordinary character
            this.nextChar();
        }

        return result;
    }

    /** Returns the character at the current scan position; caller must ensure {@code i < n}. */
    private char current() {
        return this.sql.charAt(this.i);
    }

    /** Returns the character one position ahead of the cursor, or {@code 0} if out of bounds. */
    private char peekNext() {
        final int next = this.i + 1;
        return next < this.n ? this.sql.charAt(next) : 0;
    }

    /**
     * Consumes and returns the current character, advancing the cursor and
     * updating line/column tracking.  A {@code \r\n} pair counts as one line
     * ending: this method skips the {@code \n} when the current character is
     * {@code \r} and the next is {@code \n}.
     */
    private char nextChar() {
        final char c = this.sql.charAt(this.i++);
        if (c == '\n') {
            this.line++;
            this.col = 1;
        } else if (c == '\r') {
            this.line++;
            this.col = 1;
            if (this.i < this.n && this.sql.charAt(this.i) == '\n') {
                this.i++; // skip the \n of a \r\n pair
            }
        } else {
            this.col++;
        }
        return c;
    }
}
