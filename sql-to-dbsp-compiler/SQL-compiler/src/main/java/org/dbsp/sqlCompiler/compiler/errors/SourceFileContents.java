package org.dbsp.sqlCompiler.compiler.errors;

import org.apache.calcite.sql.SqlNode;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Keep track of the contents of the source file supplied to the compiler. */
public class SourceFileContents {
    /** Currently we can have a single source file, but perhaps some day we will
     * support something like #include.  This is null if the data does not come from a file. */
    public @Nullable String sourceFileName;
    final List<String> lines = new ArrayList<>();
    final StringBuilder builder = new StringBuilder();

    public SourceFileContents() {
        this.sourceFileName = null;
    }

    public void append(String code) {
        this.lines.addAll(Arrays.asList(code.split("\n")));
        this.builder.append(code);
    }

    public String getWholeProgram() {
        return this.builder.toString();
    }

    public static String newline() {
        return System.lineSeparator();
    }

    public String lineNo(int no, boolean decorated) {
        if (!decorated)
            return "";
        return String.format("%5d|", no + 1);
    }

    /** Maximum number of context characters to show before and after an error range on a line. */
    public static final int CONTEXT_CHARS = 100;

    /**
     * Window a context line around [startSlice, endSlice] if it aligns,
     * or limit its length to a bounded window.
     */
    static String windowContextLine(String line, int startSlice, int endSlice) {
        int len = line.length();
        if (len <= 2 * CONTEXT_CHARS && startSlice == 0) {
            return line;
        }
        if (startSlice < len) {
            int end = Math.min(len, endSlice);
            String prefix = startSlice > 0 ? "..." : "";
            String suffix = end < len ? "..." : "";
            return prefix + line.substring(startSlice, end) + suffix;
        } else {
            if (len <= 2 * CONTEXT_CHARS) {
                return line;
            }
            return line.substring(0, 2 * CONTEXT_CHARS) + "...";
        }
    }

    /**
     * Window a line in a multi-line range to keep decorated snippets bounded.
     */
    static String windowMultiLine(String line, int lineIdx, int startLine, int endLine, int startCol, int endCol) {
        int len = line.length();
        if (len <= 2 * CONTEXT_CHARS) {
            return line;
        }
        if (lineIdx == startLine) {
            int safeStart = Math.max(0, Math.min(startCol, len));
            int startSlice = Math.max(0, safeStart - CONTEXT_CHARS);
            int endSlice = Math.min(len, safeStart + CONTEXT_CHARS);
            String prefix = startSlice > 0 ? "..." : "";
            String suffix = endSlice < len ? "..." : "";
            return prefix + line.substring(startSlice, endSlice) + suffix;
        } else if (lineIdx == endLine) {
            int safeEnd = Math.max(0, Math.min(endCol, len));
            int startSlice = Math.max(0, safeEnd - CONTEXT_CHARS);
            int endSlice = Math.min(len, safeEnd + CONTEXT_CHARS);
            String prefix = startSlice > 0 ? "..." : "";
            String suffix = endSlice < len ? "..." : "";
            return prefix + line.substring(startSlice, endSlice) + suffix;
        } else {
            return line.substring(0, 2 * CONTEXT_CHARS) + "...";
        }
    }

    /** Get the source code fragment at the specified position.
     * @param range     Position to extract fragment from.
     * @param decorated If true print additional decorations around, like ^^^^^ */
    public String getFragment(SourcePositionRange range, boolean decorated) {
        if (!range.isValid())
            return "";
        final int startLine = range.start.line - 1;
        int endLine = range.end.line - 1;
        final int startCol = range.start.column - 1;
        final int endCol = range.end.column;
        final StringBuilder result = new StringBuilder();
        if (startLine == endLine) {
            if (startLine >= this.lines.size())
                // This should not really happen.
                return "";

            String line = this.lines.get(startLine);
            if (!decorated) {
                int safeStart = Math.max(0, Math.min(startCol, line.length()));
                int safeEnd = Math.max(safeStart, Math.min(endCol, line.length()));
                result.append(line.substring(safeStart, safeEnd));
                result.append(SourceFileContents.newline());
                return result.toString();
            }

            final int len = line.length();
            final int safeStartCol = Math.max(0, Math.min(startCol, len));
            final int safeEndCol = Math.max(safeStartCol, Math.min(endCol, len));

            final int startSlice = Math.max(0, safeStartCol - CONTEXT_CHARS);
            final int endSlice = Math.min(len, safeEndCol + CONTEXT_CHARS);

            final String prefix = startSlice > 0 ? "..." : "";
            final String suffix = endSlice < len ? "..." : "";
            final String windowedLine = prefix + line.substring(startSlice, endSlice) + suffix;

            if (this.lines.size() - 1 == startLine && this.lines.size() > 1) {
                // Print one previous line to help disambiguate location
                String prevLine = this.lines.get(startLine - 1);
                result.append(lineNo(startLine - 1, decorated))
                        .append(windowContextLine(prevLine, startSlice, endSlice))
                        .append(SourceFileContents.newline());
            }

            result.append(lineNo(startLine, decorated))
                    .append(windowedLine)
                    .append(SourceFileContents.newline());

            int caretOffset = prefix.length() + (safeStartCol - startSlice);
            int caretCount = safeEndCol - safeStartCol;
            result.append(" ".repeat(caretOffset + 6));
            result.append("^".repeat(caretCount));
            result.append(SourceFileContents.newline());

            if (this.lines.size() > startLine + 1) {
                // Print one more line to help disambiguate location
                String nextLine = this.lines.get(startLine + 1);
                result.append(lineNo(startLine + 1, decorated))
                        .append(windowContextLine(nextLine, startSlice, endSlice))
                        .append(SourceFileContents.newline());
            }
        } else {
            // Sometimes this ends on a line which does not exist in the file
            if (endLine == this.lines.size())
                endLine = this.lines.size() - 1;
            if (endLine - startLine < 5 || !decorated) {
                for (int i = startLine; i <= endLine; i++) {
                    String line = this.lines.get(i);
                    if (!decorated) {
                        if (i == startLine)
                            line = line.substring(startCol);
                        else if (i == endLine)
                            line = line.substring(0, endCol);
                    } else {
                        line = windowMultiLine(line, i, startLine, endLine, startCol, endCol);
                    }
                    result.append(this.lineNo(i, decorated))
                            .append(line);
                    result.append(SourceFileContents.newline());
                }
            } else {
                result.append(this.lineNo(startLine, decorated))
                        .append(windowMultiLine(this.lines.get(startLine), startLine, startLine, endLine, startCol, endCol))
                        .append(SourceFileContents.newline())
                        .append(this.lineNo(startLine + 1, decorated))
                        .append(windowMultiLine(this.lines.get(startLine + 1), startLine + 1, startLine, endLine, startCol, endCol))
                        .append(SourceFileContents.newline())
                        .append("      ...")
                        .append(SourceFileContents.newline())
                        .append(this.lineNo(endLine, decorated))
                        .append(windowMultiLine(this.lines.get(endLine), endLine, startLine, endLine, startCol, endCol))
                        .append(SourceFileContents.newline());
            }
        }
        return result.toString();
    }

    public String getFragments(SourcePositionRanges ranges) {
        StringBuilder result = new StringBuilder();
        for (SourcePositionRange r: ranges.positions) {
            result.append(this.getFragment(r, true));
            result.append("\n");
        }
        return result.toString();
     }

    public String getFragment(SqlNode node) {
        return this.getFragment(
                new SourcePositionRange(node.getParserPosition()), false);
    }

    public String getSourceFileName(SourcePosition ignoredPosition) {
        return this.sourceFileName == null ? "(no input file)" : this.sourceFileName;
    }

    public void setEntireInput(@Nullable String sourceFileName, InputStream stream) throws IOException {
        this.sourceFileName = sourceFileName;
        BufferedReader br = new BufferedReader(new InputStreamReader(stream));
        String line;
        while ((line = br.readLine()) != null) {
            this.lines.add(line);
            this.builder.append(line)
                    .append(SourceFileContents.newline());
        }
    }

    public void writeAsJson(IIndentStream stream) {
        stream.append("[").increase();
        stream.joinI(",\n", this.lines, l -> Utilities.doubleQuote(l, false));
        stream.newline().decrease().append("]");
    }
}
