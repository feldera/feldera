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

    /** In a long line, a message shows at most this many characters before a range,
     * of the range, and after the range */
    static final int CONTEXT = 100;
    /** A message shows lines of up to this length whole */
    static final int LONG_LINE = 2 * CONTEXT;
    static final String ELLIPSIS = "...";

    /** The part of a source line that a message shows.
     * @param text        The part, with {@link #ELLIPSIS} where the line is cut.
     * @param shift       Subtract from a column of the line to get its column in {@code text}.
     * @param rangeStart  Start column, in the line, of the part of the range that {@code text} shows.
     * @param rangeEnd    End column, in the line, of the part of the range that {@code text} shows. */
    record Clip(String text, int shift, int rangeStart, int rangeEnd) {}

    /** The part of {@code line} that a message shows for the range of columns [from, to). */
    static Clip clip(String line, int from, int to) {
        if (line.length() <= LONG_LINE)
            return new Clip(line, 0, from, to);
        int rangeStart = Math.min(from, line.length());
        int rangeEnd = Math.max(rangeStart, Math.min(Math.min(to, rangeStart + CONTEXT), line.length()));
        int start = Math.max(0, rangeStart - CONTEXT);
        int end = Math.min(line.length(), rangeEnd + CONTEXT);
        String prefix = start > 0 ? ELLIPSIS : "";
        String suffix = end < line.length() ? ELLIPSIS : "";
        String text = prefix + line.substring(start, end) + suffix;
        return new Clip(text, start - prefix.length(), rangeStart, rangeEnd);
    }

    /** The part of line {@code i} that a message shows for a range from column {@code startCol}
     * of line {@code startLine} to column {@code endCol} of a later line {@code endLine}. */
    String multiLineClip(int i, int startLine, int startCol, int endLine, int endCol) {
        String line = this.lines.get(i);
        if (i == startLine)
            return clip(line, startCol, line.length()).text();
        if (i == endLine)
            return clip(line, endCol, endCol).text();
        return clip(line, 0, line.length()).text();
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
            if (this.lines.size() - 1 == startLine && this.lines.size() > 1) {
                // Print one previous line to help disambiguate location
                if (decorated) {
                    String prevLine = this.lines.get(startLine - 1);
                    result.append(lineNo(startLine - 1, decorated))
                            .append(clip(prevLine, startCol, endCol).text())
                            .append(SourceFileContents.newline());
                }
            }

            String line = this.lines.get(startLine);
            if (decorated) {
                Clip clip = clip(line, startCol, endCol);
                result.append(lineNo(startLine, decorated))
                        .append(clip.text())
                        .append(SourceFileContents.newline())
                        .append(" ".repeat(clip.rangeStart() - clip.shift() + 6))
                        .append("^".repeat(clip.rangeEnd() - clip.rangeStart()));
            } else {
                result.append(line, startCol, endCol);
            }
            result.append(SourceFileContents.newline());
            if (this.lines.size() > startLine + 1) {
                // Print one more line to help disambiguate location
                if (decorated) {
                    String nextLine = this.lines.get(startLine + 1);
                    result.append(lineNo(startLine + 1, decorated))
                            .append(clip(nextLine, startCol, endCol).text())
                            .append(SourceFileContents.newline());
                }
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
                        line = this.multiLineClip(i, startLine, startCol, endLine, endCol);
                    }
                    result.append(this.lineNo(i, decorated))
                            .append(line);
                    result.append(SourceFileContents.newline());
                }
            } else {
                result.append(this.lineNo(startLine, decorated))
                        .append(this.multiLineClip(startLine, startLine, startCol, endLine, endCol))
                        .append(SourceFileContents.newline())
                        .append(this.lineNo(startLine + 1, decorated))
                        .append(this.multiLineClip(startLine + 1, startLine, startCol, endLine, endCol))
                        .append(SourceFileContents.newline())
                        .append("      ...")
                        .append(SourceFileContents.newline())
                        .append(this.lineNo(endLine, decorated))
                        .append(this.multiLineClip(endLine, startLine, startCol, endLine, endCol))
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
