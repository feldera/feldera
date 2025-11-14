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

    /** Get the source code fragment at the specified position.
     * @param range     Position to extract fragment from.
     * @param decorated If true print additional decorations around, like ^^^^^ */
    public String getFragment(SourcePositionRange range, boolean decorated) {
        if (!range.isValid())
            return "";
        int startLine = range.start.line - 1;
        int endLine = range.end.line - 1;
        int startCol = range.start.column - 1;
        int endCol = range.end.column;
        StringBuilder result = new StringBuilder();
        if (startLine == endLine) {
            if (startLine >= this.lines.size())
                // This should not really happen.
                return "";
            String line = this.lines.get(startLine);
            if (!decorated)
                line = line.substring(startCol, endCol);
            result.append(lineNo(startLine, decorated))
                    .append(line);
            if (decorated)
                result.append(SourceFileContents.newline());
            if (decorated) {
                result.append(" ".repeat(startCol + 6));
                result.append("^".repeat(endCol - startCol));
            }
            result.append(SourceFileContents.newline());
            if (this.lines.size() > startLine + 1) {
                // Print one more line to help disambiguate location
                if (decorated) {
                    line = this.lines.get(startLine + 1);
                    result.append(lineNo(startLine + 1, decorated))
                            .append(line)
                            .append(SourceFileContents.newline());
                }
            }
        } else {
            if (endLine - startLine < 5 || !decorated) {
                for (int i = startLine; i <= endLine; i++) {
                    String line = this.lines.get(i);
                    if (!decorated) {
                        if (i == startLine)
                            line = line.substring(startCol);
                        else if (i == endLine)
                            line = line.substring(0, endCol);
                    }
                    result.append(this.lineNo(i, decorated))
                            .append(line);
                    result.append(SourceFileContents.newline());
                }
            } else {
                result.append(this.lineNo(startLine, decorated))
                        .append(this.lines.get(startLine))
                        .append(SourceFileContents.newline())
                        .append(this.lineNo(startLine + 1, decorated))
                        .append(this.lines.get(startLine + 1))
                        .append(SourceFileContents.newline())
                        .append("      ...")
                        .append(SourceFileContents.newline())
                        .append(this.lineNo(endLine, decorated))
                        .append(this.lines.get(endLine))
                        .append(SourceFileContents.newline());
            }
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
