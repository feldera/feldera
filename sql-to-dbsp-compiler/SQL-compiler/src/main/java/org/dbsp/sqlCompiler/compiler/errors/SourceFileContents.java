package org.dbsp.sqlCompiler.compiler.errors;

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

    public String lineNo(int no) {
        return String.format("%5d|", no + 1);
    }

    public String getFragment(SourcePositionRange range) {
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
            result.append(lineNo(startLine))
                    .append(line)
                    .append(SourceFileContents.newline());
            result.append(" ".repeat(startCol + 6));
            result.append("^".repeat(endCol - startCol));
            result.append(SourceFileContents.newline());
            if (this.lines.size() > startLine + 1) {
                // Print one more line to help disambiguate location
                line = this.lines.get(startLine + 1);
                result.append(lineNo(startLine + 1))
                        .append(line)
                        .append(SourceFileContents.newline());
            }
        } else {
            if (endLine - startLine < 5) {
                for (int i = startLine; i < endLine; i++) {
                    result.append(this.lineNo(i))
                            .append(this.lines.get(i))
                            .append(SourceFileContents.newline());
                }
            } else {
                result.append(this.lineNo(startLine))
                        .append(this.lines.get(startLine))
                        .append(SourceFileContents.newline())
                        .append(this.lineNo(startLine + 1))
                        .append(this.lines.get(startLine + 1))
                        .append(SourceFileContents.newline())
                        .append("      ...")
                        .append(SourceFileContents.newline())
                        .append(this.lineNo(endLine))
                        .append(this.lines.get(endLine))
                        .append(SourceFileContents.newline());
            }
        }
        return result.toString();
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
}
