package org.dbsp.sqlCompiler.compiler.errors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.IHasSourcePositionRange;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class CompilerMessages {
    public class Error implements IHasSourcePositionRange {
        public final SourcePositionRange range;
        public final boolean warning;
        public final String errorType;
        public final String message;

        protected Error(IHasSourcePositionRange range, boolean warning, String errorType, String message) {
            this.range = range.getPositionRange();
            this.warning = warning;
            this.errorType = errorType;
            this.message = message;
        }

        Error(SqlParseException e) {
            this(new SourcePositionRange(e.getPos()), false, "Error parsing SQL", e.getMessage());
        }

        Error(CalciteContextException e) {
            this(new SourcePositionRange(
                    new SourcePosition(e.getPosLine(), e.getPosColumn()),
                    new SourcePosition(e.getEndPosLine(), e.getEndPosColumn())),
            false, "Error in SQL statement",
                (e.getCause() != null) ? e.getCause().getMessage() :
                        (e.getMessage() != null) ? e.getMessage() : "");
        }

        Error(Throwable e) {
            this(SourcePositionRange.INVALID, false,
                    "This is a bug in the compiler (please report it to the developers)", e.getMessage());
        }

        Error(BaseCompilerException e) {
            this(e.getPositionRange(), false, e.getErrorKind(), e.getMessage());
        }

        public void format(SourceFileContents contents, StringBuilder output) {
            if (this.range.isValid()) {
                String sourceFile = contents.getSourceFileName(this.range.start);
                output.append(sourceFile)
                        .append(": ")
                        .append(this.errorType)
                        .append(SourceFileContents.newline());
                output.append(sourceFile)
                        .append(":")
                        .append(this.range.start)
                        .append(": ");
            }
            if (this.warning)
                output.append("warning:");
            else
                output.append("error:");
            output.append(" ")
                    .append(this.errorType)
                    .append(": ")
                    .append(this.message)
                    .append(SourceFileContents.newline());
            output.append(contents.getFragment(this.range));
        }

        public JsonNode toJson(SourceFileContents contents, ObjectMapper mapper) {
            ObjectNode result = mapper.createObjectNode();
            result.put("startLineNumber", this.range.start.line);
            result.put("startColumn", this.range.start.column);
            result.put("endLineNumber", this.range.end.line);
            result.put("endColumn", this.range.end.column);
            result.put("warning", this.warning);
            result.put("errorType", this.errorType);
            result.put("message", this.message);
            String snippet = contents.getFragment(this.range);
            result.put("snippet", snippet);
            return result;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            this.format(CompilerMessages.this.compiler.sources, builder);
            return builder.toString();
        }

        @Override
        public SourcePositionRange getPositionRange() {
            return this.range;
        }
    }

    public final DBSPCompiler compiler;
    public final List<Error> messages;
    public int exitCode = 0;

    public CompilerMessages(DBSPCompiler compiler) {
        this.compiler = compiler;
        this.messages = new ArrayList<>();
    }

    public void clear() {
        this.messages.clear();
    }

    public void setExitCode(int exitCode) {
        this.exitCode = exitCode;
    }

    void reportError(Error message) {
        this.messages.add(message);
        if (!message.warning) {
            this.setExitCode(1);
        }
    }

    public void reportProblem(IHasSourcePositionRange range, boolean warning,
                              String errorType, String message) {
        Error msg = new Error(range, warning, errorType, message);
        this.reportError(msg);
    }

    public void reportError(CalciteContextException e) {
        this.reportError(new Error(e));
    }

    public void reportError(SqlParseException e) {
        this.reportError(new Error(e));
    }

    public void reportError(BaseCompilerException e) {
        this.reportError(new Error(e));
    }

    public void reportError(Throwable e) {
        this.reportError(new Error(e));
    }

    public int errorCount() {
        return (int)this.messages.stream().filter(m -> !m.warning).count();
    }

    public int warningCount() {
        return (int)this.messages.stream().filter(m -> m.warning).count();
    }

    public Error getError(int ct) {
        return this.messages.get(ct);
    }

    public void show(PrintStream stream) {
        if (this.errorCount() +
                (this.compiler.options.ioOptions.quiet ? 0 : this.warningCount()) > 0)
            stream.println(this);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (this.compiler.options.ioOptions.emitJsonErrors) {
            JsonNode node = this.toJson(this.compiler.sources);
            builder.append(node.toPrettyString());
        } else {
            for (Error message: this.messages) {
                if (this.compiler.options.ioOptions.quiet && message.warning)
                    continue;
                message.format(this.compiler.sources, builder);
            }
        }
        return builder.toString();
    }

    public boolean isEmpty() {
        return this.messages.isEmpty();
    }

    public JsonNode toJson(SourceFileContents contents) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode result = mapper.createArrayNode();
        for (Error message: this.messages) {
            JsonNode node = message.toJson(contents, mapper);
            result.add(node);
        }
        return result;
    }
}
