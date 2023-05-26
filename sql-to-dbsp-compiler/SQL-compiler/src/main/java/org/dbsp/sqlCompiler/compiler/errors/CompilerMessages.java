package org.dbsp.sqlCompiler.compiler.errors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.compiler.frontend.statements.FrontEndStatement;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CompilerMessages {
    public class Error {
        public final SourcePositionRange range;
        public final boolean warning;
        public final String errorType;
        public final String message;

        Error(SourcePositionRange range, boolean warning, String errorType, String message) {
            this.range = range;
            this.warning = warning;
            this.errorType = errorType;
            this.message = message;
        }

        Error(SqlParseException e) {
            this.range = new SourcePositionRange(e.getPos());
            this.warning = false;
            this.errorType = "Error parsing SQL";
            this.message = e.getMessage();
        }

        Error(CalciteContextException e) {
            this.range = new SourcePositionRange(
                    new SourcePosition(e.getPosLine(), e.getPosColumn()),
                    new SourcePosition(e.getEndPosLine(), e.getEndPosColumn()));
            this.warning = false;
            this.errorType = "Error in SQL statement";
            String message;
            if (e.getCause() != null)
                message = e.getCause().getMessage();
            else if (e.getMessage() != null)
                message = e.getMessage();
            else
                message = "";
            this.message = Objects.requireNonNull(message);
        }

        Error(Throwable e) {
            this.range = SourcePositionRange.INVALID;
            this.warning = false;
            this.errorType = "This is a bug in the compiler (please report it to the developers)";
            this.message = e.getMessage();
        }

        Error(Unimplemented e) {
            this.range = CompilerMessages.getPositionRange(e.object);
            this.errorType = "Feature not yet implemented";
            this.warning = false;
            this.message = e.getMessage();
        }

        public void format(SourceFileContents contents, StringBuilder output) {
            String sourceFile = contents.getSourceFileName(this.range.start);
            output.append(sourceFile)
                    .append(": ")
                    .append(this.errorType)
                    .append(SourceFileContents.newline());
            output.append(sourceFile)
                    .append(":")
                    .append(this.range.start)
                    .append(":");
            if (this.warning)
                output.append(" warning");
            else
                output.append(" error");
            output.append(": ")
                    .append(this.message)
                    .append(SourceFileContents.newline());
            output.append(contents.getFragment(this.range));
        }

        public JsonNode toJson(ObjectMapper mapper) {
            ObjectNode result = mapper.createObjectNode();
            result.put("startLineNumber", this.range.start.line);
            result.put("startColumn", this.range.start.column);
            result.put("endLineNumber", this.range.end.line);
            result.put("endColumn", this.range.end.column);
            result.put("warning", this.warning);
            result.put("errorType", this.errorType);
            result.put("message", this.message);
            return result;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            this.format(CompilerMessages.this.compiler.sources, builder);
            return builder.toString();
        }
    }

    public final DBSPCompiler compiler;
    public final List<Error> messages;
    public int exitCode = 0;

    public CompilerMessages(DBSPCompiler compiler) {
        this.compiler = compiler;
        this.messages = new ArrayList<>();
    }

    public void setExitCode(int exitCode) {
        this.exitCode = exitCode;
    }

    void reportError(Error message) {
        this.messages.add(message);
        if (!message.warning)
            this.setExitCode(1);
    }

    public void reportError(SourcePositionRange range, boolean warning,
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

    public void reportError(Unimplemented e) {
        this.reportError(new Error(e));
    }

    public void reportError(Throwable e) {
        this.reportError(new Error(e));
    }

    public int errorCount() {
        return this.messages.size();
    }

    public Error getError(int ct) {
        return this.messages.get(ct);
    }

    public static SourcePositionRange getPositionRange(@Nullable Object object) {
        if (object == null)
            return SourcePositionRange.INVALID;
        if (object instanceof SqlNode) {
            SqlNode node = (SqlNode) object;
            return new SourcePositionRange(node.getParserPosition());
        } else if (object instanceof FrontEndStatement){
            FrontEndStatement stat = (FrontEndStatement) object;
            SqlNode node = stat.getNode();
            if (node != null)
                return new SourcePositionRange(node.getParserPosition());
        }
        return SourcePositionRange.INVALID;
    }

    public void show(PrintStream stream) {
        stream.println(this);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (this.compiler.options.ioOptions.emitJsonErrors) {
            JsonNode node = this.toJson();
            builder.append(node.toPrettyString());
        } else {
            for (Error message: this.messages) {
                message.format(this.compiler.sources, builder);
            }
        }
        return builder.toString();
    }

    public JsonNode toJson() {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode result = mapper.createArrayNode();
        for (Error message: this.messages) {
            JsonNode node = message.toJson(mapper);
            result.add(node);
        }
        return result;
    }
}
