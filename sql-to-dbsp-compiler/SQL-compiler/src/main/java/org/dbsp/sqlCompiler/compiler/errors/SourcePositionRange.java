package org.dbsp.sqlCompiler.compiler.errors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.IHasSourcePositionRange;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

/** A range of characters inside the source code. */
public class SourcePositionRange implements IHasSourcePositionRange {
    public final SourcePosition start;
    public final SourcePosition end;

    public static final SourcePositionRange INVALID =
            new SourcePositionRange(SourcePosition.INVALID, SourcePosition.INVALID);

    public SourcePositionRange(SourcePosition start, SourcePosition end) {
        this.start = start;
        this.end = end;
    }

    public SourcePositionRange(@Nullable SqlParserPos pos) {
        if (pos == null) {
            this.start = SourcePosition.INVALID;
            this.end = SourcePosition.INVALID;
        } else {
            this.start = new SourcePosition(pos.getLineNum(), pos.getColumnNum());
            this.end = new SourcePosition(pos.getEndLineNum(), pos.getEndColumnNum());
        }
    }

    public boolean isValid() {
        return this.start.isValid() && this.end.isValid();
    }

    @Override
    public String toString() {
        return this.start + "--" + this.end;
    }

    @Override
    public SourcePositionRange getPositionRange() {
        return this;
    }

    /** Append the position information to a JSON node */
    public void appendAsJson(ObjectNode parent) {
        parent.put("start_line_number", this.start.line);
        parent.put("start_column", this.start.column);
        parent.put("end_line_number", this.end.line);
        parent.put("end_column", this.end.column);
    }

    public JsonNode asJson() {
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        this.appendAsJson(result);
        return result;
    }

    public String toRustConstant() {
        return "\"line " + this.start.line + " column " + this.start.column + "\"";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        SourcePositionRange that = (SourcePositionRange) o;
        return start.equals(that.start) && end.equals(that.end);
    }

    @Override
    public int hashCode() {
        int result = start.hashCode();
        result = 31 * result + end.hashCode();
        return result;
    }
}
