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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        SourcePositionRange that = (SourcePositionRange) o;
        return start.equals(that.start) && end.equals(that.end);
    }

    boolean includes(SourcePositionRange other) {
        return this.start.beforeOrEqual(other.start) && other.end.beforeOrEqual(this.end);
    }

    boolean includes(SourcePosition position) {
        return this.start.beforeOrEqual(position) && position.beforeOrEqual(this.end);
    }

    /** Merge two source position ranges by creating a range that spans both. */
    SourcePositionRange merge(SourcePositionRange other) {
        SourcePosition start = this.start.min(other.start);
        SourcePosition end = this.end.max(other.end);
        return new SourcePositionRange(start, end);
    }

    @Override
    public int hashCode() {
        int result = start.hashCode();
        result = 31 * result + end.hashCode();
        return result;
    }

    public String toShortString() {
        if (!this.isValid())
            return "";
        if (this.start.line == this.end.line)
            return "#" + this.start.line;
        else
            return "#" + this.start.line + "-" + this.end.line;
    }

    /** True if a range ends just before another one starts.
     * Thsi cannot detect the case when the first range ends on a line boundary. */
    public boolean adjacent(SourcePositionRange p) {
        return this.end.line == p.start.line && this.end.column >= p.start.column - 1;
    }
}
