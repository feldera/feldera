package org.dbsp.sqlCompiler.compiler.errors;

import com.fasterxml.jackson.core.JsonLocation;

public class SourcePosition implements Comparable<SourcePosition> {
    public static final SourcePosition INVALID = new SourcePosition(0, 0);

    public final int line;    // Numbered from 1
    public final int column;  // Numbered from 1

    public SourcePosition(int line, int column) {
        this.line = line;
        this.column = column;
    }

    public SourcePosition(JsonLocation location) {
        this.line = location.getLineNr();
        this.column = location.getColumnNr();
    }

    public boolean isValid() {
        return this.line > 0 && this.column > 0;
    }

    @Override
    public String toString() {
        return this.line + ":" + this.column;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        SourcePosition that = (SourcePosition) o;
        return line == that.line && column == that.column;
    }

    public String toRustConstant() {
        return "SourcePosition::new(" + this.line + ", " + this.column + ")";
    }

    @Override
    public int hashCode() {
        int result = line;
        result = 31 * result + column;
        return result;
    }

    public boolean before(SourcePosition end) {
        return this.line < end.line || (this.line == end.line && this.column <= end.column);
    }

    public boolean beforeOrEqual(SourcePosition end) {
        return (this.line < end.line || (this.line == end.line && this.column <= end.column))
                || this.equals(end);
    }

    public SourcePosition min(SourcePosition other) {
        if (this.before(other))
            return this;
        return other;
    }

    public SourcePosition max(SourcePosition other) {
        if (this.before(other))
            return other;
        return this;
    }

    public SourcePositionRange asRange() {
        return new SourcePositionRange(this, this);
    }

    @Override
    public int compareTo(SourcePosition other) {
        int compare = Integer.compare(this.line, other.line);
        if (compare != 0)
            return compare;
        return Integer.compare(this.column, other.column);
    }

    /** We have a position described by this.  But the first character should not
     * have position (1, 0), but rather firstCharPosition.  What is the correct position of this? */
    public SourcePosition relativeTo(SourcePosition firstCharPosition) {
        if (this.line == 1)
            // For the first line the colum needs to be adjusted
            return new SourcePosition(this.line + firstCharPosition.line - 1, this.column + firstCharPosition.column);
        return new SourcePosition(this.line + firstCharPosition.line - 1, this.column);
    }
}
