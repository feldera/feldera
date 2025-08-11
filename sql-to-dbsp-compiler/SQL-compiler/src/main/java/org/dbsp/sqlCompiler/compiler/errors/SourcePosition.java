package org.dbsp.sqlCompiler.compiler.errors;

public class SourcePosition {
    public static final SourcePosition INVALID = new SourcePosition(0, 0);

    public final int line;    // Numbered from 1
    public final int column;  // Numbered from 1

    public SourcePosition(int line, int column) {
        this.line = line;
        this.column = column;
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
}
