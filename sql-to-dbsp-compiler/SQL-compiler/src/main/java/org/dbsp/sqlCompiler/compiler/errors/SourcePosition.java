package org.dbsp.sqlCompiler.compiler.errors;

class SourcePosition {
    public static final SourcePosition INVALID = new SourcePosition(0, 0);

    public final int line;    // Numbered from 1
    public final int column;  // Numbered from 1

    SourcePosition(int line, int column) {
        this.line = line;
        this.column = column;
    }

    boolean isValid() {
        return this.line > 0 && this.column > 0;
    }

    @Override
    public String toString() {
        return this.line + ":" + this.column;
    }
}
