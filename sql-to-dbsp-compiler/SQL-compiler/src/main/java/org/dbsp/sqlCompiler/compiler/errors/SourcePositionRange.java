package org.dbsp.sqlCompiler.compiler.errors;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.IHasSourcePositionRange;

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
}
