package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

public interface IHasSourcePositionRange {
    SourcePositionRange getPositionRange();
}
