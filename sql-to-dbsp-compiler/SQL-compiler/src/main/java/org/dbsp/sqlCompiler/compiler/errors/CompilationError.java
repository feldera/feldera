package org.dbsp.sqlCompiler.compiler.errors;

import org.apache.calcite.runtime.CalciteContextException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;

public final class CompilationError extends BaseCompilerException {
    public CompilationError(CalciteContextException exception) {
        this(exception.getMessage() != null ? exception.getMessage() : "Error",
                new SourcePositionRange(
                        new SourcePosition(exception.getPosLine(), exception.getPosColumn()),
                        new SourcePosition(exception.getEndPosLine(), exception.getEndPosColumn())));
    }

    public CompilationError(String message) {
        this(message, CalciteObject.EMPTY);
    }

    public CompilationError(String message, CalciteObject object) {
        super(message, object);
    }

    public CompilationError(String message, SourcePositionRange range) {
        super(message, range);
    }

    @Override
    public String getErrorKind() {
        return "Compilation error";
    }
}
