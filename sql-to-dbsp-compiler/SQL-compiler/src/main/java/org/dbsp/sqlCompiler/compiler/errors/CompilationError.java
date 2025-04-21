package org.dbsp.sqlCompiler.compiler.errors;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;

public final class CompilationError extends BaseCompilerException {
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
