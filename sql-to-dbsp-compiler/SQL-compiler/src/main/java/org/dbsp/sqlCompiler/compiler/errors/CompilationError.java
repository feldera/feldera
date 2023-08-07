package org.dbsp.sqlCompiler.compiler.errors;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;

public class CompilationError extends BaseCompilerException {
    public CompilationError(String message) {
        super(message, CalciteObject.EMPTY);
    }

    @Override
    public String getErrorKind() {
        return "Compilation error";
    }
}
