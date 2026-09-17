package org.dbsp.sqlCompiler.compiler.errors;

import org.apache.calcite.runtime.CalciteContextException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;

public final class CompilationError extends BaseCompilerException {
    static final String DEFAULT_MESSAGE = "Compilation error";

    /** Title displayed in front of the message, as in {@link
     * org.dbsp.sqlCompiler.compiler.IErrorReporter#reportWarning}. */
    private final String title;

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
        this.title = DEFAULT_MESSAGE;
    }

    public CompilationError(String message, SourcePositionRange range) {
        super(message, range);
        this.title = DEFAULT_MESSAGE;
    }

    public CompilationError(SourcePositionRange range, String title, String message) {
        super(message, range);
        this.title = title;
    }

    @Override
    public String getErrorKind() {
        return this.title;
    }
}
