package org.dbsp.sqlCompiler.compiler.errors;

import org.apache.calcite.runtime.CalciteContextException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;

public final class CompilationError extends BaseCompilerException {
    static final String DEFAULT_MESSAGE = "Compilation error";

    /** Title displayed in front of the message, as in {@link
     * org.dbsp.sqlCompiler.compiler.IErrorReporter#reportWarning}. */
    private final String title;

    /** The message of the exception without its position, which the error reports separately */
    static String messageWithoutPosition(CalciteContextException exception) {
        Throwable cause = exception.getCause();
        String message = cause != null ? cause.getMessage() : exception.getMessage();
        return message != null ? message : "Error";
    }

    public CompilationError(CalciteContextException exception) {
        this(messageWithoutPosition(exception),
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
