package org.dbsp.sqlCompiler.compiler.errors;

import org.dbsp.sqlCompiler.compiler.IHasSourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;

import javax.annotation.Nullable;

/** Base class for exceptions which are thrown by the compiler. */
public abstract class BaseCompilerException
        extends RuntimeException
        implements IHasSourcePositionRange {
    public final CalciteObject calciteObject;

    protected BaseCompilerException(String message, CalciteObject calciteObject, @Nullable Throwable throwable) {
        super(message, throwable);
        this.calciteObject = calciteObject;
    }

    protected BaseCompilerException(String message, SourcePositionRange range) {
        this(message, CalciteObject.create(range), null);
    }

    protected BaseCompilerException(String message, CalciteObject calciteObject) {
        this(message, calciteObject, null);
    }

    public SourcePositionRange getPositionRange() {
        return this.calciteObject.getPositionRange();
    }

    public abstract String getErrorKind();
}
