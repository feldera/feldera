package org.dbsp.util;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;

public interface IValidate {
    /** Report errors if the object is invalid.
     * Return 'true' if it is valid. */
    boolean validate(IErrorReporter reporter);
}
