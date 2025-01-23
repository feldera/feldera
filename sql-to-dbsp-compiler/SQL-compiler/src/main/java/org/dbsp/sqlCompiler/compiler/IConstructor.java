package org.dbsp.sqlCompiler.compiler;

/** Interface implemented by constructor expressions */
public interface IConstructor {
    /** True when the constructed expression has all arguments constant */
    boolean isConstant();
}
