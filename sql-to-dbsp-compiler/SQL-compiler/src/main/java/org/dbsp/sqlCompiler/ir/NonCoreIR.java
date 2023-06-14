package org.dbsp.sqlCompiler.ir;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation indicates that an IR class is not part of the
 * "core" IR - i.e., it does not surface directly in the implementation
 * of SQL queries, but it is only used for test or library generation.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface NonCoreIR { }
