package org.dbsp.sqlCompiler.compiler.sql.tools;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Marks a test that needs additional support.
 * A mark on a class applies to all its tests and to the tests of its subclasses. */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
@Inherited
public @interface NeedsSupport {
    /** What the runtime must support to run the test */
    String value() default "";
}
