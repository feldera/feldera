package org.dbsp.simulator.util;

import javax.annotation.Nonnull;
import javax.annotation.meta.TypeQualifierDefault;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Applies the {@link Nonnull} annotation to every field unless overridden.
 */
@Documented
@Nonnull
@TypeQualifierDefault(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FieldsAreNonnullByDefault
{}
