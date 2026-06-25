package org.dbsp.sqlCompiler.compiler.frontend.connectors;

import org.dbsp.util.Utilities;

/**
 * Validation interface for connector config POJOs.  The reporter carries the
 * JSON/source-position start of the validated document
 * so that errors can point at the exact field that caused the problem.
 */
public interface IValidateConfig {
    /**
     * Validate the object after Jackson deserialization.
     *
     * @param reporter Field-aware error reporter; use {@link ConfigReporter#warnPath} to emit
     *                 warnings with accurate source locations.
     * @return {@code true} if the config is valid, {@code false} if at least one warning
     *         was emitted.
     */
    boolean validate(ConfigReporter reporter);

    default boolean checkNonEmpty(ConfigReporter reporter, String property, String propertyName) {
        if (property.isBlank()) {
            reporter.warnPath(propertyName, "Invalid configuration",
                    "required field " + Utilities.doubleQuote(propertyName, false) + " is missing or empty");
            return false;
        }
        return true;
    }
}
