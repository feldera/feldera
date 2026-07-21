package org.dbsp.sqlCompiler.compiler;

/** Selects the runtime representation of the SQL VARIANT type.
 *
 * TODO: remove this class when FlatVariant becomes the only representation.
 *
 * When enabled, generated Rust uses the flat-buffer {@code FlatVariant} type
 * for VARIANT values: the type name, the {@code FV} short name used in cast
 * function names, variant literals, and the variant runtime function names
 * all switch together, and udf.rs implementations must be written against
 * {@code FlatVariant}. Connector metadata keeps the enum {@code Variant}.
 *
 * The mode is process-global because type short names are produced by
 * singleton type objects with no access to the compiler instance. Each
 * {@link DBSPCompiler} resets it from the {@code FELDERA_FLAT_VARIANT}
 * environment variable; a {@code SET feldera_flat_variant = 'on'} statement in
 * the program overrides the environment. Compiling programs with different
 * settings concurrently in one process produces undefined results:
 * {@code reset()} and {@code set()} calls from concurrent compilations
 * interleave silently, with no detection or error. If concurrent
 * compilation ever becomes a requirement, replace this global with a
 * compilation-scoped mechanism (e.g., a {@code ThreadLocal}). */
public class VariantMode {
    private static volatile boolean enabled = envDefault();

    private VariantMode() {}

    private static boolean envDefault() {
        String env = System.getenv(ProgramMetadata.USE_FLAT_VARIANT);
        return env != null && (env.equals("1")
                || env.equalsIgnoreCase("on") || env.equalsIgnoreCase("true"));
    }

    /** Reset to the environment default; called per compiler instance. */
    public static void reset() {
        enabled = envDefault();
    }

    public static void set(boolean value) {
        enabled = value;
    }

    public static boolean isEnabled() {
        return enabled;
    }
}
