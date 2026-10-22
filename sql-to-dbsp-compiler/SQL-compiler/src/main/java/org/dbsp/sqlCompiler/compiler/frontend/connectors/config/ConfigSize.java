package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.config.ConfigQuantity.ParseException;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * Parser and validator for the human-readable size format used by connector
 * configuration, such as {@code "500M"}, {@code "512Mi"} or {@code "4096"}.
 *
 * <p>The syntax is the one Kubernetes uses for a resource quantity: a number and a
 * unit.  Decimal units are {@code k}, {@code M}, {@code G}, {@code T}, {@code P} and
 * {@code E}, each a thousand times the one before; binary units are {@code Ki},
 * {@code Mi}, {@code Gi}, {@code Ti}, {@code Pi} and {@code Ei}, each 1024 times the one
 * before.  A unit may carry a trailing {@code B}, as in {@code "512MiB"}, and a bare
 * number is a number of bytes.  One term only; negative values are rejected.
 *
 * <p>This mirrors the runtime parser in {@code crates/feldera-types/src/size.rs}, so
 * the compiler accepts exactly the configuration values the pipeline accepts.
 */
public final class ConfigSize {
    private ConfigSize() {}

    private static final String EXPECTED_UNITS =
            "expected one of \"k\", \"M\", \"G\", \"T\", \"P\", \"E\", \"Ki\", \"Mi\", \"Gi\", "
                    + "\"Ti\", \"Pi\", or \"Ei\", each with or without a trailing \"B\"";

    /** Each unit, with and without a trailing {@code B}, and its size in bytes. */
    private static final ConfigQuantity.MeasurementUnits UNITS = new ConfigQuantity.MeasurementUnits(
            withTrailingB(Map.ofEntries(
                    Map.entry("", BigInteger.ONE),
                    // Kubernetes writes a kilobyte as "k" only, but the capital is what
                    // the other decimal units use, so it is read too.
                    Map.entry("k", BigInteger.TEN.pow(3)),
                    Map.entry("K", BigInteger.TEN.pow(3)),
                    Map.entry("M", BigInteger.TEN.pow(6)),
                    Map.entry("G", BigInteger.TEN.pow(9)),
                    Map.entry("T", BigInteger.TEN.pow(12)),
                    Map.entry("P", BigInteger.TEN.pow(15)),
                    Map.entry("E", BigInteger.TEN.pow(18)),
                    Map.entry("Ki", BigInteger.ONE.shiftLeft(10)),
                    Map.entry("Mi", BigInteger.ONE.shiftLeft(20)),
                    Map.entry("Gi", BigInteger.ONE.shiftLeft(30)),
                    Map.entry("Ti", BigInteger.ONE.shiftLeft(40)),
                    Map.entry("Pi", BigInteger.ONE.shiftLeft(50)),
                    Map.entry("Ei", BigInteger.ONE.shiftLeft(60)))),
            "size", EXPECTED_UNITS);

    /** Adds the spelling with a trailing {@code B} of each unit, as in {@code "512MiB"}. */
    private static Map<String, BigInteger> withTrailingB(Map<String, BigInteger> units) {
        Map<String, BigInteger> all = new HashMap<>(units);
        units.forEach((unit, bytes) -> all.put(unit + "B", bytes));
        return Map.copyOf(all);
    }

    /**
     * Parses a configuration for a size (memory, storage) and returns its value in bytes.
     *
     * @param text The size as written, such as {@code "512Mi"}.
     * @return The size in bytes.
     * @throws ParseException if {@code text} is not a valid size; the message explains
     *                        what is wrong with the value and what units exist.
     */
    public static BigInteger parseBytes(String text) {
        if (text.startsWith("-"))
            throw new ParseException("size " + Utilities.singleQuote(text) + " cannot be negative");
        // The runtime parser accepts one leading sign character, not a run of them.
        String rest = text.startsWith("+") ? text.substring(1) : text;
        if (rest.isEmpty())
            throw new ParseException(
                    "size is empty; write a number of bytes, or a number and a unit such as \"512Mi\"");

        int unitStart = 0;
        while (unitStart < rest.length() && ConfigQuantity.isNumberChar(rest.charAt(unitStart)))
            unitStart++;
        String number = rest.substring(0, unitStart);
        String unit = rest.substring(unitStart);

        // A term with no digits at all is a unit problem, not a number problem,
        // which is the order the runtime parser reports them in. Which half is
        // wrong depends on whether the rest is a unit: "Mi" is one and wants a
        // number in front of it, "lots" is not one at all.
        if (number.isEmpty()) {
            if (UNITS.isKnown(unit))
                throw new ParseException(
                        ConfigReporter.partOf(unit, text, "size") + " has no number in front of it");
            throw UNITS.unknown(unit, text);
        }
        String[] parts = ConfigQuantity.splitNumber(number, text, "size");
        BigInteger total = ConfigQuantity.term(parts, UNITS.value(unit, text));

        // The runtime refuses a size it cannot hold, so refuse it here too.
        if (total.compareTo(ConfigQuantity.U64_MAX) > 0)
            throw new ParseException("size " + Utilities.singleQuote(text) + " is too large");
        return total;
    }

    private static final ConfigQuantity.Format FORMAT = new ConfigQuantity.Format(
            ConfigSize::parseBytes, BigInteger.ONE,
            "expected a size such as \"512Mi\"; " + EXPECTED_UNITS);

    /**
     * Checks both spellings of a size setting, reporting a malformed value, or a
     * configuration that writes both spellings.  A bare number is read in bytes.
     *
     * @param reporter    Reporter used to emit the error.
     * @param path        Relative JSON Pointer suffix of the current spelling.
     * @param value       Value of the current spelling, or {@code null} if absent.
     * @param legacyPath  Relative JSON Pointer suffix of the deprecated spelling.
     * @param legacyValue Value of the deprecated spelling, or {@code null} if absent.
     * @return {@code true} if there is nothing to report.
     */
    public static boolean check(ConfigReporter reporter,
                                String path, @Nullable JsonNode value,
                                String legacyPath, @Nullable JsonNode legacyValue) {
        return ConfigQuantity.check(reporter, FORMAT, path, value, legacyPath, legacyValue);
    }

    /**
     * Checks both spellings of a size setting and returns the one in effect, its value
     * in bytes.  The deprecated spelling counted bytes, for every connector setting this
     * covers.  The setting has a default but cannot be unset: the runtime refuses a JSON
     * {@code null} under either spelling, so this reports it.
     *
     * @param reporter    Reporter used to emit an error.
     * @param path        Relative JSON Pointer suffix of the size-string field.
     * @param value       Size string the user wrote, or {@code null} if absent.
     * @param legacyPath  Relative JSON Pointer suffix of the deprecated byte-count field.
     * @param legacyValue Number of bytes the user wrote, or {@code null} if absent.
     * @return The setting in effect, or {@code null} if neither field is set or the
     *         value is malformed, in which case the error was already reported.
     */
    @Nullable
    public static ConfigQuantity.Setting resolveRequired(ConfigReporter reporter,
                                                         String path, @Nullable JsonNode value,
                                                         String legacyPath, @Nullable JsonNode legacyValue) {
        return ConfigQuantity.resolve(reporter, FORMAT, path, value, legacyPath, legacyValue, true);
    }
}
