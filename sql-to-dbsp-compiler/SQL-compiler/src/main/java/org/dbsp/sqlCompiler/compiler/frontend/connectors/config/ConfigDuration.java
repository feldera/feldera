package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.config.ConfigQuantity.ParseException;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Map;

/**
 * Parser and validator for the human-readable duration format used by connector
 * configuration, such as {@code "500ms"}, {@code "30s"}, {@code "1h30m"} or {@code "30d"}.
 *
 * <p>Accepted units are {@code ns}, {@code us} (also spelled {@code \u00b5s} or {@code \u03bcs}),
 * {@code ms}, {@code s}, {@code m}, {@code h}, and {@code d}.  A value may chain several
 * terms, as in {@code "1h30m"}, and the terms are summed.  A bare {@code "0"} is accepted
 * without a unit.  Negative values are rejected.
 *
 * <p>This mirrors the runtime parser in {@code crates/feldera-types/src/duration.rs}, so
 * that the compiler accepts exactly the configuration values the pipeline accepts.
 */
public final class ConfigDuration {
    private ConfigDuration() {}

    public static final long NANOS_PER_MICRO = 1_000L;
    public static final long NANOS_PER_MILLI = 1_000_000L;
    public static final long NANOS_PER_SECOND = 1_000_000_000L;

    private static final String EXPECTED_UNITS =
            "expected one of \"ns\", \"us\", \"ms\", \"s\", \"m\", \"h\", or \"d\"";

    /** Each unit and its length in nanoseconds. */
    private static final ConfigQuantity.MeasurementUnits UNITS = new ConfigQuantity.MeasurementUnits(
            Map.ofEntries(
                    Map.entry("ns", BigInteger.ONE),
                    // U+00B5 is the "micro sign", U+03BC is "Greek small letter mu".
                    Map.entry("us", BigInteger.valueOf(NANOS_PER_MICRO)),
                    Map.entry("\u00b5s", BigInteger.valueOf(NANOS_PER_MICRO)),
                    Map.entry("\u03bcs", BigInteger.valueOf(NANOS_PER_MICRO)),
                    Map.entry("ms", BigInteger.valueOf(NANOS_PER_MILLI)),
                    Map.entry("s", BigInteger.valueOf(NANOS_PER_SECOND)),
                    Map.entry("m", BigInteger.valueOf(60 * NANOS_PER_SECOND)),
                    Map.entry("h", BigInteger.valueOf(60 * 60 * NANOS_PER_SECOND)),
                    Map.entry("d", BigInteger.valueOf(24 * 60 * 60 * NANOS_PER_SECOND))),
            "duration", EXPECTED_UNITS);

    /**
     * Parses a configuration duration and returns its length in nanoseconds.
     *
     * @param text The duration as written, such as {@code "1h30m"}.
     * @return The length of the duration in nanoseconds.
     * @throws ParseException if {@code text} is not a valid duration; the message explains
     *                        what is wrong with the value and what units exist.
     */
    public static BigInteger parseNanos(String text) {
        if (text.startsWith("-"))
            throw new ParseException("duration " + Utilities.singleQuote(text) + " cannot be negative");
        // The runtime parser accepts one leading sign character, not a run of them.
        String rest = text.startsWith("+") ? text.substring(1) : text;
        if (rest.isEmpty())
            throw new ParseException(
                    "duration is empty; write \"0\", or a number and a unit such as \"500ms\"");
        // A bare "0" needs no unit, but only as the whole string, so "1s0" stays an error.
        if (rest.equals("0"))
            return BigInteger.ZERO;

        BigInteger total = BigInteger.ZERO;
        int position = 0;
        while (position < rest.length()) {
            int unitStart = position;
            while (unitStart < rest.length() && ConfigQuantity.isNumberChar(rest.charAt(unitStart)))
                unitStart++;
            String number = rest.substring(position, unitStart);

            int unitEnd = unitStart;
            while (unitEnd < rest.length() && !ConfigQuantity.isNumberChar(rest.charAt(unitEnd)))
                unitEnd++;
            String unit = rest.substring(unitStart, unitEnd);
            position = unitEnd;

            if (unit.isEmpty())
                throw new ParseException(
                        ConfigReporter.partOf(number, text, "duration") + " has no unit; " + EXPECTED_UNITS);
            // A term with no digits at all is a unit problem, not a number problem,
            // which is the order the runtime parser reports them in. Which half is
            // wrong depends on whether the rest is a unit: "s" is one and wants a
            // number in front of it, "forever" is not one at all.
            if (number.isEmpty()) {
                if (UNITS.isKnown(unit))
                    throw new ParseException(
                            ConfigReporter.partOf(unit, text, "duration") + " has no number in front of it");
                throw UNITS.unknown(unit, text);
            }
            String[] parts = ConfigQuantity.splitNumber(number, text, "duration");
            total = total.add(ConfigQuantity.term(parts, UNITS.value(unit, text)));
        }

        // The runtime refuses a duration it cannot hold, so refuse it here too.
        if (total.compareTo(RUNTIME_CEILING) > 0)
            throw new ParseException("duration " + Utilities.singleQuote(text) + " is too large");
        return total;
    }

    private static final BigInteger U32_MAX = BigInteger.valueOf(4294967295L);

    /**
     * True if {@code node} is a whole, non-negative JSON number no larger than {@code max}.
     * Read as a {@link BigInteger}, so a value beyond a {@code long} does not wrap.
     */
    private static boolean isUnsignedAtMost(JsonNode node, BigInteger max) {
        if (!node.isIntegralNumber())
            return false;
        BigInteger value = node.bigIntegerValue();
        return value.signum() >= 0 && value.compareTo(max) <= 0;
    }

    /** The largest duration the runtime holds, in nanoseconds: just under {@code U64_MAX + 1} seconds. */
    private static final BigInteger RUNTIME_CEILING =
            ConfigQuantity.U64_MAX.multiply(BigInteger.valueOf(NANOS_PER_SECOND))
                    .add(BigInteger.valueOf(NANOS_PER_SECOND - 1));

    /**
     * Checks a duration setting whose value is either a duration string or a
     * {@code {secs, nanos}} object.  A bare number is reported as an error, and so is a
     * configuration that writes both spellings of the setting.
     *
     * @param reporter    Reporter used to emit the error.
     * @param path        Relative JSON Pointer suffix of the current spelling.
     * @param value       Value of the current spelling, or {@code null} if absent.
     * @param legacyPath  Relative JSON Pointer suffix of the deprecated spelling.
     * @param legacyValue Value of the deprecated spelling, or {@code null} if absent.
     * @return {@code true} if there is nothing to report.
     */
    public static boolean checkExpiry(ConfigReporter reporter,
                                      String path, @Nullable JsonNode value,
                                      String legacyPath, @Nullable JsonNode legacyValue) {
        if (ConfigQuantity.reportBothSpellings(reporter, path, value, legacyPath, legacyValue))
            return false;
        boolean hasCurrent = Utilities.isNotNull(value);
        boolean hasLegacy = Utilities.isNotNull(legacyValue);
        if (!hasCurrent && !hasLegacy)
            return true;
        JsonNode node = hasCurrent ? value : legacyValue;
        String where = hasCurrent ? path : legacyPath;
        assert node != null;
        if (node.isTextual()) {
            try {
                parseNanos(node.asText());
                return true;
            } catch (ParseException e) {
                ConfigQuantity.reportMalformed(reporter, where, e);
                return false;
            }
        }
        if (node.isObject()) {
            JsonNode secs = node.get("secs");
            JsonNode nanos = node.get("nanos");
            // The runtime reads `secs` as a u64 and `nanos` as a u32, and rejects
            // any other key; a missing one counts as zero.
            int known = (secs == null ? 0 : 1) + (nanos == null ? 0 : 1);
            boolean wellFormed = known > 0 && node.size() == known
                    && (secs == null || isUnsignedAtMost(secs, ConfigQuantity.U64_MAX))
                    && (nanos == null || isUnsignedAtMost(nanos, U32_MAX));
            if (wellFormed)
                return true;
        }
        reporter.warnPath(where, "Invalid configuration",
                "expected a duration such as \"30s\", or a {secs, nanos} object");
        return false;
    }

    /**
     * Format of a duration setting whose deprecated spelling counts {@code legacyUnitNanos}.
     */
    private static ConfigQuantity.Format format(long legacyUnitNanos) {
        return new ConfigQuantity.Format(ConfigDuration::parseNanos, BigInteger.valueOf(legacyUnitNanos),
                "expected a duration such as \"30s\"; " + EXPECTED_UNITS);
    }

    /**
     * Checks both spellings of a duration setting, reporting a malformed value, or a
     * configuration that writes both spellings.  A bare number is read in seconds.
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
        return ConfigQuantity.check(reporter, format(NANOS_PER_SECOND),
                path, value, legacyPath, legacyValue);
    }

    /**
     * Checks both spellings of a duration setting and returns the one in effect, its
     * value in nanoseconds.
     *
     * @param reporter        Reporter used to emit an error.
     * @param path            Relative JSON Pointer suffix of the duration-string field.
     * @param value           Duration string the user wrote, or {@code null} if absent.
     * @param legacyPath      Relative JSON Pointer suffix of the deprecated integer field.
     * @param legacyValue     Integer the user wrote, or {@code null} if absent.
     * @param legacyUnitNanos Length of the deprecated field's unit, in nanoseconds.
     * @return The setting in effect, or {@code null} if neither field is set or the duration
     *         string is malformed, in which case the error was already reported.
     */
    @Nullable
    public static ConfigQuantity.Setting resolve(ConfigReporter reporter,
                                                 String path, @Nullable JsonNode value,
                                                 String legacyPath, @Nullable JsonNode legacyValue,
                                                 long legacyUnitNanos) {
        return ConfigQuantity.resolve(reporter, format(legacyUnitNanos),
                path, value, legacyPath, legacyValue, false);
    }
}
