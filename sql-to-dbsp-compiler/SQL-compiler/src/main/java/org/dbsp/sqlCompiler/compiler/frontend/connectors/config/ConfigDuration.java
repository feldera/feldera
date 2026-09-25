package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;

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

    /** Thrown when a duration string does not match the configuration duration format. */
    public static class ParseException extends IllegalArgumentException {
        public ParseException(String message) {
            super(message);
        }
    }

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
            throw new ParseException("duration " + quote(text) + " cannot be negative");
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
            while (unitStart < rest.length() && isNumberChar(rest.charAt(unitStart)))
                unitStart++;
            String number = rest.substring(position, unitStart);

            int unitEnd = unitStart;
            while (unitEnd < rest.length() && !isNumberChar(rest.charAt(unitEnd)))
                unitEnd++;
            String unit = rest.substring(unitStart, unitEnd);
            position = unitEnd;

            if (unit.isEmpty())
                throw new ParseException(
                        inDuration(number, text) + " has no unit; " + EXPECTED_UNITS);
            // A term with no digits at all is a unit problem, not a number problem,
            // which is the order the runtime parser reports them in. Which half is
            // wrong depends on whether the rest is a unit: "s" is one and wants a
            // number in front of it, "forever" is not one at all.
            if (number.isEmpty()) {
                if (isKnownUnit(unit))
                    throw new ParseException(
                            inDuration(unit, text) + " has no number in front of it");
                throw new ParseException(
                        "unknown unit " + inDuration(unit, text) + "; " + EXPECTED_UNITS);
            }
            String[] parts = splitNumber(number, text);
            total = total.add(termNanos(parts[0], parts[1], unitNanos(unit, text)));
        }

        // The runtime refuses a duration it cannot hold, so refuse it here too.
        if (total.compareTo(RUNTIME_CEILING) > 0)
            throw new ParseException("duration " + quote(text) + " is too large");
        return total;
    }

    /** The largest whole number of seconds the runtime holds: {@code u64::MAX}. */
    private static final BigInteger U64_MAX = new BigInteger("18446744073709551615");

    /** The largest duration the runtime holds, in nanoseconds: just under {@code U64_MAX + 1} seconds. */
    private static final BigInteger RUNTIME_CEILING =
            U64_MAX.multiply(BigInteger.valueOf(NANOS_PER_SECOND))
                    .add(BigInteger.valueOf(NANOS_PER_SECOND - 1));

    /** How many digits after the point are read; see the runtime parser. */
    private static final int MAX_FRACTION_DIGITS = 18;

    /**
     * Splits a number into its digits before and after the point.
     *
     * @param number The number part of one term, such as {@code "1.5"}.
     * @param text   The whole duration, quoted in the error message.
     * @return The whole part and the fractional part, each possibly empty.
     * @throws ParseException if {@code number} is not a number.
     */
    private static String[] splitNumber(String number, String text) {
        int point = number.indexOf('.');
        String whole = point < 0 ? number : number.substring(0, point);
        String fraction = point < 0 ? "" : number.substring(point + 1);
        boolean ok = !(whole.isEmpty() && fraction.isEmpty())
                && number.indexOf('.', point + 1) < 0
                && whole.chars().allMatch(Character::isDigit)
                && fraction.chars().allMatch(Character::isDigit)
                && (point < 0 || !fraction.isEmpty());
        if (!ok)
            throw new ParseException(quote(text) + " is not a duration: "
                    + quote(number) + " is not a number");
        return new String[] { whole, fraction };
    }

    /**
     * Computes the length of one term, such as {@code "1.5"} with the {@code "s"} unit.
     *
     * <p>A fraction is an exact ratio, so {@code "0.5s"} is {@code unitNanos * 5 / 10},
     * rounded to the nearest nanosecond with halves away from zero, as the runtime does.
     *
     * @param whole     Digits before the point, possibly empty.
     * @param fraction  Digits after the point, possibly empty.
     * @param unitNanos Length of the term's unit, in nanoseconds.
     * @return The length of the term in nanoseconds.
     */
    private static BigInteger termNanos(String whole, String fraction, long unitNanos) {
        BigInteger unit = BigInteger.valueOf(unitNanos);
        BigInteger term = whole.isEmpty() ? BigInteger.ZERO : unit.multiply(new BigInteger(whole));
        if (fraction.isEmpty())
            return term;
        int kept = Math.min(fraction.length(), MAX_FRACTION_DIGITS);
        BigInteger numerator = new BigInteger(fraction.substring(0, kept));
        BigInteger denominator = BigInteger.TEN.pow(kept);
        return term.add(unit.multiply(numerator).add(denominator.shiftRight(1)).divide(denominator));
    }

    /** True for the characters that make up the number part of a duration term. */
    private static boolean isNumberChar(char c) {
        return (c >= '0' && c <= '9') || c == '.';
    }

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
        boolean hasCurrent = isNotNull(value);
        boolean hasLegacy = isNotNull(legacyValue);
        if (bothWritten(value, legacyValue)) {
            reporter.warnPath(legacyPath, "Invalid configuration",
                    "\"" + legacyPath + "\" is another spelling of \"" + path
                    + "\"; write one of the two");
            return false;
        }
        if (!hasCurrent && !hasLegacy)
            return true;
        JsonNode node = hasCurrent ? value : legacyValue;
        String where = hasCurrent ? path : legacyPath;
        assert node != null;
        if (node.isTextual())
            return check(reporter, where, node.asText());
        if (node.isObject()) {
            JsonNode secs = node.get("secs");
            JsonNode nanos = node.get("nanos");
            boolean wellFormed = node.size() <= 2
                    && (secs != null || nanos != null)
                    && (secs == null || (secs.isIntegralNumber() && secs.asLong() >= 0))
                    && (nanos == null || (nanos.isIntegralNumber() && nanos.asLong() >= 0));
            if (wellFormed)
                return true;
        }
        reporter.warnPath(where, "Invalid configuration",
                "expected a duration such as \"30s\", or a {secs, nanos} object");
        return false;
    }

    /** Whether {@code unit} is one this format accepts. */
    private static boolean isKnownUnit(String unit) {
        return switch (unit) {
            case "ns", "us", "\u00b5s", "\u03bcs", "ms", "s", "m", "h", "d" -> true;
            default -> false;
        };
    }

    private static long unitNanos(String unit, String text) {
        return switch (unit) {
            case "ns" -> 1L;
            // U+00B5 is the "micro sign", U+03BC is "Greek small letter mu".
            case "us", "\u00b5s", "\u03bcs" -> NANOS_PER_MICRO;
            case "ms" -> NANOS_PER_MILLI;
            case "s" -> NANOS_PER_SECOND;
            case "m" -> 60 * NANOS_PER_SECOND;
            case "h" -> 60 * 60 * NANOS_PER_SECOND;
            case "d" -> 24 * 60 * 60 * NANOS_PER_SECOND;
            default -> throw new ParseException(
                    "unknown unit " + inDuration(unit, text) + "; " + EXPECTED_UNITS);
        };
    }

    /**
     * Quotes a value for an error message.
     *
     * <p>Not {@code Utilities.doubleQuote}: that escapes every character outside ASCII,
     * so a value written with the micro sign would print as an escape sequence.
     */
    private static String quote(String value) {
        return "'" + value + "'";
    }

    /**
     * Names the faulty part of a duration for an error message: just the part when it is
     * the whole duration, otherwise the part and the duration it came from.
     */
    private static String inDuration(String part, String text) {
        return part.equals(text) ? quote(part) : quote(part) + " in duration " + quote(text);
    }

    /** Turns a JSON Pointer suffix such as {@code "a/b"} into the field name {@code "a.b"}. */
    private static String fieldName(String path) {
        return path.replace('/', '.');
    }

    /**
     * Checks a duration-valued field, reporting a configuration error if it is malformed.
     *
     * @param reporter Reporter used to emit the error.
     * @param path     Relative JSON Pointer suffix of the field, as
     *                 {@link ConfigReporter#warnPath} expects it.
     * @param value    Value the user wrote, or {@code null} if the field is absent.
     * @return {@code true} if the field is absent or holds a valid duration.
     */
    public static boolean check(ConfigReporter reporter, String path, @Nullable JsonNode value) {
        return !isNotNull(value) || read(reporter, path, value, NANOS_PER_SECOND) != null;
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
        if (!isNotNull(value) && !isNotNull(legacyValue) && !bothWritten(value, legacyValue))
            return true;
        return resolve(reporter, path, value, legacyPath, legacyValue, NANOS_PER_SECOND) != null;
    }

    private static boolean check(ConfigReporter reporter, String path, @Nullable String text) {
        if (text == null)
            return true;
        try {
            parseNanos(text);
            return true;
        } catch (ParseException e) {
            reporter.warnPath(path, "Invalid configuration",
                    "\"" + fieldName(path) + "\": " + e.getMessage());
            return false;
        }
    }

    /**
     * The duration field a user set, paired with its value.  A setting has two spellings,
     * the duration string and the deprecated integer, and errors about the value must name
     * the one the user actually wrote.
     *
     * @param path    Relative JSON Pointer suffix of the field the value came from.
     * @param display The value as written.
     * @param nanos   The value in nanoseconds.
     */
    public record Setting(String path, String display, BigInteger nanos) {}

    /**
     * Checks both spellings of a duration setting and returns the one in effect.
     *
     * <p>Each spelling reaches the same setting, so writing both is an error, as it
     * is in the runtime.
     *
     * @param reporter        Reporter used to emit an error for a malformed duration string.
     * @param path            Relative JSON Pointer suffix of the duration-string field.
     * @param value           Duration string the user wrote, or {@code null} if absent.
     * @param legacyPath      Relative JSON Pointer suffix of the deprecated integer field.
     * @param legacyValue     Integer the user wrote, or {@code null} if absent.
     * @param legacyUnitNanos Length of the deprecated field's unit, in nanoseconds.
     * @return The setting in effect, or {@code null} if neither field is set or the duration
     *         string is malformed, in which case the error was already reported.
     */
    @Nullable
    public static Setting resolve(ConfigReporter reporter,
                                  String path, @Nullable JsonNode value,
                                  String legacyPath, @Nullable JsonNode legacyValue,
                                  long legacyUnitNanos) {
        boolean hasCurrent = isNotNull(value);
        boolean hasLegacy = isNotNull(legacyValue);
        if (bothWritten(value, legacyValue)) {
            // The runtime binds both keys to one field, so a configuration carrying both
            // is rejected there as a duplicate. Say so here, where the position is known.
            reporter.warnPath(legacyPath, "Invalid configuration",
                    "\"" + legacyPath + "\" is another spelling of \"" + path
                    + "\"; write one of the two");
            return null;
        }
        if (hasCurrent)
            return read(reporter, path, value, legacyUnitNanos);
        if (hasLegacy)
            return read(reporter, legacyPath, legacyValue, legacyUnitNanos);
        return null;
    }

    /**
     * True if both spellings of a setting appear in the configuration, even with a
     * {@code null} value: the runtime binds both keys to one field and rejects the pair
     * as a duplicate either way.  Jackson reads an explicit {@code null} as a
     * {@code NullNode} and leaves an absent field {@code null}, so the two are told apart.
     */
    private static boolean bothWritten(@Nullable JsonNode value, @Nullable JsonNode legacyValue) {
        return value != null && legacyValue != null;
    }

    /** True if {@code node} is present and is not the JSON {@code null}. */
    private static boolean isNotNull(@Nullable JsonNode node) {
        return node != null && !node.isNull();
    }

    /**
     * Reads one spelling of a duration setting, reporting an error if it is malformed.
     *
     * @param reporter  Reporter used to emit the error.
     * @param path      Relative JSON Pointer suffix of the field.
     * @param value     Value the user wrote, or {@code null} if absent.
     * @param unitNanos Length of one unit of a bare number, in nanoseconds.
     * @return The setting, or {@code null} if it is absent or malformed.
     */
    @Nullable
    private static Setting read(ConfigReporter reporter, String path, @Nullable JsonNode value,
                                long unitNanos) {
        if (value == null)
            return null;
        if (value.isTextual()) {
            String text = value.asText();
            if (!check(reporter, path, text))
                return null;
            return new Setting(path, text, parseNanos(text));
        }
        BigInteger legacy = wholeNumber(value);
        if (legacy != null)
            return new Setting(path, legacy.toString(),
                    legacy.multiply(BigInteger.valueOf(unitNanos)));
        reporter.warnPath(path, "Invalid configuration",
                "expected a duration such as \"30s\"; " + EXPECTED_UNITS);
        return null;
    }

    /**
     * Returns the value of a bare number the runtime reads as a count of units: a whole,
     * non-negative number no larger than {@code u64::MAX}.  JSON has one number type, so
     * {@code 10.0} and {@code 1e1} count as whole numbers too.
     *
     * @param value A JSON value.
     * @return The number, or {@code null} if {@code value} is not such a number.
     */
    @Nullable
    private static BigInteger wholeNumber(JsonNode value) {
        if (!value.isNumber())
            return null;
        BigDecimal number = value.decimalValue();
        if (number.signum() < 0)
            return null;
        BigInteger whole;
        try {
            whole = number.toBigIntegerExact();
        } catch (ArithmeticException notWhole) {
            return null;
        }
        return whole.compareTo(U64_MAX) > 0 ? null : whole;
    }

    /**
     * Reports a configuration error if {@code setting} is below {@code minNanos}.
     *
     * @param reporter    Reporter used to emit the error.
     * @param setting     The setting in effect, or {@code null} to check nothing.
     * @param minNanos    Smallest accepted value, in nanoseconds.
     * @param requirement What the value must be, phrased for the error message and ending
     *                    with an example, e.g. {@code "at least 1 second, for example \"1s\""}.
     * @return {@code true} if there is nothing to report.
     */
    public static boolean checkMinimum(ConfigReporter reporter, @Nullable Setting setting,
                                       long minNanos, String requirement) {
        if (setting == null || setting.nanos().compareTo(BigInteger.valueOf(minNanos)) >= 0)
            return true;
        reporter.warnPath(setting.path(), "Invalid configuration",
                "\"" + fieldName(setting.path()) + "\" is " + quote(setting.display())
                        + "; it must be " + requirement);
        return false;
    }
}
