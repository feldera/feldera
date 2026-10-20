package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.function.Function;

/**
 * What the size and duration formats of connector configuration share: the number
 * part of a term, the bare number of the deprecated spelling, and a setting that has
 * two spellings.  {@link ConfigSize} and {@link ConfigDuration} add their units.
 */
public final class ConfigQuantity {
    private ConfigQuantity() {}

    /** Thrown when a size or duration string does not match its format. */
    public static class ParseException extends IllegalArgumentException {
        public ParseException(String message) {
            super(message);
        }
    }

    /** The largest count the runtime holds: {@code u64::MAX}. */
    static final BigInteger U64_MAX = new BigInteger("18446744073709551615");

    /** How many digits after the point are read; see the runtime parsers. */
    private static final int MAX_FRACTION_DIGITS = 18;

    /**
     * The units of one format, each with its value in the smallest unit: bytes for a
     * size, nanoseconds for a duration.
     *
     * @param values   Every accepted spelling of a unit, with its value.
     * @param kind     What a value of this format is, such as {@code "size"}.
     * @param expected The accepted units, for error messages.
     */
    record MeasurementUnits(Map<String, BigInteger> values, String kind, String expected) {
        /** Whether {@code unit} is one this format accepts. */
        boolean isKnown(String unit) {
            return this.values.containsKey(unit);
        }

        /**
         * The value of {@code unit} in the smallest unit.
         *
         * @param text The whole value, quoted in the error message.
         * @throws ParseException if this format has no such unit.
         */
        BigInteger value(String unit, String text) {
            BigInteger value = this.values.get(unit);
            if (value == null)
                throw this.unknown(unit, text);
            return value;
        }

        /** The error for a unit this format does not have. */
        ParseException unknown(String unit, String text) {
            return new ParseException("unknown unit " + ConfigReporter.partOf(unit, text, this.kind)
                    + "; " + this.expected);
        }
    }

    /** True for the characters that make up the number part of a term. */
    static boolean isNumberChar(char c) {
        return (c >= '0' && c <= '9') || c == '.';
    }

    /**
     * Splits the number part of a term into its digits before and after the point.
     *
     * @param number The number part of one term, such as {@code "1.5"}.
     * @param text   The whole value, quoted in the error message.
     * @param kind   What the value is, such as {@code "size"}, for the error message.
     * @return The whole part and the fractional part, each possibly empty.
     * @throws ParseException if {@code number} is not a number.
     */
    static String[] splitNumber(String number, String text, String kind) {
        int point = number.indexOf('.');
        String whole = point < 0 ? number : number.substring(0, point);
        String fraction = point < 0 ? "" : number.substring(point + 1);
        // A point with digits on only one side, as in "5.s" or ".5Gi", is a number,
        // as it is in the runtime.
        boolean ok = !(whole.isEmpty() && fraction.isEmpty())
                && number.indexOf('.', point + 1) < 0
                && whole.chars().allMatch(Character::isDigit)
                && fraction.chars().allMatch(Character::isDigit);
        if (!ok)
            throw new ParseException(Utilities.singleQuote(text) + " is not a " + kind + ": "
                    + Utilities.singleQuote(number) + " is not a number");
        return new String[] { whole, fraction };
    }

    /**
     * Computes one term, such as {@code "1.5"} with the {@code "Gi"} unit.
     *
     * <p>A fraction is an exact ratio, so {@code "1.5Gi"} is {@code unit * 15 / 10},
     * rounded to the nearest whole count with halves away from zero, as the runtime does.
     *
     * @param parts The whole part and the fractional part from {@link #splitNumber}.
     * @param unit  Size of the term's unit, in the smallest unit (bytes, nanoseconds).
     * @return The term in the smallest unit.
     */
    static BigInteger term(String[] parts, BigInteger unit) {
        String whole = parts[0];
        String fraction = parts[1];
        BigInteger term = whole.isEmpty() ? BigInteger.ZERO : unit.multiply(new BigInteger(whole));
        if (fraction.isEmpty())
            return term;
        int kept = Math.min(fraction.length(), MAX_FRACTION_DIGITS);
        BigInteger numerator = new BigInteger(fraction.substring(0, kept));
        BigInteger denominator = BigInteger.TEN.pow(kept);
        return term.add(unit.multiply(numerator).add(denominator.shiftRight(1)).divide(denominator));
    }

    /**
     * The field a user set, paired with its value.  A setting has two spellings, the
     * string and the deprecated number, and errors about the value must name the one
     * the user actually wrote.
     *
     * @param jsonPath Relative JSON Pointer suffix of the field the value came from.
     * @param display  The value as written.
     * @param value    The value in the smallest unit: bytes for a size, nanoseconds
     *                 for a duration.
     */
    public record Setting(String jsonPath, String display, BigInteger value) {}

    /**
     * The format of one kind of setting.
     *
     * @param parser     Parses a string, throwing {@link ParseException} if it is malformed.
     * @param legacyUnit What one unit of a bare number is worth, in the smallest unit.
     * @param expected   Error message for a value that is neither a string nor a whole
     *                   number, naming an example and the units.
     */
    record Format(Function<String, BigInteger> parser, BigInteger legacyUnit, String expected) {}

    /**
     * Checks both spellings of a setting, reporting a malformed value, or a
     * configuration that writes both spellings.
     *
     * @return {@code true} if there is nothing to report.
     */
    static boolean check(ConfigReporter reporter, Format format,
                         String path, @Nullable JsonNode value,
                         String legacyPath, @Nullable JsonNode legacyValue) {
        if (!Utilities.isNotNull(value) && !Utilities.isNotNull(legacyValue)
                && !Utilities.bothNonNull(value, legacyValue))
            return true;
        return resolve(reporter, format, path, value, legacyPath, legacyValue, false) != null;
    }

    /**
     * Checks both spellings of a setting and returns the one in effect.
     *
     * <p>Each spelling reaches the same setting, so writing both is an error, as it
     * is in the runtime.
     *
     * @param reporter    Reporter used to emit an error.
     * @param format      Format of the setting.
     * @param path        Relative JSON Pointer suffix of the current spelling.
     * @param value       Value of the current spelling, or {@code null} if absent.
     * @param legacyPath  Relative JSON Pointer suffix of the deprecated spelling.
     * @param legacyValue Value of the deprecated spelling, or {@code null} if absent.
     * @param required    Whether the setting has a default but no "unset" value, so the
     *                    runtime refuses a JSON {@code null} under either spelling.
     * @return The setting in effect, or {@code null} if neither field is set or the
     *         value is malformed, in which case the error was already reported.
     */
    @Nullable
    static Setting resolve(ConfigReporter reporter, Format format,
                           String path, @Nullable JsonNode value,
                           String legacyPath, @Nullable JsonNode legacyValue,
                           boolean required) {
        if (reportBothSpellings(reporter, path, value, legacyPath, legacyValue))
            return null;
        if (required) {
            String nullPath = isJsonNull(value) ? path : isJsonNull(legacyValue) ? legacyPath : null;
            if (nullPath != null) {
                reporter.warnPath(nullPath, "Invalid configuration",
                        "\"" + ConfigReporter.fieldName(nullPath)
                                + "\" cannot be null; remove it to use the default");
                return null;
            }
        }
        if (Utilities.isNotNull(value))
            return parse(reporter, format, path, value);
        if (Utilities.isNotNull(legacyValue))
            return parse(reporter, format, legacyPath, legacyValue);
        return null;
    }

    /** True if {@code node} is present and is the JSON {@code null}. */
    private static boolean isJsonNull(@Nullable JsonNode node) {
        return node != null && node.isNull();
    }

    /**
     * Reports a configuration that writes both spellings of a setting.  The runtime
     * binds both keys to one field and rejects the pair as a duplicate, even with a JSON
     * {@code null} value.  Say so here, where the position is known.
     *
     * @return {@code true} if both spellings are written and the error was reported.
     */
    static boolean reportBothSpellings(ConfigReporter reporter,
                                       String path, @Nullable JsonNode value,
                                       String legacyPath, @Nullable JsonNode legacyValue) {
        if (!Utilities.bothNonNull(value, legacyValue))
            return false;
        reporter.warnPath(legacyPath, "Invalid configuration",
                "\"" + ConfigReporter.fieldName(legacyPath) + "\" is another spelling of \""
                        + ConfigReporter.fieldName(path) + "\"; write one of the two");
        return true;
    }

    /**
     * Parses one spelling of a setting, reporting an error if it is malformed.
     *
     * @param reporter Reporter used to emit the error.
     * @param format   Format of the setting.
     * @param jsonPath Relative JSON Pointer suffix of the field.
     * @param value    Value the user wrote.
     * @return The setting, or {@code null} if it is malformed.
     */
    @Nullable
    private static Setting parse(ConfigReporter reporter, Format format,
                                 String jsonPath, JsonNode value) {
        if (value.isTextual()) {
            String text = value.asText();
            try {
                return new Setting(jsonPath, text, format.parser().apply(text));
            } catch (ParseException e) {
                reportMalformed(reporter, jsonPath, e);
                return null;
            }
        }
        BigInteger legacy = wholeNumber(value);
        if (legacy != null)
            return new Setting(jsonPath, legacy.toString(), legacy.multiply(format.legacyUnit()));
        reporter.warnPath(jsonPath, "Invalid configuration",
                "\"" + ConfigReporter.fieldName(jsonPath) + "\": " + value
                        + " is neither a string nor a whole number; " + format.expected());
        return null;
    }

    /** Reports a malformed string at the field it was written in. */
    static void reportMalformed(ConfigReporter reporter, String jsonPath, ParseException e) {
        reporter.warnPath(jsonPath, "Invalid configuration",
                "\"" + ConfigReporter.fieldName(jsonPath) + "\": " + e.getMessage());
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
    static BigInteger wholeNumber(JsonNode value) {
        if (!value.isNumber())
            return null;
        // Jackson reads a literal such as 1e400 as an infinite double, which has no
        // decimal value; it is no count of units either.
        if (value.isFloatingPointNumber() && !Double.isFinite(value.doubleValue()))
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
     * Reports a configuration error if {@code setting} is below {@code minimum}.
     *
     * @param reporter    Reporter used to emit the error.
     * @param setting     The setting in effect, or {@code null} to check nothing.
     * @param minimum     Smallest accepted value, in the smallest unit (bytes, nanoseconds).
     * @param requirement What the value must be, phrased for the error message and ending
     *                    with an example, e.g. {@code "greater than 0, for example \"1Mi\""}.
     * @return {@code true} if there is nothing to report.
     */
    public static boolean checkMinimum(ConfigReporter reporter, @Nullable Setting setting,
                                       long minimum, String requirement) {
        if (setting == null || setting.value().compareTo(BigInteger.valueOf(minimum)) >= 0)
            return true;
        reporter.warnPath(setting.jsonPath(), "Invalid configuration",
                "\"" + ConfigReporter.fieldName(setting.jsonPath()) + "\" is "
                        + Utilities.singleQuote(setting.display()) + "; it must be " + requirement);
        return false;
    }
}
