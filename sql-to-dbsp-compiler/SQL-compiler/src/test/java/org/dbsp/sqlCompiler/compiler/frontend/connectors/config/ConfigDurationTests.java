package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

/** Tests for the connector configuration duration parser. */
public class ConfigDurationTests {
    private static final long MICRO = 1_000L;
    private static final long MILLI = 1_000_000L;
    private static final long SECOND = 1_000_000_000L;

    /** Asserts that {@code text} parses to {@code nanos}. */
    private static void assertNanos(long nanos, String text) {
        assertNanos(BigInteger.valueOf(nanos), text);
    }

    /** Asserts that {@code text} parses to {@code nanos}. */
    private static void assertNanos(BigInteger nanos, String text) {
        Assert.assertEquals(text, nanos, ConfigDuration.parseNanos(text));
    }

    /** Asserts that {@code text} is rejected with a message containing {@code expected}. */
    private static void assertRejected(String text, String expected) {
        try {
            ConfigDuration.parseNanos(text);
            Assert.fail("expected " + text + " to be rejected");
        } catch (ConfigDuration.ParseException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expected));
        }
    }

    @Test
    public void everyUnit() {
        assertNanos(5, "5ns");
        assertNanos(5 * MICRO, "5us");
        assertNanos(5 * MICRO, "5\u00b5s");
        assertNanos(5 * MICRO, "5\u03bcs");
        assertNanos(5 * MILLI, "5ms");
        assertNanos(5 * SECOND, "5s");
        assertNanos(5 * 60 * SECOND, "5m");
        assertNanos(5 * 60 * 60 * SECOND, "5h");
        assertNanos(5 * 24 * 60 * 60 * SECOND, "5d");
    }

    @Test
    public void bareZero() {
        assertNanos(0, "0");
        assertNanos(0, "0s");
    }

    @Test
    public void chainedTerms() {
        assertNanos(5_400 * SECOND, "1h30m");
        assertNanos(3_661 * SECOND + 500 * MILLI, "1h1m1s500ms");
    }

    @Test
    public void fractionalAndSignedValues() {
        assertNanos(500 * MILLI, "0.5s");
        assertNanos(90 * SECOND, "1.5m");
        assertNanos(30 * SECOND, "+30s");
    }

    @Test
    public void largeButRepresentableAccepted() {
        assertNanos(100_000L * 86_400L * SECOND, "100000d");
    }

    @Test
    public void emptyRejected() {
        assertRejected("", "duration is empty");
    }

    @Test
    public void negativeRejected() {
        assertRejected("-5s", "cannot be negative");
        assertRejected("-0", "cannot be negative");
    }

    @Test
    public void missingUnitRejected() {
        assertRejected("30", "'30' has no unit");
        // A bare zero is accepted only as the whole value.
        assertRejected("1s0", "'0' in duration '1s0' has no unit");
    }

    @Test
    public void unknownUnitRejected() {
        assertRejected("30sec", "unknown unit 'sec' in duration '30sec'");
        assertRejected("30x", "unknown unit 'x'");
        // The message lists the units the user may write instead.
        assertRejected("30x", "expected one of");
    }

    @Test
    public void malformedNumberRejected() {
        assertRejected("s", "'s' has no number in front of it");
        assertRejected("forever", "unknown unit 'forever';");
        assertRejected("1.2.3s", "'1.2.3s' is not a duration: '1.2.3' is not a number");
    }

    /** A duration of more than 2^53 nanoseconds keeps every digit. */
    @Test
    public void largeValuesKeepEveryDigit() {
        assertNanos(100_000L * 24 * 60 * 60 * SECOND + 12 * 60 * 60 * SECOND + 345 * MILLI,
                "100000d12h345ms");
    }

    /**
     * A fraction scales as an exact ratio and rounds to the nearest nanosecond, halves
     * away from zero.
     */
    @Test
    public void fractionsScaleExactly() {
        assertNanos(1_500 * MILLI, "1.5s");
        assertNanos(131_072_504 * MICRO, "131072.504ms");
        assertNanos(1, "0.0000000005s");
        assertNanos(62_091_912_342_805L, "0.718656392856533598d");
    }

    /** A duration longer than {@code u64::MAX} seconds is rejected. */
    @Test
    public void pastTheRuntimeCeilingRejected() {
        assertRejected("18446744073709551616s", "is too large");
        assertRejected("99999999999999999999999999s", "is too large");
        assertNanos(new BigInteger("18446744073709551615").multiply(BigInteger.valueOf(SECOND)),
                "18446744073709551615s");
    }

    /** A duration longer than a {@code long} of nanoseconds holds keeps its exact length. */
    @Test
    public void longerThanALongIsExact() {
        assertNanos(BigInteger.valueOf(1_000_000L * 24 * 60 * 60).multiply(BigInteger.valueOf(SECOND)),
                "1000000d");
        assertNanos(new BigInteger("1000000000000000000000"), "1000000000000000000000ns");
    }
}
