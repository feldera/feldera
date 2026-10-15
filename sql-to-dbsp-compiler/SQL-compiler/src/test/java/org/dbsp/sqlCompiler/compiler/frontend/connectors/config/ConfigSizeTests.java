package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

/** Tests for the connector configuration size parser. */
public class ConfigSizeTests {
    private static final long KIB = 1L << 10;
    private static final long MIB = 1L << 20;
    private static final long GIB = 1L << 30;

    /** Asserts that {@code text} parses to {@code bytes}. */
    private static void assertBytes(long bytes, String text) {
        assertBytes(BigInteger.valueOf(bytes), text);
    }

    /** Asserts that {@code text} parses to {@code bytes}. */
    private static void assertBytes(BigInteger bytes, String text) {
        Assert.assertEquals(text, bytes, ConfigSize.parseBytes(text));
    }

    /** Asserts that {@code text} is rejected with a message containing {@code expected}. */
    private static void assertRejected(String text, String expected) {
        try {
            ConfigSize.parseBytes(text);
            Assert.fail("expected " + text + " to be rejected");
        } catch (ConfigQuantity.ParseException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expected));
        }
    }

    @Test
    public void everyUnit() {
        assertBytes(0, "0");
        assertBytes(4096, "4096");
        assertBytes(4096, "4096B");
        assertBytes(4096, "+4096");
        assertBytes(1_000, "1k");
        assertBytes(1_000, "1K");
        assertBytes(1_000, "1kB");
        assertBytes(KIB, "1Ki");
        assertBytes(KIB, "1KiB");
        assertBytes(500_000_000, "500M");
        assertBytes(512 * MIB, "512Mi");
        assertBytes(512 * MIB, "512MiB");
        assertBytes(1_000_000_000, "1G");
        assertBytes(GIB, "1Gi");
        assertBytes(1_000_000_000_000L, "1T");
        assertBytes(1L << 40, "1Ti");
        assertBytes(1_000_000_000_000_000L, "1P");
        assertBytes(1L << 50, "1Pi");
        assertBytes(1_000_000_000_000_000_000L, "1E");
        assertBytes(1L << 60, "1Ei");
        assertBytes(new BigInteger("18446744073709551615"), "18446744073709551615");
    }

    @Test
    public void fractionsScaleExactly() {
        assertBytes(GIB + GIB / 2, "1.5Gi");
        assertBytes(MIB / 2, "0.5Mi");
        assertBytes(2_500, "2.5k");
        assertBytes(GIB, "1.Gi");
        assertBytes(MIB / 2, ".5Mi");
        // Rounded to the nearest byte, halves away from zero.
        assertBytes(1, "0.5");
        assertBytes(0, "0.4");
        assertBytes(1_001, "1.0005k");
        assertBytes(1_000, "1.0004k");
        // Digits past the eighteenth are dropped, not rounded.
        assertBytes(0, "0.0000000000000000001Ei");
        assertBytes((1L << 60) + 1, "1.000000000000000001Ei");
    }

    @Test
    public void rejectsBadInput() {
        assertRejected("", "size is empty");
        assertRejected("+", "size is empty");
        assertRejected("-1Gi", "cannot be negative");
        assertRejected("--1", "cannot be negative");
        assertRejected("1m", "unknown unit 'm' in size '1m'");
        assertRejected("1g", "unknown unit 'g'");
        assertRejected("1b", "unknown unit 'b'");
        assertRejected("1 Mi", "unknown unit ' Mi'");
        assertRejected("1Gi512Mi", "unknown unit 'Gi512Mi'");
        assertRejected("1e3", "unknown unit 'e3'");
        assertRejected("1..2", "'1..2' is not a number");
        assertRejected("lots", "unknown unit 'lots'");
        assertRejected("Mi", "'Mi' has no number in front of it");
        assertRejected(".", "'.' is not a number");
        assertRejected("B", "'B' has no number in front of it");
        assertRejected("18446744073709551616", "too large");
        assertRejected("16Ei", "too large");
        assertRejected("19E", "too large");
    }

    /** The unit list in the messages names every unit the parser reads. */
    @Test
    public void unitsAreListedInMessages() {
        try {
            ConfigSize.parseBytes("1x");
            Assert.fail();
        } catch (ConfigQuantity.ParseException e) {
            for (String unit : new String[] { "k", "M", "G", "T", "P", "E", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei" })
                Assert.assertTrue(unit, e.getMessage().contains("\"" + unit + "\""));
            Assert.assertTrue(e.getMessage().contains("trailing \"B\""));
        }
    }
}
