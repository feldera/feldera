package org.dbsp.util;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** An immutable string of Unicode characters, stored in UTF-8 like the runtime's strings.
 * Lengths count code points, and the order is the order of the code points.
 * Java Strings count UTF-16 code units instead, and compare differently. */
public final class Utf8String implements Comparable<Utf8String> {
    private final byte[] bytes;

    public static final Utf8String EMPTY = new Utf8String(new byte[0]);

    private Utf8String(byte[] bytes) {
        this.bytes = bytes;
    }

    /** Converts a Java string, which must not have an unpaired surrogate */
    public static Utf8String fromJava(String value) {
        Utilities.enforce(value.codePoints().noneMatch(
                c -> c >= Character.MIN_SURROGATE && c <= Character.MAX_SURROGATE),
                () -> "String has an unpaired surrogate: " + value);
        return new Utf8String(value.getBytes(StandardCharsets.UTF_8));
    }

    /** True if the byte starts a code point */
    static boolean isLeadingByte(byte b) {
        return (b & 0xC0) != 0x80;
    }

    public int codePointCount() {
        int count = 0;
        for (byte b : this.bytes)
            if (isLeadingByte(b))
                count++;
        return count;
    }

    /** The first codePoints code points, or the whole string if it is shorter */
    public Utf8String prefix(int codePoints) {
        int count = 0;
        for (int i = 0; i < this.bytes.length; i++) {
            if (isLeadingByte(this.bytes[i])) {
                if (count == codePoints)
                    return new Utf8String(Arrays.copyOf(this.bytes, i));
                count++;
            }
        }
        return this;
    }

    /** Appends spaces until the string has codePoints code points */
    public Utf8String padRight(int codePoints) {
        int missing = codePoints - this.codePointCount();
        if (missing <= 0)
            return this;
        byte[] result = Arrays.copyOf(this.bytes, this.bytes.length + missing);
        Arrays.fill(result, this.bytes.length, result.length, (byte) ' ');
        return new Utf8String(result);
    }

    /** The text if it is all ASCII, else null.  Java parsers accept digits that the runtime rejects. */
    @Nullable
    public String asciiText() {
        for (byte b : this.bytes)
            if (b < 0)
                return null;
        return this.toJavaString();
    }

    /** The value as an int, parsed like the runtime's CAST: the characters that Java's trim
     * removes are ignored, and the rest must be ASCII digits with an optional sign.
     * Null if the text is not such a number, or if it does not fit. */
    @Nullable
    public Integer asInt() {
        Long value = this.asLong();
        if (value == null || value != value.intValue())
            return null;
        return value.intValue();
    }

    /** The value as a long, parsed like {@link #asInt} */
    @Nullable
    public Long asLong() {
        String text = this.asciiText();
        if (text == null)
            return null;
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    /** The text as a Java string, for names, messages, and output.  Its
     * length and indexes count UTF-16 code units. */
    public String toJavaString() {
        return new String(this.bytes, StandardCharsets.UTF_8);
    }

    @Override
    public int compareTo(Utf8String other) {
        // UTF-8 byte order is the order of the code points
        return Arrays.compareUnsigned(this.bytes, other.bytes);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Utf8String other && Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.bytes);
    }

    @Override
    public String toString() {
        return this.toJavaString();
    }
}
