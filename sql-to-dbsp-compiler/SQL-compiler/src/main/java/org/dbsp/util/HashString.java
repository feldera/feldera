package org.dbsp.util;

import javax.annotation.Nullable;

/** A Hash value produced by SHA256 */
public record HashString(String value) {
    /** Number of digits that is safe to use in abbreviations */
    static final int SHORT_SIZE = 12;

    @Override
    public String toString() {
        return this.value;
    }

    public String shortString() {
        // Hopefully there are no collisions in the first 12 digits.
        return this.value.substring(0, SHORT_SIZE);
    }

    public String makeIdentifier(@Nullable String prefix) {
        if (prefix == null)
            prefix = "s";
        return prefix + "_" + this.shortString();
    }
}
