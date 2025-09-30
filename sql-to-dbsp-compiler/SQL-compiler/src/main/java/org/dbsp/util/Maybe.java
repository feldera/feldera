package org.dbsp.util;

/** Ternary logic values */
public enum Maybe {
    NO,
    MAYBE,
    YES;

    public boolean toBool() {
        return switch (this) {
            case NO -> false;
            case MAYBE -> throw new RuntimeException("Cannot convert MAYBE to BOOLEAN");
            case YES -> true;
        };
    }
}
