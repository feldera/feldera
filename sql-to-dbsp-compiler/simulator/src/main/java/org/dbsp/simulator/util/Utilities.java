package org.dbsp.simulator.util;

import javax.annotation.Nullable;

public class Utilities {
    private Utilities() {}

    /**
     * Just adds single quotes around a string.  No escaping is performed.
     */
     public static String singleQuote(@Nullable String other) {
         return "'" + other + "'";
     }
}
