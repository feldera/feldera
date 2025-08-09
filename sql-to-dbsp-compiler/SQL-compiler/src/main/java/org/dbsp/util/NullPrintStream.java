package org.dbsp.util;

import java.io.PrintStream;

public class NullPrintStream extends PrintStream {
    public static final NullPrintStream INSTANCE = new NullPrintStream();

    private NullPrintStream() {
        super(NullOutputStream.INSTANCE);
    }
}
