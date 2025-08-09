package org.dbsp.util;

import java.io.IOException;
import java.io.OutputStream;

public class NullOutputStream extends OutputStream {
    public static final NullOutputStream INSTANCE = new NullOutputStream();

    public NullOutputStream() {
    }

    @Override
    public void write(final byte[] b) throws IOException {
    }

    @Override
    public void write(final byte[] b, final int off, final int len) {
    }

    @Override
    public void write(final int b) {
    }
}























