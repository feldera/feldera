package org.dbsp.sqllogictest;

import org.junit.Test;

import java.io.IOException;

/** A unit test that invokes tests in rotation */
public class RotateTests {
    @Test
    public void rotate() throws IOException, ClassNotFoundException {
        Main.rotateTests();
    }

    @Test
    public void quick() throws IOException, ClassNotFoundException {
        Main.quick();
    }
}
