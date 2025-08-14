/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.util;

import org.dbsp.sqlCompiler.compiler.errors.CompilationError;

import java.util.HashMap;
import java.util.Map;

/** Logging class which can output nicely indented strings.
 * The logger extends IndentStream, and thus provides the capability
 * to output nicely indented hierarchical visualizations. */
public class Logger {
    private final Map<Class<?>, Integer> loggingLevel = new HashMap<>();
    private final IndentStream debugStream;
    private final IIndentStream noStream;

    /** There is only one instance of the logger for the whole program. */
    public static final Logger INSTANCE = new Logger();

    private Logger() {
        this.debugStream = new IndentStream(System.err);
        this.noStream = new NullIndentStream();
    }

    /** Get the logging stream for messages below this logging level.
     * @param clazz   Class which does the logging.
     * @param level   Level of message that is being logged.
     * @return        A stream where the message can be appended. */
    public IIndentStream belowLevel(Class<?> clazz, int level) {
        int debugLevel = this.getLoggingLevel(clazz);
        if (debugLevel >= level)
            return this.debugStream;
        return this.noStream;
    }

    /**
     * Get the logging stream for messages below this logging level.
     * @param module  Module which does the logging.
     * @param level   Level of message that is being logged.
     * @return        A stream where the message can be appended. */
    public IIndentStream belowLevel(IWritesLogs module, int level) {
        return this.belowLevel(module.getClass(), level);
    }

    /** Debug level is controlled per module and can be changed dynamically.
     * @param clazz   Class.
     * @param level   Debugging level.
     * @return Previous logging level for this module. */
    public int setLoggingLevel(Class<?> clazz, int level) {
        int previous = this.loggingLevel.getOrDefault(clazz, 0);
        this.loggingLevel.put(clazz, level);
        return previous;
    }

    static final String root = "org.dbsp.sqlCompiler.compiler";

    /* Packages containing classes that can be instrumented with logging,
    * relative to root. */
    static final String[] packages = new String[] {
            "",
            "visitors.inner",
            "visitors.outer",
            "visitors.multi",
            "backend",
            "backend.rust",
            "backend.rust.multi",
            "visitors.outer.monotonicity",
            "visitors.unusedFields",
            "frontend",
            "frontend.calciteCompiler"
    };

    Class<?> locateClass(String className) {
        for (String pack: packages) {
            String path = pack.isEmpty() ? root : root + "." + pack;
            try {
                path += "." + className;
                return Class.forName(path);
            } catch (ClassNotFoundException e) {
                // continue
            }
        }
        throw new CompilationError("Class " + className + " not found for setting up logging");
    }

    /** Debug level is controlled per module and can be changed dynamically.
     * @param className   Class; must be a visitor.
     * @param level   Debugging level.
     * @return Previous logging level for this module. */
    @SuppressWarnings("UnusedReturnValue")
    public int setLoggingLevel(String className, int level) {
        Class<?> clazz = this.locateClass(className);
        return this.setLoggingLevel(clazz, level);
    }

    public <T> int getLoggingLevel(Class<T> clazz) {
        if (this.loggingLevel.isEmpty())
            return 0;
        for (var e: this.loggingLevel.entrySet()) {
            Class<?> c = e.getKey();
            if (c.isAssignableFrom(clazz))
                return e.getValue();
        }
        return 0;
    }

    /**
     * Where logging should be redirected.
     * Notice that the indentation is *not* reset when the stream is changed. */
    public Appendable setDebugStream(Appendable writer) {
        return this.debugStream.setOutputStream(writer);
    }
}
