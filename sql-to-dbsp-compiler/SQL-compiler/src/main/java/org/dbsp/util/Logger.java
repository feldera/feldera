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

import java.util.HashMap;
import java.util.Map;

/**
 * Logging class which can output nicely indented strings.
 * The logger extends IndentStream, and thus provides the capability
 * to output nicely indented hierarchical visualizations.
 */
public class Logger implements IDebuggable {
    private final Map<String, Integer> debugLevel = new HashMap<>();
    private final IndentStream debugStream;
    private final IIndentStream noStream;

    /**
     * There is only one instance of the logger for the whole program.
     */
    public static final Logger INSTANCE = new Logger();

    private Logger() {
        this.debugStream = new IndentStream(System.err);
        this.noStream = new NullIndentStream();
    }

    public IIndentStream from(String module, int level) {
        int debugLevel = this.getDebugLevel(module);
        if (debugLevel >= level)
            return this.debugStream;
        return this.noStream;
    }

    /**
     * Get the logging stream.
     * @param module  Module which does the logging.
     * @param level   Level of message that is being logged.
     * @return        A stream where the message can be appended.
     */
    public IIndentStream from(IModule module, int level) {
        return this.from(module.getModule(), level);
    }

    /**
     * Debug level is controlled per module and can be changed dynamically.
     * @param module  Module name.
     * @param level   Debugging level.
     */
    @Override
    public void setDebugLevel(String module, int level) {
        this.debugLevel.put(module, level);
    }

    /**
     * The current debug level for the specified module.
     */
    public int getDebugLevel(String module) {
        return this.debugLevel.getOrDefault(module, 0);
    }

    public <T> int getDebugLevel(Class<T> clazz) {
        return this.debugLevel.getOrDefault(clazz.getSimpleName(), 0);
    }

    /**
     * Where logging should be redirected.
     * Notice that the indentation is *not* reset when the stream is changed.
     */
    @Override
    public Appendable setDebugStream(Appendable writer) {
        return this.debugStream.setOutputStream(writer);
    }
}
