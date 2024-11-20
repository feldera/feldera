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
 *
 *
 */

package org.dbsp.util;

import java.util.HashMap;
import java.util.Map;

/** Used to generate new names during a program execution. */
public class NameGen {
    private final String prefix;

    static final Map<String, Integer> nextId = new HashMap<>();

    @SuppressWarnings("unused")
    public NameGen() {
        this.prefix = "id";
    }

    /**
     * Create a new name generator that generates names starting with the
     * specified prefix.  Generated names will look like prefixN, where
     * N is an increasing number.
     * @param prefix  Prefix for all names.
     */
    public NameGen(String prefix) {
        this.prefix = prefix;
        if (!nextId.containsKey(this.prefix))
            nextId.put(this.prefix, 0);
    }

    public int getNext() {
        if (!nextId.containsKey(this.prefix))
            nextId.put(this.prefix, 0);
        return nextId.get(this.prefix);
    }

    public String nextName() {
        int id = this.getNext();
        nextId.put(this.prefix, id+1);
        return this.prefix + id;
    }

    /**
     * Do not use this method.
     * It is for testing only.
     */
    public static void reset() {
        nextId.clear();
    }
}
