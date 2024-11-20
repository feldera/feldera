/*
 * Copyright 2023 VMware, Inc.
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

import java.util.Set;

/** Generates a fresh name that does not appear in a set of used names.
 * The name is a legal identifier in many languages if the prefix supplied to
 * 'freshName' also is.  The names are composed only of the prefix, underscores, and digits. */
public class FreshName {
    final Set<String> used;

    /** @param used Keep track of the used names in this set.  */
    public FreshName(Set<String> used) {
        this.used = used;
    }

    /**
     * Generate a fresh name starting with the specified prefix.
     *
     * @param prefix  Prefix for the new name.
     * @param remember If true, add the generated name to the set of used names. */
    public String freshName(String prefix, boolean remember) {
        String name = prefix;
        long counter = 0;
        while (this.used.contains(name)) {
            name = prefix + "_" + counter;
            counter++;
        }
        if (remember)
            this.used.add(name);
        return name;
    }
}
