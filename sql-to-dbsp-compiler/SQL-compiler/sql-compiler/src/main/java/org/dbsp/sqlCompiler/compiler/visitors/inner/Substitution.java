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

package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Describes a substitution from names to values.
 */
public class Substitution<T> {
    /**
     * Maps names to values.
     */
    final Map<String, T> replacement;
    /**
     * When a variable is defined it shadows parameters with the same
     * name.  These are represented in the tombstones.
     */
    final Set<String> tombstone;

    public Substitution() {
        this.replacement = new HashMap<>();
        this.tombstone = new HashSet<>();
    }

    public void substitute(String name, @Nullable T expression) {
        if (expression == null)
            this.tombstone.add(name);
        else
            this.replacement.put(name, expression);
    }

    /**
     * Returns null if there is a tombstone with this name.
     * Returns the substitution otherwise.
     * Throws if there is no such substitution.
     */
    @Nullable
    public T getReplacement(String name) {
        if (this.tombstone.contains(name))
            return null;
        return Utilities.getExists(this.replacement, name);
    }

    public boolean contains(String name) {
        return this.tombstone.contains(name) ||
                this.replacement.containsKey(name);
    }

    public void clear() {
        this.replacement.clear();
        this.tombstone.clear();
    }

    @Override
    public String toString() {
        return this.replacement +
            System.lineSeparator() + "HIDDEN: " + this.tombstone;
    }
}
