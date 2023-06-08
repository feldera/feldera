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

package org.dbsp.sqlCompiler.compiler.backend.optimize;

import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A set of nested contexts where substitution is performed.
 * Each context is a namespace which can define new substitutions, which may
 * shadow substitutions in the outer contexts.
 */
public class SubstitutionContext<T> {
    protected final List<Substitution<T>> stack;

    public SubstitutionContext() {
        this.stack = new ArrayList<>();
    }

    public void newContext() {
        this.stack.add(new Substitution<>());
    }

    public void popContext() {
        Utilities.removeLast(this.stack);
    }

    public void substitute(String name, @Nullable T value) {
        if (this.stack.isEmpty())
            throw new RuntimeException("Empty context");
        this.stack.get(this.stack.size() - 1).substitute(name, value);
    }

    public void mustBeEmpty() {
        if (!this.stack.isEmpty())
            throw new RuntimeException("Non-empty context");
    }

    /**
     * The substitution for this name.
     * null if there is a tombstone or no substitution.
     */
    @Nullable
    public T get(String name) {
        for (int i = 0; i < this.stack.size(); i++) {
            int index = this.stack.size() - i - 1;
            Substitution<T> subst = this.stack.get(index);
            if (subst.contains(name))
                return subst.getReplacement(name);
        }
        return null;
    }

    /**
     * True if there is a substitution for this variable.
     */
    public boolean containsSubstitution(String name) {
        for (int i = 0; i < this.stack.size(); i++) {
            int index = this.stack.size() - i - 1;
            Substitution<T> subst = this.stack.get(index);
            if (subst.contains(name)) {
                T expression = subst.getReplacement(name);
                return expression != null;
            }
        }
        return false;
    }
}
