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

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A set of nested scopes where substitution is performed.
 * Each scope is a namespace which can define new substitutions, which may
 * shadow substitutions in the outer contexts. */
public class Scopes<K, V> {
    protected final List<Substitution<K, V>> stack;

    public Scopes() {
        this.stack = new ArrayList<>();
    }

    public void newContext() {
        this.stack.add(new Substitution<>());
    }

    public void popContext() {
        Utilities.removeLast(this.stack);
    }

    public void substitute(K key, V value) {
        if (this.stack.isEmpty())
            throw new InternalCompilerError("Empty context", CalciteObject.EMPTY);
        this.stack.get(this.stack.size() - 1).substitute(key, value);
    }

    public void mustBeEmpty() {
        if (!this.stack.isEmpty())
            throw new InternalCompilerError("Non-empty context", CalciteObject.EMPTY);
    }

    /**
     * The substitution for this key.
     * null if there isn't any. */
    @Nullable
    public V get(K name) {
        for (int i = 0; i < this.stack.size(); i++) {
            int index = this.stack.size() - i - 1;
            Substitution<K, V> subst = this.stack.get(index);
            if (subst.containsKey(name))
                return subst.get(name);
        }
        return null;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    public Scopes<K, V> clone() {
        Scopes<K, V> result = new Scopes<>();
        for (Substitution<K, V> subst: this.stack) {
            Substitution<K, V> copy = subst.clone();
            result.stack.add(copy);
        }
        return result;
    }

    void clear() {
        this.stack.clear();
    }

    @Override
    public String toString() {
        return this.stack.toString();
    }

    public void replace(Scopes<K, V> save) {
        this.stack.clear();
        this.stack.addAll(save.stack);
    }
}
