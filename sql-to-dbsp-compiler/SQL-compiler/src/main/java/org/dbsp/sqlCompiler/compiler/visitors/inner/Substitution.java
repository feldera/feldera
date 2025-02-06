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

import org.dbsp.util.TriFunction;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Substitution<K, V> extends HashMap<K, V> {
    public Substitution() {}

    public Substitution(HashMap<K, V> data) {
        data.forEach(this::substitute);
    }

    public void substitute(K name, V value) {
        this.put(name, value);
    }

    public void substituteNew(K key, V value) {
        Utilities.putNew(this, key, value);
    }

    public Substitution<K, V> clone() {
        return new Substitution<>(this);
    }

    public Substitution<K, V> mergeWith(
            Substitution<K, V> other, TriFunction<K, V, V, V> merger) {
        if (this.equals(other))
            return this;

        Substitution<K, V> result = new Substitution<>();
        Set<K> commonKeys = new HashSet<>(this.keySet());
        commonKeys.addAll(other.keySet());

        for (K k: commonKeys) {
            V thisValue = this.get(k);
            V otherValue = other.get(k);
            V merged = merger.apply(k, thisValue, otherValue);
            if (merged != null)
                result.substitute(k, merged);
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (Map.Entry<K, V> entry: this.entrySet()) {
            builder.append(entry.toString()).append(",");
        }
        builder.append("]");
        return builder.toString();
    }
}
