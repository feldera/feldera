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

package org.dbsp.sqlCompiler.compiler.backend.jit.ir;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.util.IIndentStream;

/**
 * An objects that refers to another one by its id.
 * An invalid reference has a negative id.
 */
public class JITReference extends JITNode {
    public final long id;

    public JITReference(long id) {
        this.id = id;
    }

    /**
     * Create an invalid reference.
     */
    public JITReference() {
        this.id = -1;
    }

    public long getId() {
        return this.id;
    }

    @Override
    public String toString() {
        if (this.isValid())
            return Long.toString(this.id);
        else
            return "#";
    }

    public boolean isValid() {
        return this.id >= 0L;
    }

    public void mustBeValid() {
        if (!this.isValid())
            throw new InternalCompilerError("Expected a valid reference", this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JITReference that = (JITReference) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.toString());
    }
}
