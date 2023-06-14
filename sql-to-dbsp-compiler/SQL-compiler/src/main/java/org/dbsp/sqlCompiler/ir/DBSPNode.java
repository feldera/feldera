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

package org.dbsp.sqlCompiler.circuit;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustVisitor;

import javax.annotation.Nullable;

/**
 * Base interface for all DBSP nodes.
 */
public abstract class DBSPNode
        implements IDBSPNode {
    static long crtId = 0;

    public final long id;

    /**
     * Original query Sql node that produced this node.
     */
    private final @Nullable
    Object node;

    protected DBSPNode(@Nullable Object node) {
        this.node = node;
        this.id = crtId++;
    }

    @Nullable
    public Object getNode() { return this.node; }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public String toString() {
        // This should only be used for debugging.
        // Otherwise, the compiler options used to generate this string may not match
        // what the actual compiler options are.
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        if (this.is(IDBSPInnerNode.class))
            return ToRustInnerVisitor.toRustString(compiler, this.to(IDBSPInnerNode.class), false);
        return ToRustVisitor.toRustString(compiler, this.to(IDBSPOuterNode.class));
    }
}
