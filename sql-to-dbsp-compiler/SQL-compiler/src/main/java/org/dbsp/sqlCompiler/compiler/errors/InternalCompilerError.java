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

package org.dbsp.sqlCompiler.compiler.errors;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.IDBSPNode;

import javax.annotation.Nullable;

/** Signals a bug in the compiler -- some expected invariant doesn't hold. */
public final class InternalCompilerError extends BaseCompilerException {
    @Nullable
    public final IDBSPNode dbspNode;

    InternalCompilerError(String message, CalciteObject node,
                          @Nullable IDBSPNode dbspNode) {
        super(message, node);
        this.dbspNode = dbspNode;
    }

    public InternalCompilerError(String message, CalciteObject node) {
        this(message, node, null);
    }

    public InternalCompilerError(String message) {
        this(message, CalciteObject.EMPTY, null);
    }

    public InternalCompilerError(String message, IDBSPNode node) {
        this(message, CalciteObject.EMPTY, node);
    }

    @Override
    public SourcePositionRange getPositionRange() {
        if (this.dbspNode != null)
            return this.dbspNode.getNode().getPositionRange();
        return this.calciteObject.getPositionRange();
    }

    @Override
    public String getErrorKind() {
        return "Compiler error";
    }
}
