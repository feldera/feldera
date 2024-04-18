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

import org.dbsp.sqlCompiler.compiler.IHasSourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.IDBSPNode;

import javax.annotation.Nullable;

/** Exception thrown when the compiler encounters a code
 * construct that is not yet fully implemented.  This is a legal construct,
 * and the compiler should eventually support it. */
public class UnimplementedException
        extends BaseCompilerException
        implements IHasSourcePositionRange {
    @Nullable
    public final IDBSPNode dbspNode;

    public static final String kind = "Not yet implemented";

    protected UnimplementedException(String message, @Nullable IDBSPNode node, CalciteObject object) {
        super(message, object);
        this.dbspNode = node;
    }

    public UnimplementedException() {
        this(kind, null, CalciteObject.EMPTY);
    }

    public UnimplementedException(String message) {
        this(message, null, CalciteObject.EMPTY);
    }

    public UnimplementedException(IDBSPNode node) {
        this(node.getClass().getSimpleName() + ":" + node,
                node, CalciteObject.EMPTY);
    }

    public UnimplementedException(CalciteObject object) {
        this(object.toString(), null, object);
    }

    public UnimplementedException(String message, IDBSPNode node) {
        this(message + " " + node.getClass().getSimpleName() + ":" + node,
                node, CalciteObject.EMPTY);
    }

    public UnimplementedException(String message, CalciteObject node) {
        this(message + " " + node,
                null, node);
    }

    public UnimplementedException(IDBSPNode node, Throwable cause) {
        super(cause.getMessage()
                        + System.lineSeparator()
                        + node
                , CalciteObject.EMPTY, cause);
        this.dbspNode = node;
    }

    public UnimplementedException(CalciteObject node, Throwable cause) {
        super(cause.getMessage()
                        + System.lineSeparator()
                        + node
                , CalciteObject.EMPTY, cause);
        this.dbspNode = null;
    }

    @Override
    public SourcePositionRange getPositionRange() {
        if (this.dbspNode != null)
            return this.dbspNode.getNode().getPositionRange();
        return this.calciteObject.getPositionRange();
    }

    @Override
    public String getErrorKind() {
        return kind;
    }
}
