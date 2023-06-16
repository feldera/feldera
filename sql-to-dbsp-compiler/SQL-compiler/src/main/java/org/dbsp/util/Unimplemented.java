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

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.IDBSPNode;

import javax.annotation.Nullable;

/**
 * Exception thrown for code that is not yet implemented.
 */
public class Unimplemented extends RuntimeException {
    @Nullable
    public final IDBSPNode dbspNode;
    @Nullable
    public final CalciteObject calciteObject;

    public Unimplemented() {
        super("Not yet implemented");
        this.dbspNode = null;
        this.calciteObject = null;
    }

    public Unimplemented(String message) {
        super("Not yet implemented" + message);
        this.dbspNode = null;
        this.calciteObject = null;
    }

    public Unimplemented(IDBSPNode node) {
        super("Not yet implemented: " + node.getClass().getSimpleName() + ":" + node);
        this.dbspNode = node;
        this.calciteObject = null;
    }

    public Unimplemented(CalciteObject node) {
        super("Not yet implemented: " + node.getClass().getSimpleName() + ":" + node);
        this.dbspNode = null;
        this.calciteObject = node;
    }

    public Unimplemented(String message, IDBSPNode node) {
        super("Not yet implemented: " + message + " " + node.getClass().getSimpleName() + ":" + node);
        this.dbspNode = node;
        this.calciteObject = null;
    }

    public Unimplemented(String message, CalciteObject node) {
        super("Not yet implemented: " + message + " " + node.getClass().getSimpleName() + ":" + node);
        this.dbspNode = null;
        this.calciteObject = node;
    }

    public Unimplemented(IDBSPNode node, Throwable cause) {
        super("Not yet implemented: "
                        + cause.getMessage()
                        + System.lineSeparator()
                        + node
                , cause);
        this.dbspNode = node;
        this.calciteObject = null;
    }

    public Unimplemented(CalciteObject node, Throwable cause) {
        super("Not yet implemented: "
                        + cause.getMessage()
                        + System.lineSeparator()
                        + node
                , cause);
        this.dbspNode = null;
        this.calciteObject = node;
    }

    public SourcePositionRange getPositionRange() {
        if (this.dbspNode != null)
            return this.dbspNode.getNode().getPositionRange();
        else if (this.calciteObject != null)
            return this.calciteObject.getPositionRange();
        return SourcePositionRange.INVALID;
    }
}
