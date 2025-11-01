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

    public static final String KIND = "Not yet implemented";
    public static final String SUGGEST = "You can suggest support for this feature by filing an issue at https://github.com/feldera/feldera/issues";

    static String concat(String... strings) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String str: strings) {
            if (str.isEmpty()) continue;
            if (!first)
                builder.append(System.lineSeparator());
            first = false;
            builder.append(str);
        }
        return builder.toString();
    }

    static String makeMessage(String message, String alternative) {
        if (!message.contains("feldera/issues"))
            return concat(message, SUGGEST, alternative);
        else return concat(message, alternative);
    }

    protected UnimplementedException(String message, @Nullable IDBSPNode node, CalciteObject object) {
        super(makeMessage(message, ""), object);
        this.dbspNode = node;
    }

    public UnimplementedException(String message, int issue, String suggestion, CalciteObject object) {
        this(makeMessage(message + System.lineSeparator() +
                "This is tracked by issue https://github.com/feldera/feldera/issues/" +
                issue, suggestion), object);
    }

    public UnimplementedException(String message, int issue, CalciteObject object) {
        this(makeMessage(message + System.lineSeparator() +
                "This is tracked by issue https://github.com/feldera/feldera/issues/" +
                issue, ""), object);
    }

    public UnimplementedException() {
        this(KIND, null, CalciteObject.EMPTY);
    }

    public UnimplementedException(String message) {
        this(message, null, CalciteObject.EMPTY);
    }

    public UnimplementedException(String message, IDBSPNode node) {
        this(message + " " + node.getClass().getSimpleName() + ":" + node,
                node, CalciteObject.EMPTY);
    }

    public UnimplementedException(String message, CalciteObject node) {
        this(message, null, node);
    }

    public UnimplementedException(IDBSPNode node, Throwable cause) {
        super(makeMessage(cause.getMessage()
                        + System.lineSeparator()
                        + node, "")
                , node.getNode(), cause);
        this.dbspNode = node;
    }

    public UnimplementedException(CalciteObject node, Throwable cause) {
        super(makeMessage(cause.getMessage()
                        + System.lineSeparator()
                        + node, "")
                , node, cause);
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
        return KIND;
    }
}
