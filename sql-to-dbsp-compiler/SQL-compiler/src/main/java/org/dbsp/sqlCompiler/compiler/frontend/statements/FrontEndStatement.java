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

package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.ICastable;

/** Base class for statements produced by the compiler front-end.
 * The representation is mostly at the level of RelNode, but there
 * is also some SqlNode-level information. */
public abstract class FrontEndStatement implements ICastable {
    public final CalciteCompiler.ParsedStatement node;
    /** Original statement compiled. */
    public final String statement;

    protected FrontEndStatement(CalciteCompiler.ParsedStatement node, String statement) {
        this.node = node;
        this.statement = statement;
    }

    protected FrontEndStatement(CalciteCompiler.ParsedStatement node) {
        this(node, node.toString());
    }

    public CalciteObject getCalciteObject() {
        return CalciteObject.create(this.node.statement());
    }

    public SourcePositionRange getPosition() {
        if (this.node.visible())
            return new SourcePositionRange(this.node.statement().getParserPosition());
        else
            return SourcePositionRange.INVALID;
    }
}
