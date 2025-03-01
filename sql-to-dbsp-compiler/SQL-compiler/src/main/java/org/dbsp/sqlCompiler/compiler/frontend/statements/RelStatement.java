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
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteSqlNode;
import org.dbsp.util.ICastable;

/** Base class for statements produced by the compiler front-end.
 * The representation is mostly at the level of RelNode, but there
 * is also some SqlNode-level information. */
public abstract class RelStatement implements ICastable {
    public final ParsedStatement parsedStatement;

    protected RelStatement(ParsedStatement parsedStatement) {
        this.parsedStatement = parsedStatement;
    }

    public CalciteSqlNode getCalciteObject() {
        return CalciteObject.create(this.parsedStatement.statement());
    }

    public String getStatement() {
        return this.parsedStatement.toString();
    }

    /** True if this is code written by the user, false if it's system-inserted code */
    public boolean isVisible() {
        return this.parsedStatement.visible();
    }

    public SourcePositionRange getPosition() {
        if (this.parsedStatement.visible())
            return new SourcePositionRange(this.parsedStatement.statement().getParserPosition());
        else
            return SourcePositionRange.INVALID;
    }
}
