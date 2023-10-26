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

package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.util.Linq;

/**
 * Represents a Semigroup trait implementation.
 * TODO: this probably should not exist, but it matches the DBSP APIs.
 */
public class DBSPTypeSemigroup extends DBSPTypeUser {
    public DBSPTypeSemigroup(DBSPType[] elementTypes, DBSPType[] semigroupTypes) {
        super(CalciteObject.EMPTY, DBSPTypeCode.SEMIGROUP, "Semigroup" + elementTypes.length, false,
                Linq.concat(semigroupTypes, elementTypes));
        if (elementTypes.length != semigroupTypes.length)
            throw new InternalCompilerError("Each element must have a corresponding semigroup, but I have " +
                    elementTypes.length + " and " + semigroupTypes.length, this);
    }

    public int semigroupSize() {
        return this.typeArgs.length / 2;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        for (DBSPType type: this.typeArgs)
            type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }
}
