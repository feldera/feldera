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

package org.dbsp.sqlCompiler.ir;

import org.dbsp.sqlCompiler.compiler.IHasCalciteObject;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.util.ICastable;
import org.dbsp.util.IHasId;
import org.dbsp.util.ToIndentableString;

import javax.annotation.Nullable;

/**
 * An IR node that is used to represent DBSP circuits.
 */
@SuppressWarnings("unused")
public interface IDBSPNode extends ICastable, IHasId, ToIndentableString, IHasCalciteObject {
    default <T> T checkNull(@Nullable T value) {
        if (value == null)
            this.error("Null pointer");
        if (value == null)
            throw new InternalCompilerError("Did not expect a null value", this);
        return value;
    }

    default void error(String message) {
        throw new InternalCompilerError(message, this.getNode());
    }
}
