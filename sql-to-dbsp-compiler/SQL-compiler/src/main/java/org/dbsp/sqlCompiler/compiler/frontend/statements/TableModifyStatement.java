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

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;

import javax.annotation.Nullable;
import java.util.List;

/** Describes an INSERT or REMOVE statement. */
public class TableModifyStatement extends RelStatement {
    public final ProgramIdentifier tableName;
    /** Exact ordered destination selectors; null means the implicit table order.
     * Diagnostic spelling is separate from the resolved identifier. */
    public record TargetColumn(ProgramIdentifier name, SourcePositionRange position, String displayName) { }
    @Nullable public final List<TargetColumn> targetColumns;
    @Nullable
    public RelNode rel;
    /** True for insert, false for remove */
    public final boolean insert;

    public TableModifyStatement(ParsedStatement node, boolean insert, ProgramIdentifier tableName,
                                @Nullable List<TargetColumn> targetColumns) {
        super(node);
        this.insert = insert;
        this.tableName = tableName;
        this.targetColumns = targetColumns == null ? null : List.copyOf(targetColumns);
        this.rel = null;
    }

    public void setTranslation(RelNode rel) {
        this.rel = rel;
    }

    public String getSqlOperationName() {
        return this.insert ? "INSERT" : "REMOVE";
    }
}
