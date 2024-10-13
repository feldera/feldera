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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlIdentifier;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.parser.PropertyList;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragment;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** The representation of a CREATE VIEW AS ... DDL statement. */
public class CreateViewStatement extends CreateRelationStatement {
    /** Compiled and optimized query. */
    private final RelRoot compiled;
    public final SqlCreateView createView;
    public final SqlCreateView.ViewKind viewKind;
    public static final String EMIT_FINAL = "emit_final";
    public static final int NO_COLUMN = -1;

    public CreateViewStatement(CalciteCompiler.ParsedStatement node, String tableName,
                               boolean nameIsQuoted, List<RelColumnMetadata> columns, SqlCreateView createView,
                               RelRoot compiled, @Nullable PropertyList properties) {
        super(node, tableName, nameIsQuoted, columns, properties);
        this.viewKind = createView.viewKind;
        this.createView = createView;
        this.compiled = compiled;
    }

    public RelNode getRelNode() {
        return Objects.requireNonNull(this.compiled).rel;
    }

    public RelRoot getRoot() {
        return this.compiled;
    }

    @Override
    public JsonNode asJson() {
        JsonNode node = super.asJson();
        ObjectNode object = (ObjectNode) node;
        object.put("materialized", viewKind == SqlCreateView.ViewKind.MATERIALIZED);
        return object;
    }

    /** Column number specified by "emit_final" annotation.  -1 if no such annotation */
    public int emitFinalColumn() {
        if (this.properties == null)
            return NO_COLUMN;
        SqlFragment val = this.properties.getPropertyValue(EMIT_FINAL);
        if (val == null)
            return NO_COLUMN;
        try {
            int index = Integer.parseInt(val.getString());
            if (index >= 0 && index < this.columns.size())
                return index;
            throw new CompilationError("View " + Utilities.singleQuote(relationName) +
                    " does not have a column with number " + index,
                    CalciteObject.create(val.getParserPosition()));
        } catch (NumberFormatException ignored) {}

        return this.getColumnIndex(new SqlIdentifier(val.getString(), val.getParserPosition()));
    }
}
