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

import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/** Base class for CreateTableStatement and CreateViewStatement. */
public abstract class CreateRelationStatement
        extends FrontEndStatement
        implements IHasSchema {
    public final String relationName;
    public final boolean nameIsQuoted;
    public final List<RelColumnMetadata> columns;
    @Nullable final Map<String, String> connectorProperties;

    protected CreateRelationStatement(SqlNode node, String statement, String relationName,
                                      boolean nameIsQuoted,
                                      List<RelColumnMetadata> columns,
                                      @Nullable Map<String, String> connectorProperties) {
        super(node, statement);
        this.nameIsQuoted = nameIsQuoted;
        this.relationName = relationName;
        this.columns = columns;
        this.connectorProperties = connectorProperties;
    }

    public AbstractTable getEmulatedTable() {
        return new CalciteTableDescription(this);
    }

    public String getName() {
        return this.relationName;
    }

    public List<RelColumnMetadata> getColumns() {
        return this.columns;
    }

    @Nullable public Map<String, String> getConnectorProperties() { return this.connectorProperties; }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (RelColumnMetadata col: this.columns) {
            builder.append(col.toString());
            builder.append(",");
        }

        return "CreateRelationStatement{" +
                "tableName='" + this.relationName + '\'' +
                ", columns=" + builder +
                '}';
    }

    @Override
    public boolean nameIsQuoted() {
        return this.nameIsQuoted;
    }

    public CalciteObject getNode() {
        return this.getCalciteObject();
    }
}
