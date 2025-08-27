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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.Linq;
import org.dbsp.util.Properties;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** Base class for CreateTableStatement and CreateViewStatement. */
public abstract class CreateRelationStatement
        extends RelStatement
        implements IHasSchema {
    public final ProgramIdentifier relationName;
    public final List<RelColumnMetadata> columns;
    @Nullable final Properties properties;

    protected CreateRelationStatement(ParsedStatement node, ProgramIdentifier relationName,
                                      List<RelColumnMetadata> columns, @Nullable Properties properties) {
        super(node);
        this.relationName = relationName;
        this.columns = columns;
        this.properties = properties;
    }

    public AbstractTable getEmulatedTable() {
        return new CalciteTableDescription(this);
    }

    public ProgramIdentifier getName() {
        return this.relationName;
    }

    public List<Integer> getPrimaryKeyColumns() {
        List<Integer> keys = new ArrayList<>();
        for (int i = 0; i < this.columns.size(); i++) {
            RelColumnMetadata meta = this.columns.get(i);
            if (meta.isPrimaryKey)
                keys.add(i);
        }
        return keys;
    }

    public List<RelColumnMetadata> getColumns() {
        return this.columns;
    }

    @Nullable public Properties getProperties() { return this.properties; }

    @Nullable
    public String getPropertyValue(String property) {
        if (this.properties == null)
            return null;
        return this.properties.getPropertyValue(property);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (RelColumnMetadata col: this.columns) {
            builder.append(col.toString());
            builder.append(",");
        }

        return this.getClass().getSimpleName() + "{" +
                "tableName='" + this.relationName + '\'' +
                ", columns=" + builder +
                '}';
    }

    public RelDataType getRowType() {
        return new RelRecordType(Linq.map(this.columns, c -> c.field));
    }

    public CalciteObject getNode() {
        return this.getCalciteObject();
    }
}
