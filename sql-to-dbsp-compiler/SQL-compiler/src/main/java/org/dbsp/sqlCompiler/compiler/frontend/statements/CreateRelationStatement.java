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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for CreateTableStatement and CreateViewStatement.
 */
public abstract class CreateRelationStatement extends FrontEndStatement {
    public final String tableName;
    public final List<RelDataTypeField> columns;

    protected CreateRelationStatement(SqlNode node, String statement,
                                      String tableName, @Nullable String comment,
                                      List<RelDataTypeField> columns) {
        super(node, statement, comment);
        this.tableName = tableName;
        this.columns = columns;
    }

    public class EmulatedTable extends AbstractTable implements ScannableTable {
        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            // We don't plan to use this method, but the optimizer requires this API
            throw new UnsupportedException(new CalciteObject(node));
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (RelDataTypeField ci: CreateRelationStatement.this.columns)
                builder.add(ci);
            return builder.build();
        }

        public String getStatement() {
            return CreateRelationStatement.this.statement;
        }
    }

    public AbstractTable getEmulatedTable() {
        return new EmulatedTable();
    }

    /**
     * Return the index of the specified column.
     */
    public int getColumnIndex(SqlIdentifier id) {
        for (int i = 0; i < this.columns.size(); i++) {
            if (this.columns.get(i).getName().equals(id.toString()))
                return i;
        }
        throw new InternalCompilerError("Column not found", new CalciteObject(id));
    }

    public DBSPTypeTuple getRowType(TypeCompiler compiler) {
        List<DBSPType> fields = new ArrayList<>();
        for (RelDataTypeField col: this.columns) {
            DBSPType fType = compiler.convertType(col.getType());
            fields.add(fType);
        }
        return new DBSPTypeTuple(fields);
    }

    @Override
    public String toString() {
        return "CreateRelationStatement{" +
                "tableName='" + this.tableName + '\'' +
                ", columns=" + this.columns +
                '}';
    }

    public JsonNode getDefinedObjectSchema() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        result.put("name", this.tableName);
        ArrayNode fields = result.putArray("fields");
        for (RelDataTypeField col: this.columns) {
            ObjectNode column = fields.addObject();
            column.put("name", col.getName());
            column.put("type", col.getType().toString());
            column.put("nullable", col.getType().isNullable());
        }
        return result;
    }
}
