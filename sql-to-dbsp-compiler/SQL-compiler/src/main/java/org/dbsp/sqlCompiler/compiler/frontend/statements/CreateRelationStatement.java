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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.JsonBuilder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for CreateTableStatement and CreateViewStatement.
 */
public abstract class CreateRelationStatement extends FrontEndStatement {
    public final String relationName;
    public final boolean nameIsQuoted;
    public final List<RelColumnMetadata> columns;

    protected CreateRelationStatement(SqlNode node, String statement, String relationName,
                                      boolean nameIsQuoted, @Nullable String comment,
                                      List<RelColumnMetadata> columns) {
        super(node, statement, comment);
        this.nameIsQuoted = nameIsQuoted;
        this.relationName = relationName;
        this.columns = columns;
    }

    public class EmulatedTable extends AbstractTable implements ScannableTable {
        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            // We don't plan to use this method, but the optimizer requires this API
            throw new UnsupportedException(CalciteObject.create(node));
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (RelColumnMetadata meta: CreateRelationStatement.this.columns)
                builder.add(meta.field);
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
        throw new InternalCompilerError("Column not found", CalciteObject.create(id));
    }

    public DBSPTypeTuple getRowTypeAsTuple(TypeCompiler compiler) {
        return this.getRowTypeAsStruct(compiler).toTuple();
    }

    public DBSPTypeStruct getRowTypeAsStruct(TypeCompiler compiler) {
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        for (RelColumnMetadata col: this.columns) {
            DBSPType fType = compiler.convertType(col.getType(), true);
            fields.add(new DBSPTypeStruct.Field(
                    this.getCalciteObject(), col.getName(), col.getName(), fType, col.nameIsQuoted));
        }
        return new DBSPTypeStruct(this.getCalciteObject(), this.relationName, this.relationName, fields);
    }

    @Override
    public String toString() {
        return "CreateRelationStatement{" +
                "tableName='" + this.relationName + '\'' +
                ", columns=" + this.columns +
                '}';
    }

    public JsonNode getDefinedObjectSchema() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        result.put("name", this.relationName);
        result.put("case_sensitive", this.nameIsQuoted);
        ArrayNode fields = result.putArray("fields");
        ArrayNode keyFields = mapper.createArrayNode();
        boolean hasKey = false;
        for (RelColumnMetadata col: this.columns) {
            ObjectNode column = fields.addObject();
            column.put("name", col.getName());
            column.put("case_sensitive", col.nameIsQuoted);
            if (col.isPrimaryKey) {
                keyFields.add(col.getName());
                hasKey = true;
            }
            Object object = RelJson.create().withJsonBuilder(new JsonBuilder())
                    .toJson(col.getType());
            try {
                // Is there a better way to do this?
                String json = mapper.writeValueAsString(object);
                JsonNode repr = mapper.readTree(json);
                column.set("columntype", repr);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        if (hasKey)
            result.set("primary_key", keyFields);
        return result;
    }
}
