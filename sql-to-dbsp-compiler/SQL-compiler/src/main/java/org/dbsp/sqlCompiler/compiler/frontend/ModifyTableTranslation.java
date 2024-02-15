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

package org.dbsp.sqlCompiler.compiler.frontend;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.TableModifyStatement;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Information used to translate INSERT or DELETE SQL statements
 */
class ModifyTableTranslation implements ICompilerComponent {
    /**
     * Result of the VALUES expression.
     */
    @Nullable
    private DBSPZSetLiteral valuesTranslation;
    /**
     * Maps each column index to the actual destination column index.
     * This handles SQL statements such as
     * INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104);
     * which specify explicitly the order of columns.
     */
    @Nullable
    private HashMap<Integer, Integer> columnPermutation;
    @Nullable
    DBSPTypeTuple resultType;
    final DBSPCompiler compiler;

    public ModifyTableTranslation(TableModifyStatement statement,
                                  CreateTableStatement tableDefinition,
                                  @Nullable SqlNodeList columnList,
                                  DBSPCompiler compiler) {
        this.valuesTranslation = null;
        this.compiler = compiler;
        this.columnPermutation = null;
        DBSPTypeTuple sourceType = tableDefinition.getRowTypeAsTuple(this.compiler.getTypeCompiler());
        if (columnList != null) {
            // The column list specifies an order for the columns that are assigned,
            // which may not be the order of the columns in the table.  We need to
            // compute a permutation.
            this.columnPermutation = new HashMap<>();
            int index = 0;
            DBSPType[] columnTypes = new DBSPType[columnList.size()];
            for (SqlNode node : columnList) {
                if (!(node instanceof SqlIdentifier)) {
                    throw new UnimplementedException(statement.getCalciteObject());
                }
                SqlIdentifier id = (SqlIdentifier) node;
                int actualIndex = tableDefinition.getColumnIndex(id);
                // This must be a permutation
                for (int value : this.columnPermutation.values())
                    if (value == actualIndex)
                        throw new InternalCompilerError("Not a column permutation " +
                                this.columnPermutation, statement.getCalciteObject());
                Utilities.putNew(this.columnPermutation, index, actualIndex);
                columnTypes[index] = sourceType.getFieldType(actualIndex);
                index++;
            }
            this.resultType = new DBSPTypeTuple(columnTypes);
        } else {
            this.resultType = sourceType;
        }
    }

    public DBSPZSetLiteral getTranslation() {
        return Objects.requireNonNull(this.valuesTranslation);
    }

    public DBSPTypeTuple getResultType() {
        return Objects.requireNonNull(this.resultType);
    }

    DBSPExpression permuteColumns(DBSPExpression expression) {
        assert this.columnPermutation != null;
        DBSPExpression[] columns = new DBSPExpression[this.columnPermutation.size()];
        DBSPTupleExpression tuple = expression.to(DBSPTupleExpression.class);
        for (Map.Entry<Integer, Integer> e : this.columnPermutation.entrySet()) {
            columns[e.getValue()] = tuple.get(e.getKey());
        }
        return new DBSPTupleExpression(columns);
    }

    DBSPZSetLiteral permuteColumns(DBSPZSetLiteral source) {
        assert this.resultType != null;
        if (this.columnPermutation == null)
            return source;
        DBSPZSetLiteral result = DBSPZSetLiteral.emptyWithElementType(this.resultType);
        for (Map.Entry<DBSPExpression, Long> e : source.data.entrySet()) {
            DBSPExpression perm = this.permuteColumns(e.getKey());
            result.add(perm, e.getValue());
        }
        return result;
    }

    public void setResult(DBSPZSetLiteral literal) {
        if (this.valuesTranslation != null)
            throw new InternalCompilerError("Overwriting logical value translation", CalciteObject.EMPTY);
        this.valuesTranslation = this.permuteColumns(literal);
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }
}
