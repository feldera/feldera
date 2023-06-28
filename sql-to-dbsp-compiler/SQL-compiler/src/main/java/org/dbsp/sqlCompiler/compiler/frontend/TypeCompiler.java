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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TypeCompiler implements ICompilerComponent {
    final DBSPCompiler compiler;

    public TypeCompiler(DBSPCompiler parent) {
        this.compiler = parent;
    }

    public static DBSPType makeZSet(DBSPType elementType, DBSPType weightType) {
        return new DBSPTypeZSet(elementType.getNode(), elementType, weightType);
    }

    public DBSPType convertType(RelDataType dt) {
        CalciteObject node = new CalciteObject(dt);
        boolean nullable = dt.isNullable();
        if (dt.isStruct()) {
            List<DBSPType> fields = new ArrayList<>();
            for (RelDataTypeField field: dt.getFieldList()) {
                DBSPType type = this.convertType(field.getType());
                fields.add(type);
            }
            return new DBSPTypeTuple(node, fields);
        } else {
            SqlTypeName tn = dt.getSqlTypeName();
            switch (tn) {
                case BOOLEAN:
                    return new DBSPTypeBool(node, nullable);
                case TINYINT:
                    return new DBSPTypeInteger(node, 8, true, nullable);
                case SMALLINT:
                    return new DBSPTypeInteger(node, 16, true, nullable);
                case INTEGER:
                    return new DBSPTypeInteger(node, 32, true, nullable);
                case BIGINT:
                    return new DBSPTypeInteger(node, 64, true, nullable);
                case DECIMAL: {
                    int precision = dt.getPrecision();
                    int scale = dt.getScale();
                    if (precision > DBSPTypeDecimal.MAX_PRECISION) {
                        // This would sure benefit from source position information, but we don't have any!
                        compiler.reportError(SourcePositionRange.INVALID, true, "Out of bounds",
                                "DECIMAL value precision " + precision +
                                        " is larger than the maximum supported precision; truncating to " +
                                        DBSPTypeDecimal.MAX_PRECISION
                                );
                        precision = DBSPTypeDecimal.MAX_PRECISION;
                    }
                    if (scale > DBSPTypeDecimal.MAX_SCALE) {
                        compiler.reportError(SourcePositionRange.INVALID, true, "Out of bounds",
                                "DECIMAL value scale " + scale +
                                        " is larger than the maximum supported scale; truncating to " +
                                        DBSPTypeDecimal.MAX_SCALE
                        );
                        scale = DBSPTypeDecimal.MAX_SCALE;
                    }
                    return new DBSPTypeDecimal(node, precision, scale, nullable);
                }
                case FLOAT:
                case REAL:
                    return DBSPTypeFloat.INSTANCE.setMayBeNull(nullable);
                case DOUBLE:
                    return DBSPTypeDouble.INSTANCE.setMayBeNull(nullable);
                case CHAR:
                case VARCHAR: {
                    int precision = dt.getPrecision();
                    if (precision == 0)
                        throw new UnsupportedException("0 size string", node);
                    if (precision == RelDataType.PRECISION_NOT_SPECIFIED)
                        precision = 0;
                    return new DBSPTypeString(node, precision, tn.equals(SqlTypeName.CHAR), nullable);
                }
                case NULL:
                    return DBSPTypeNull.INSTANCE;
                case SYMBOL:
                    return DBSPTypeKeyword.INSTANCE;
                case ARRAY: {
                    RelDataType ct = Objects.requireNonNull(dt.getComponentType());
                    DBSPType elementType = this.convertType(ct);
                    return new DBSPTypeVec(elementType);
                }
                case BINARY:
                case VARBINARY:
                case UNKNOWN:
                case ANY:
                case MULTISET:
                case MAP:
                case DISTINCT:
                case STRUCTURED:
                case ROW:
                case OTHER:
                case CURSOR:
                case COLUMN_LIST:
                case DYNAMIC_STAR:
                case SARG:
                case TIME:
                case TIME_WITH_LOCAL_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    throw new UnimplementedException(node);
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                    return new DBSPTypeMonthsInterval(node, nullable);
                case INTERVAL_DAY:
                case INTERVAL_DAY_HOUR:
                case INTERVAL_DAY_MINUTE:
                case INTERVAL_DAY_SECOND:
                case INTERVAL_HOUR:
                case INTERVAL_HOUR_MINUTE:
                case INTERVAL_HOUR_SECOND:
                case INTERVAL_MINUTE:
                case INTERVAL_MINUTE_SECOND:
                case INTERVAL_SECOND:
                    return new DBSPTypeMillisInterval(node, nullable);
                case GEOMETRY:
                    return DBSPTypeGeoPoint.INSTANCE.setMayBeNull(nullable);
                case TIMESTAMP:
                    return DBSPTypeTimestamp.INSTANCE.setMayBeNull(nullable);
                case DATE:
                    return DBSPTypeDate.INSTANCE.setMayBeNull(nullable);
            }
        }
        throw new UnimplementedException(node);
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }
}
