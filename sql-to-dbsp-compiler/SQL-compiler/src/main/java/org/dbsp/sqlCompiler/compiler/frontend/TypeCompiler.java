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
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelStruct;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBinary;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeGeoPoint;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeKeyword;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMonthsInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeReal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.util.FreshName;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class TypeCompiler implements ICompilerComponent {
    final DBSPCompiler compiler;

    public TypeCompiler(DBSPCompiler parent) {
        this.compiler = parent;
    }

    public static DBSPTypeZSet makeZSet(DBSPType elementType) {
        return new DBSPTypeZSet(elementType.getNode(), elementType);
    }

    public static DBSPTypeIndexedZSet makeIndexedZSet(
            DBSPType keyType, DBSPType elementType) {
        return new DBSPTypeIndexedZSet(elementType.getNode(), keyType, elementType);
    }

    public static DBSPTypeIndexedZSet makeIndexedZSet(DBSPTypeRawTuple kvtype) {
        assert kvtype.size() == 2;
        return new DBSPTypeIndexedZSet(kvtype.getNode(), kvtype.tupFields[0], kvtype.tupFields[1]);
    }

    public DBSPType convertType(
            CalciteObject node, ProgramIdentifier name,
            List<RelColumnMetadata> columns, boolean asStruct, boolean mayBeNull) {
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        int index = 0;
        for (RelColumnMetadata col : columns) {
            DBSPType fType = this.convertType(col.getType(), true);
            fields.add(new DBSPTypeStruct.Field(col.node, col.getName(), index++, fType));
        }
        String saneName = this.compiler.getSaneStructName(name);
        DBSPTypeStruct struct = new DBSPTypeStruct(node, name, saneName, fields, mayBeNull);
        if (asStruct) {
            return struct;
        } else {
            List<DBSPType> typeFields = new ArrayList<>();
            for (RelColumnMetadata col : columns) {
                DBSPType fType = this.convertType(col.getType(), false);
                typeFields.add(fType);
            }
            return new DBSPTypeTuple(node, mayBeNull, struct, typeFields);
        }
    }

    /**
     * Convert a Calcite RelDataType to an equivalent DBSP type.
     * @param dt         Data type to convert.
     * @param asStruct   If true convert a Struct type to a DBSPTypeStruct, otherwise
     *                   convert it to a DBSPTypeTuple.
     */
    public DBSPType convertType(RelDataType dt, boolean asStruct) {
        CalciteObject node = CalciteObject.create(dt);
        boolean nullable = dt.isNullable();
        DBSPTypeStruct struct;
        if (dt.isStruct()) {
            boolean isNamedStruct = dt instanceof RelStruct;
            String saneName = this.compiler.getSaneStructName(new ProgramIdentifier("*", false));
            if (isNamedStruct) {
                RelStruct rs = (RelStruct) dt;
                ProgramIdentifier simpleName = Utilities.toIdentifier(rs.typeName);
                // Struct must be already declared
                struct = Objects.requireNonNull(this.compiler.getStructByName(simpleName));
            } else {
                List<DBSPTypeStruct.Field> fields = new ArrayList<>();
                FreshName fieldNameGen = new FreshName(new HashSet<>());
                int index = 0;
                for (RelDataTypeField field : dt.getFieldList()) {
                    DBSPType type = this.convertType(field.getType(), true);
                    String fieldName = field.getName();
                    if (this.compiler().options.languageOptions.lenient)
                        // If we are not lenient and names are duplicated
                        // we will get an exception below where we create the struct.
                        fieldName = fieldNameGen.freshName(fieldName, true);
                    fields.add(new DBSPTypeStruct.Field(
                            CalciteObject.create(dt), new ProgramIdentifier(fieldName, false), index++, type));
                }
                struct = new DBSPTypeStruct(node, new ProgramIdentifier(saneName, false), saneName, fields, nullable);
            }
            if (asStruct) {
                return struct;
            } else {
                DBSPType[] fieldTypes = new DBSPType[dt.getFieldCount()];
                int i = 0;
                for (RelDataTypeField field : dt.getFieldList()) {
                    DBSPType type = this.convertType(field.getType(), asStruct);
                    fieldTypes[i++] = type;
                }
                return new DBSPTypeTuple(node, nullable, struct, fieldTypes);
            }
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
                        compiler.reportWarning(SourcePositionRange.INVALID, "Out of bounds",
                                "DECIMAL value precision " + precision +
                                        " is larger than the maximum supported precision; truncating to " +
                                        DBSPTypeDecimal.MAX_PRECISION
                                );
                        precision = DBSPTypeDecimal.MAX_PRECISION;
                    }
                    if (scale > DBSPTypeDecimal.MAX_SCALE) {
                        compiler.reportWarning(SourcePositionRange.INVALID, "Out of bounds",
                                "DECIMAL value scale " + scale +
                                        " is larger than the maximum supported scale; truncating to " +
                                        DBSPTypeDecimal.MAX_SCALE
                        );
                        scale = DBSPTypeDecimal.MAX_SCALE;
                    }
                    return new DBSPTypeDecimal(node, precision, scale, nullable);
                }
                case REAL:
                    return new DBSPTypeReal(CalciteObject.EMPTY, nullable);
                case FLOAT:
                case DOUBLE:
                    return new DBSPTypeDouble(CalciteObject.EMPTY, nullable);
                case CHAR:
                case VARCHAR: {
                    int precision = dt.getPrecision();
                    if (precision == RelDataType.PRECISION_NOT_SPECIFIED)
                        //noinspection ReassignedVariable,DataFlowIssue
                        precision = DBSPTypeString.UNLIMITED_PRECISION;
                    return new DBSPTypeString(node, precision, tn.equals(SqlTypeName.CHAR), nullable);
                }
                case VARBINARY:
                case BINARY: {
                    int precision = dt.getPrecision();
                    if (precision == RelDataType.PRECISION_NOT_SPECIFIED)
                        //noinspection ReassignedVariable,DataFlowIssue
                        precision = DBSPTypeBinary.UNLIMITED_PRECISION;
                    return new DBSPTypeBinary(node, precision, nullable);
                }
                case NULL:
                    return new DBSPTypeNull(CalciteObject.EMPTY);
                case SYMBOL:
                    return new DBSPTypeKeyword();
                case ARRAY: {
                    RelDataType ct = Objects.requireNonNull(dt.getComponentType());
                    DBSPType elementType = this.convertType(ct, asStruct);
                    return new DBSPTypeVec(elementType, dt.isNullable());
                }
                case UNKNOWN:
                case ANY:
                    // Not sure whether this is right
                    return DBSPTypeAny.getDefault();
                case MAP: {
                    RelDataType kt = Objects.requireNonNull(dt.getKeyType());
                    DBSPType keyType = this.convertType(kt, asStruct);
                    RelDataType vt = Objects.requireNonNull(dt.getValueType());
                    DBSPType valueType = this.convertType(vt, asStruct);
                    return new DBSPTypeMap(keyType, valueType, dt.isNullable());
                }
                case MULTISET:
                case DISTINCT:
                case STRUCTURED:
                case ROW:
                case OTHER:
                case CURSOR:
                case DYNAMIC_STAR:
                case SARG:
                case TIME_WITH_LOCAL_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    throw new UnimplementedException("Support for SQL type " + Utilities.singleQuote(tn.getName())
                            + " not yet implemented", node);
                case INTERVAL_YEAR:
                    return new DBSPTypeMonthsInterval(node, DBSPTypeMonthsInterval.Units.YEARS, nullable);
                case INTERVAL_YEAR_MONTH:
                    return new DBSPTypeMonthsInterval(node, DBSPTypeMonthsInterval.Units.YEARS_TO_MONTHS, nullable);
                case INTERVAL_MONTH:
                    return new DBSPTypeMonthsInterval(node, DBSPTypeMonthsInterval.Units.MONTHS, nullable);
                case INTERVAL_DAY:
                    return new DBSPTypeMillisInterval(node, DBSPTypeMillisInterval.Units.DAYS, nullable);
                case INTERVAL_DAY_HOUR:
                    return new DBSPTypeMillisInterval(node, DBSPTypeMillisInterval.Units.DAYS_TO_HOURS, nullable);
                case INTERVAL_DAY_MINUTE:
                    return new DBSPTypeMillisInterval(node, DBSPTypeMillisInterval.Units.DAYS_TO_MINUTES, nullable);
                case INTERVAL_DAY_SECOND:
                    return new DBSPTypeMillisInterval(node, DBSPTypeMillisInterval.Units.DAYS_TO_SECONDS, nullable);
                case INTERVAL_HOUR:
                    return new DBSPTypeMillisInterval(node, DBSPTypeMillisInterval.Units.HOURS, nullable);
                case INTERVAL_HOUR_MINUTE:
                    return new DBSPTypeMillisInterval(node, DBSPTypeMillisInterval.Units.HOURS_TO_MINUTES, nullable);
                case INTERVAL_HOUR_SECOND:
                    return new DBSPTypeMillisInterval(node, DBSPTypeMillisInterval.Units.HOURS_TO_SECONDS, nullable);
                case INTERVAL_MINUTE:
                    return new DBSPTypeMillisInterval(node, DBSPTypeMillisInterval.Units.MINUTES, nullable);
                case INTERVAL_MINUTE_SECOND:
                    return new DBSPTypeMillisInterval(node, DBSPTypeMillisInterval.Units.MINUTES_TO_SECONDS, nullable);
                case INTERVAL_SECOND:
                    return new DBSPTypeMillisInterval(node, DBSPTypeMillisInterval.Units.SECONDS, nullable);
                case GEOMETRY:
                    return new DBSPTypeGeoPoint(node, nullable);
                case TIMESTAMP:
                    return new DBSPTypeTimestamp(node, nullable);
                case DATE:
                    return new DBSPTypeDate(node, nullable);
                case TIME:
                    return new DBSPTypeTime(node, nullable);
                case VARIANT:
                    return new DBSPTypeVariant(node, nullable);
                default:
                    break;
            }
        }
        throw new UnimplementedException("Support for SQL type " + Utilities.singleQuote(dt.getFullTypeString())
                + " not yet implemented", node);
    }

    @Override
    public DBSPCompiler compiler() {
        return this.compiler;
    }
}
