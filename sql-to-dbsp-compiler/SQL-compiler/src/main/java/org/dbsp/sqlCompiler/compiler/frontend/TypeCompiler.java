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
import org.dbsp.sqlCompiler.compiler.ViewColumnMetadata;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelStruct;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.IsDateType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeFP;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUuid;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.primitive.IHasPrecision;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
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
import java.util.Set;

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

    /**
     * Given operands for an operation that requires identical types on left and right,
     * compute the type that both operands must be cast to.
     * @param node       Compilation context.
     * @param left       Left operand type.
     * @param right      Right operand type.
     * @param error      Extra message to show in case of error.
     * @param acceptStrings  If true accept string types as left and/or right.
     * @return           Common type operands must be cast to.
     */
    public static DBSPType reduceType(CalciteObject node, DBSPType left, DBSPType right,
                                      String error, boolean acceptStrings) {
        if (left.code == DBSPTypeCode.NULL)
            return right.withMayBeNull(true);
        if (right.code == DBSPTypeCode.NULL)
            return left.withMayBeNull(true);
        boolean anyNull = left.mayBeNull || right.mayBeNull;
        if (left.sameTypeIgnoringNullability(right))
            return left.withMayBeNull(anyNull);

        if (left.is(DBSPTypeTimestamp.class) && right.is(IsDateType.class))
            return left.withMayBeNull(anyNull);
        if (right.is(DBSPTypeTimestamp.class) && left.is(IsDateType.class))
            return right.withMayBeNull(anyNull);

        if (left.is(DBSPTypeTupleBase.class)) {
            if (!right.is(DBSPTypeTupleBase.class))
                throw new CompilationError(error + "Implicit conversion between " +
                        left.asSqlString() + " and " + right.asSqlString() + " not supported", node);
            DBSPTypeTupleBase lTuple = left.to(DBSPTypeTupleBase.class);
            DBSPTypeTupleBase rTuple = right.to(DBSPTypeTupleBase.class);
            if (lTuple.size() != rTuple.size() || lTuple.isRaw() != rTuple.isRaw())
                throw new CompilationError(error + "Implicit conversion between " +
                        left.asSqlString() + " and " + right.asSqlString() + " not supported", node);
            List<DBSPType> fields = new ArrayList<>(lTuple.size());
            for (int i = 0; i < lTuple.size(); i++)
                fields.add(reduceType(node, lTuple.getFieldType(i), rTuple.getFieldType(i),
                        error, acceptStrings));
            return lTuple.makeRelatedTupleType(fields);
        }

        if (acceptStrings) {
            if (left.code == DBSPTypeCode.STRING && right.code == DBSPTypeCode.STRING) {
                DBSPTypeString ls = left.to(DBSPTypeString.class);
                DBSPTypeString rs = right.to(DBSPTypeString.class);
                boolean fixed = ls.fixed && rs.fixed;
                int precision = Math.max(ls.precision, rs.precision);
                if (ls.precision == IHasPrecision.UNLIMITED_PRECISION ||
                        rs.precision == IHasPrecision.UNLIMITED_PRECISION)
                    precision = IHasPrecision.UNLIMITED_PRECISION;
                return DBSPTypeString.create(left.getNode(), precision, fixed, anyNull);
            }
        }

        IsNumericType ln = left.as(IsNumericType.class);
        IsNumericType rn = right.as(IsNumericType.class);

        DBSPTypeInteger li = left.as(DBSPTypeInteger.class);
        DBSPTypeInteger ri = right.as(DBSPTypeInteger.class);
        DBSPTypeDecimal ld = left.as(DBSPTypeDecimal.class);
        DBSPTypeDecimal rd = right.as(DBSPTypeDecimal.class);
        DBSPTypeFP lf = left.as(DBSPTypeFP.class);
        DBSPTypeFP rf = right.as(DBSPTypeFP.class);
        if (ln == null || rn == null) {
            throw new CompilationError(error + "Implicit conversion between " +
                    left.asSqlString() + " and " + right.asSqlString() + " not supported", node);
        }
        if (li != null) {
            if (ri != null) {
                // INT op INT, choose the wider int type
                int width = Math.max(li.getWidth(), ri.getWidth());
                boolean signed;
                if (li.getWidth() > ri.getWidth())
                    signed = li.signed;
                else if (li.getWidth() == ri.getWidth())
                    signed = li.signed && ri.signed; // signed if both signed
                else
                    signed = ri.signed;
                return new DBSPTypeInteger(left.getNode(), width, signed, anyNull);
            }
            if (rf != null) {
                // Calcite uses the float always
                return rf.withMayBeNull(anyNull);
            }
            if (rd != null) {
                // INT op DECIMAL
                // widen the DECIMAL type enough to hold the left type
                return new DBSPTypeDecimal(rd.getNode(),
                        Math.max(ln.getPrecision(), rn.getPrecision()), rd.scale, anyNull);
            }
        }
        if (lf != null) {
            if (ri != null) {
                // FLOAT op INT, Calcite uses the float always
                return lf.withMayBeNull(anyNull);
            }
            if (rf != null) {
                // FLOAT op FLOAT, choose widest
                if (ln.getPrecision() < rn.getPrecision())
                    return right.withMayBeNull(anyNull);
                else
                    return left.withMayBeNull(anyNull);
            }
            if (rd != null) {
                // FLOAT op DECIMAL, convert to DOUBLE
                return new DBSPTypeDouble(left.getNode(), anyNull);
            }
        }
        if (ld != null) {
            if (ri != null) {
                // DECIMAL op INTEGER, make a decimal wide enough to hold result
                return new DBSPTypeDecimal(right.getNode(),
                        Math.max(ln.getPrecision(), rn.getPrecision()), ld.scale, anyNull);
            }
            if (rf != null) {
                // DECIMAL op FLOAT, convert to DOUBLE
                return new DBSPTypeDouble(right.getNode(), anyNull);
            }

            if (rd != null) {
                int scale = Math.max(ld.scale, rd.scale);
                int before = Math.max(ld.precision - ld.scale, rd.precision - rd.scale);
                int precision = before + scale;
                if (precision < DBSPTypeDecimal.MAX_PRECISION) {
                    return new DBSPTypeDecimal(ld.getNode(), precision, scale, anyNull);
                } else {
                    throw new CompilationError(error + "Could not derive a common type between " +
                            left.asSqlString() + " and " + right.asSqlString() + "; please use explicit casts", node);
                }
            }
        }
        throw new UnimplementedException("Cast from " + right + " to " + left, left.getNode());
    }

    public DBSPType convertType(
            CalciteObject node, ProgramIdentifier name,
            List<RelColumnMetadata> columns, boolean asStruct, boolean mayBeNull) {
        SourcePositionRange range = node.getPositionRange();
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        int index = 0;
        for (RelColumnMetadata col : columns) {
            DBSPType fType = this.convertType(range, col.getType(), true);
            fields.add(new DBSPTypeStruct.Field(col.node, col.getName(), index++, fType));
        }
        String saneName = this.compiler.generateStructName(name, fields);
        DBSPTypeStruct struct = new DBSPTypeStruct(node, name, saneName, fields, mayBeNull);
        if (asStruct) {
            return struct;
        } else {
            List<DBSPType> typeFields = new ArrayList<>();
            for (RelColumnMetadata col : columns) {
                DBSPType fType = this.convertType(range, col.getType(), false);
                typeFields.add(fType);
            }
            return new DBSPTypeTuple(node, mayBeNull, struct, typeFields);
        }
    }

    public static DBSPTypeStruct asStruct(
            DBSPCompiler compiler,
            CalciteObject node, ProgramIdentifier name,
            List<ViewColumnMetadata> columns, boolean mayBeNull) {
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        int index = 0;
        for (ViewColumnMetadata col : columns) {
            fields.add(new DBSPTypeStruct.Field(col.node, col.getName(), index++, col.getType()));
        }
        String saneName = compiler.generateStructName(name, fields);
        return new DBSPTypeStruct(node, name, saneName, fields, mayBeNull);
    }

    final Set<Integer> timePrecisionsWarned = new HashSet<>();
    final Set<Integer> timestampPrecisionsWarned = new HashSet<>();

    /**
     * Convert a Calcite RelDataType to an equivalent DBSP type.
     * @param dt         Data type to convert.
     * @param asStruct   If true convert a Struct type to a DBSPTypeStruct, otherwise
     *                   convert it to a DBSPTypeTuple.
     */
    public DBSPType convertType(SourcePositionRange context, RelDataType dt, boolean asStruct) {
        CalciteObject node = CalciteObject.create(dt);
        boolean nullable = dt.isNullable();
        DBSPTypeStruct struct;
        if (dt.isStruct()) {
            boolean isNamedStruct = dt instanceof RelStruct;
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
                    DBSPType type = this.convertType(context, field.getType(), true);
                    String fieldName = field.getName();
                    if (this.compiler().options.languageOptions.lenient)
                        // If we are not lenient and names are duplicated
                        // we will get an exception below where we create the struct.
                        fieldName = fieldNameGen.freshName(fieldName, true);
                    fields.add(new DBSPTypeStruct.Field(
                            CalciteObject.create(dt), new ProgramIdentifier(fieldName), index++, type));
                }
                String saneName = this.compiler.generateStructName(new ProgramIdentifier("*", false), fields);
                struct = new DBSPTypeStruct(node, new ProgramIdentifier(saneName, false), saneName, fields, nullable);
            }
            if (asStruct) {
                return struct.withMayBeNull(dt.isNullable());
            } else {
                DBSPType[] fieldTypes = new DBSPType[dt.getFieldCount()];
                int i = 0;
                for (RelDataTypeField field : dt.getFieldList()) {
                    DBSPType type = this.convertType(context, field.getType(), asStruct);
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
                    return DBSPTypeInteger.getType(node, DBSPTypeCode.INT8, nullable);
                case SMALLINT:
                    return DBSPTypeInteger.getType(node, DBSPTypeCode.INT16, nullable);
                case INTEGER:
                    return DBSPTypeInteger.getType(node, DBSPTypeCode.INT32, nullable);
                case BIGINT:
                    return DBSPTypeInteger.getType(node, DBSPTypeCode.INT64, nullable);
                case UTINYINT:
                    return DBSPTypeInteger.getType(node, DBSPTypeCode.UINT8, nullable);
                case USMALLINT:
                    return DBSPTypeInteger.getType(node, DBSPTypeCode.UINT16, nullable);
                case UINTEGER:
                    return DBSPTypeInteger.getType(node, DBSPTypeCode.UINT32, nullable);
                case UBIGINT:
                    return DBSPTypeInteger.getType(node, DBSPTypeCode.UINT64, nullable);
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
                    return DBSPTypeReal.create(nullable);
                case FLOAT:
                case DOUBLE:
                    return DBSPTypeDouble.create(nullable);
                case CHAR:
                case VARCHAR: {
                    int precision = dt.getPrecision();
                    if (precision == RelDataType.PRECISION_NOT_SPECIFIED)
                        //noinspection ReassignedVariable,DataFlowIssue
                        precision = DBSPTypeString.UNLIMITED_PRECISION;
                    return DBSPTypeString.create(node, precision, tn.equals(SqlTypeName.CHAR), nullable);
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
                    return DBSPTypeNull.INSTANCE;
                case SYMBOL:
                    return DBSPTypeKeyword.INSTANCE;
                case ARRAY: {
                    RelDataType ct = Objects.requireNonNull(dt.getComponentType());
                    DBSPType elementType = this.convertType(context, ct, asStruct);
                    return new DBSPTypeArray(elementType, dt.isNullable());
                }
                case UNKNOWN:
                case ANY:
                    // Not sure whether this is right
                    return DBSPTypeAny.getDefault();
                case MAP: {
                    RelDataType kt = Objects.requireNonNull(dt.getKeyType());
                    DBSPType keyType = this.convertType(context, kt, asStruct);
                    RelDataType vt = Objects.requireNonNull(dt.getValueType());
                    DBSPType valueType = this.convertType(context, vt, asStruct);
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
                    return DBSPTypeGeoPoint.create(node, nullable);
                case TIMESTAMP:
                    if (dt.getPrecision() != DBSPTypeTimestamp.PRECISION &&
                            !timestampPrecisionsWarned.contains(dt.getPrecision())) {
                        this.compiler.reportWarning(context, "TIMESTAMP precision ignored",
                                "TIMESTAMP precision is always TIMESTAMP(" + DBSPTypeTimestamp.PRECISION +
                                        "); specified precision " + dt.getPrecision() + " is ignored");
                        timestampPrecisionsWarned.add(dt.getPrecision());
                    }
                    return DBSPTypeTimestamp.create(node, nullable);
                case DATE:
                    return new DBSPTypeDate(node, nullable);
                case TIME:
                    if (dt.getPrecision() != DBSPTypeTime.PRECISION &&
                            !timePrecisionsWarned.contains(dt.getPrecision())) {
                        this.compiler.reportWarning(context, "TIME precision ignored",
                                "TIME precision is always TIME(" + DBSPTypeTime.PRECISION +
                                        "); specified precision " + dt.getPrecision() + " is ignored");
                        timePrecisionsWarned.add(dt.getPrecision());
                    }
                    return new DBSPTypeTime(node, nullable);
                case UUID:
                    return new DBSPTypeUuid(node, nullable);
                case VARIANT:
                    return DBSPTypeVariant.create(node, nullable);
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
