package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVariantNullLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeStr;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeReal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestampTz;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUuid;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeGeoPoint;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;

/** Preserve connections to the built-in types that later passes may reuse.
 * These checks never allocate types and are used only by the internal seam writer. */
final class ReplayTypes {
    private ReplayTypes() { }

    static boolean isSharedNode(IDBSPNode node) {
        return (node instanceof DBSPType type && isSingleton(type))
                || node instanceof DBSPVariantNullLiteral;
    }

    static boolean isSingleton(DBSPType type) {
        if (type instanceof DBSPTypeRawTuple)
            return type == DBSPTypeRawTuple.EMPTY;
        if (type instanceof DBSPTypeAny || type instanceof DBSPTypeNull
                || type instanceof DBSPTypeStr || type instanceof DBSPTypeVoid)
            return true;
        if (type instanceof DBSPTypeBool)
            return type == DBSPTypeBool.create(type.mayBeNull);
        if (type instanceof DBSPTypeDate)
            return type == DBSPTypeDate.create(type.mayBeNull);
        if (type instanceof DBSPTypeDouble)
            return type == DBSPTypeDouble.create(type.mayBeNull);
        if (type instanceof DBSPTypeReal)
            return type == DBSPTypeReal.create(type.mayBeNull);
        if (type instanceof DBSPTypeTime)
            return type == DBSPTypeTime.create(type.mayBeNull);
        if (type instanceof DBSPTypeTimestamp)
            return type == DBSPTypeTimestamp.create(type.mayBeNull);
        if (type instanceof DBSPTypeTimestampTz)
            return type == DBSPTypeTimestampTz.create(type.mayBeNull);
        if (type instanceof DBSPTypeUuid)
            return type == DBSPTypeUuid.create(type.mayBeNull);
        if (type instanceof DBSPTypeVariant)
            return type == DBSPTypeVariant.create(type.mayBeNull);
        if (type instanceof DBSPTypeGeoPoint)
            return type == DBSPTypeGeoPoint.create(CalciteObject.EMPTY, type.mayBeNull);
        if (type instanceof DBSPTypeUSize)
            return type == DBSPTypeUSize.create(type.mayBeNull);
        if (type instanceof DBSPTypeString)
            return type == DBSPTypeString.varchar(type.mayBeNull);
        return false;
    }
}
