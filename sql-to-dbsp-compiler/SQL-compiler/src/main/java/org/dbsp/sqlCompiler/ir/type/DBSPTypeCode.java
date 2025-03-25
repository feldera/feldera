package org.dbsp.sqlCompiler.ir.type;

import javax.annotation.Nullable;

public enum DBSPTypeCode {
    ANY("ANY", "_", "_"),
    BOOL("BOOL", "b", "bool"),
    BYTES("BINARY", "bytes", "ByteArray"),
    DATE("DATE", "Date", "Date"),
    DECIMAL("DECIMAL", "decimal", "Decimal"),
    DECIMAL_RUNTIME("DECIMAL", "decimal", "Decimal"),
    DOUBLE("DOUBLE", "d", "F64"),
    REAL("REAL", "f", "F32"),
    GEOPOINT("GEOPOINT", "geopoint", "GeoPoint"),
    INT8("TINYINT", "i8", "i8"),
    INT16("SHORTINT", "i16", "i16"),
    INT32("INT", "i32", "i32"),
    INT64("BIGINT", "i64", "i64"),
    INT128(null, "i128", "i128"),
    INTERVAL_SHORT("INTERVAL SECONDS", "ShortInterval", "ShortInterval"),
    INTERVAL_LONG("INTERVAL MONTHS", "LongInterval", "LongInterval"),
    ISIZE(null, "i", "isize"),
    KEYWORD(null, "", ""),
    NULL("NULL", "null", "()"),
    STR(null, "str", "str"),
    STRING("VARCHAR", "s", "SqlString"),
    TIME("TIME", "Time", "Time"),
    TIMESTAMP("TIMESTAMP", "Timestamp", "Timestamp"),
    TIMESTAMP_TZ("TIMESTAMP WITH TIME ZONE", "", ""),
    UINT16(null, "u16", "u16"),
    UINT32(null, "u32", "u32"),
    UINT64(null, "u64", "u64"),
    UINT128(null, "u128", "u128"),
    USIZE(null, "u", "usize"),
    UUID("UUID", "Uuid", "Uuid"),
    VOID(null, "void", "()"),
    WEIGHT(null, "Weight", "Weight"),
    // Derived types
    FUNCTION("FUNCTION", "", ""),
    INDEXED_ZSET(null, "", ""),
    RAW_TUPLE("STRUCT", "", ""),
    REF(null, "", ""),
    STRUCT("STRUCT", "", ""),
    TUPLE("STRUCT", "", "Tup"),
    // User-defined types
    COMPARATOR(null, "", ""),
    RESULT(null, "", "Result"),
    OPTION(null, "", "Option"),
    TYPEDBOX(null, "", "TypedBox"),
    SEMIGROUP(null, "", ""),
    STREAM(null, "", ""),
    USER(null, "", ""),
    ARRAY("ARRAY", "", "Array"),
    MAP("MAP", "", "Map"),
    VARIANT("VARIANT", "V", "Variant"),
    ZSET("MULTISET", "", ""),
    // Abstract type, used in some dataflow analysis.
    ABSTRACT(null, "", "");

    @Nullable
    public final String sqlName;
    public final String shortName;
    public final String rustName;

    DBSPTypeCode(@Nullable String sqlName, String shortName, String rustName) {
        this.sqlName = sqlName;
        this.shortName = shortName;
        this.rustName = rustName;
    }

    @Override
    public String toString() {
        return this.shortName;
    }
}
