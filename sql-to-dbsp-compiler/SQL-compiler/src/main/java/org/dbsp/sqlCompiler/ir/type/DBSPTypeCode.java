package org.dbsp.sqlCompiler.ir.type;

public enum DBSPTypeCode {
    ANY("_", "_"),
    BOOL("b", "bool"),
    BYTES("bytes", "ByteArray"),
    DATE("Date", "Date"),
    DATE_TZ("", ""),
    DECIMAL("decimal", "SQLDecimal"),
    DOUBLE("d", "F64"),
    REAL("f", "F32"),
    GEOPOINT("geopoint", "GeoPoint"),
    INT8("i8", "i8"),
    INT16("i16", "i16"),
    INT32("i32", "i32"),
    INT64("i64", "i64"),
    INTERVAL_SHORT("ShortInterval", "ShortInterval"),
    INTERVAL_LONG("LongInterval", "LongInterval"),
    ISIZE("i", "isize"),
    KEYWORD("", ""),
    NULL("null", "()"),
    STR("str", "str"),
    STRING("s", "String"),
    TIME("Time", "Time"),
    TIMESTAMP("Timestamp", "Timestamp"),
    TIMESTAMP_TZ("", ""),
    UINT16("u16", "u16"),
    UINT32("u32", "u32"),
    UINT64("u64", "u64"),
    USIZE("u", "usize"),
    VOID("void", ""),
    WEIGHT("Weight", "Weight"),
    // Derived types
    FUNCTION("", ""),
    INDEXED_ZSET("", ""),
    RAW_TUPLE("", ""),
    REF("", ""),
    RESULT("", "Result"),
    SEMIGROUP("", ""),
    STREAM("", ""),
    STRUCT("", ""),
    TUPLE("", ""),
    USER("", ""),
    VEC("", ""),
    ZSET("", "");

    public final String shortName;
    public final String rustName;

    DBSPTypeCode(String shortName, String rustName) {
        this.shortName = shortName;
        this.rustName = rustName;
    }

    @Override
    public String toString() {
        return this.shortName;
    }
}
