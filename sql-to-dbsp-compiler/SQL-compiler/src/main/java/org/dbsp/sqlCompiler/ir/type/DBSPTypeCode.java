package org.dbsp.sqlCompiler.ir.type;

public enum DBSPTypeCode {
    ANY("_", "_", ""),
    BOOL("b", "bool", "Bool"),
    BYTES("byte[]", "ByteArray", ""),
    DATE("Date", "Date", "Date"),
    DATE_TZ("", "", ""),
    DECIMAL("decimal", "Decimal", "Decimal"),
    DOUBLE("d", "F64", "F64"),
    REAL("f", "F32", "F32"),
    GEOPOINT("geopoint", "GeoPoint", ""),
    INT8("i8", "i8", ""),
    INT16("i16", "i16", "I16"),
    INT32("i32", "i32", "I32"),
    INT64("i64", "i64", "I64"),
    INTERVAL_SHORT("ShortInterval", "ShortInterval", ""),
    INTERVAL_LONG("LongInterval", "LongInterval", ""),
    ISIZE("i", "isize", "Isize"),
    KEYWORD("", "", ""),
    NULL("null", "()", ""),
    STR("str", "str", ""),
    STRING("s", "String", "String"),
    TIME("Time", "Time", "Time"),
    TIMESTAMP("Timestamp", "Timestamp", "Timestamp"),
    TIMESTAMP_TZ("", "", ""),
    UNIT("", "()", "Unit"),  // JIT use only
    UINT16("u16", "u16", ""),
    UINT32("u32", "u32", "U32"),
    UINT64("u64", "u64", ""),
    USIZE("u", "usize", "Usize"),
    VOID("void", "", ""),
    WEIGHT("Weight", "Weight", ""),
    // Derived types
    KV("", "", ""), // JIT use only
    FUNCTION("", "", ""),
    INDEXED_ZSET("", "", ""),
    RAW_TUPLE("", "", ""),
    REF("", "", ""),
    SEMIGROUP("", "", ""),
    STREAM("", "", ""),
    STRUCT("", "", ""),
    TUPLE("", "", ""),
    USER("", "", ""),
    VEC("", "", ""),
    ZSET("", "", "");

    public final String shortName;
    public final String rustName;
    public final String jitName;

    DBSPTypeCode(String shortName, String rustName, String jitName) {
        this.shortName = shortName;
        this.rustName = rustName;
        this.jitName = jitName;
    }

    @Override
    public String toString() {
        return this.shortName;
    }
}
