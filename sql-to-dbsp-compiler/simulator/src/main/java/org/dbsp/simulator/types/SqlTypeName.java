package org.dbsp.simulator.types;

public enum SqlTypeName {
    BOOLEAN("BOOLEAN"),
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    INTEGER("INTEGER"),
    BIGINT("BIGINT"),
    CHAR("CHAR"),
    VARCHAR("VARCHAR"),
    DECIMAL("DECIMAL"),
    REAL("REAL"),
    DOUBLE("DOUBLE"),
    DATE("DATE"),
    DATE_TZ("DATE WITH TIMEZONE"),
    TIME("TIME"),
    TIME_TZ("TIME WITH TIMEZONE"),
    TIMESTAMP("TIMESTAMP"),
    TIMESTAMP_TZ("TIMESTAMP WITH TIMEZONE"),
    SHORT_INTERVAL("INTERVAL"),
    LONG_INTERVAL("INTERVAL"),
    TUPLE("TUPLE"),
    ZSET("ZSET"),
    INDEXED_ZSET("INDEXED ZSET");

    public final String text;

    SqlTypeName(String text) {
        this.text = text;
    }
}
