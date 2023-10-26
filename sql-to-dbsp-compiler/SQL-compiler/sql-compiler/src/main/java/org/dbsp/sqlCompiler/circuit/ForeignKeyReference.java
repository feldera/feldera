package org.dbsp.sqlCompiler.circuit;

public class ForeignKeyReference {
    public final String tableName;
    public final String columnName;

    public ForeignKeyReference(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }
}
