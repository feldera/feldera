package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.util.Utilities;

import java.util.List;

/** Canonical representation of a foreign key from a table.
 * A table can have many foreign keys.
 * Each foreign key maps a set of columns from the table
 * to a set of columns from another table. */
public class ForeignKey {
    public ForeignKey(TableAndColumns thisTable, TableAndColumns otherTable) {
        this.thisTable = thisTable;
        this.otherTable = otherTable;
    }

    /** Represents a table and a list of columns. */
    public static class TableAndColumns {
        // Position of the whole list of columns in source code
        public final SqlParserPos listpos;
        // Kept as SqlIdentifier to preserve source position for validation
        public final SqlIdentifier tableName;
        public final List<SqlIdentifier> columns;

        TableAndColumns(SqlParserPos listPos, SqlIdentifier tableName, List<SqlIdentifier> columns) {
            this.listpos = listPos;
            this.tableName = tableName;
            this.columns = columns;
        }
    }

    public final TableAndColumns thisTable;
    public final TableAndColumns otherTable;

    public JsonNode asJson() {
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        ArrayNode columns = result.putArray("columns");
        for (SqlIdentifier column: this.thisTable.columns)
            columns.add(column.getSimple());
        result.put("refers", this.otherTable.tableName.toString());
        ArrayNode tocolumns = result.putArray("tocolumns");
        for (SqlIdentifier column: this.thisTable.columns)
            tocolumns.add(column.getSimple());
        return result;
    }
}
