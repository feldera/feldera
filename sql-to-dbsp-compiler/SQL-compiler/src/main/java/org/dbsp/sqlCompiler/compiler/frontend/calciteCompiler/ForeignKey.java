package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragment;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragmentIdentifier;
import org.dbsp.util.IJson;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.List;

/** Canonical representation of a foreign key from a table.
 * A table can have many foreign keys.
 * Each foreign key maps a set of columns from the table
 * to a set of columns from another table. */
public class ForeignKey implements IJson {
    public ForeignKey(TableAndColumns thisTable, TableAndColumns otherTable) {
        this.thisTable = thisTable;
        this.otherTable = otherTable;
    }

    /** Represents a table and a list of columns. */
    public static class TableAndColumns implements IJson {
        // Position of the whole list of columns in source code
        public final SourcePositionRange listPos;
        public final SqlFragmentIdentifier tableName;
        public final List<SqlFragmentIdentifier> columnNames;

        TableAndColumns(SourcePositionRange listPos, SqlFragmentIdentifier tableName,
                        List<SqlFragmentIdentifier> columnNames) {
            this.listPos = listPos;
            this.tableName = tableName;
            this.columnNames = columnNames;
        }

        public static TableAndColumns fromJson(JsonNode node) {
            String tableName = Utilities.getStringProperty(node, "tableName");
            JsonNode cols = Utilities.getProperty(node, "columnNames");
            List<String> columns = Linq.list(Linq.map(cols.elements(), JsonNode::asText));
            return new TableAndColumns(SourcePositionRange.INVALID,
                    new SqlFragmentIdentifier(tableName),
                    Linq.map(columns, SqlFragmentIdentifier::new));
        }

        @Override
        public void asJson(ToJsonInnerVisitor visitor) {
            visitor.stream.beginObject()
                    .label("tableName").append(this.tableName.getString())
                    .label("columnNames").beginArray();
            for (var f: this.columnNames)
                visitor.stream.append(f.getString());
            visitor.stream.endArray().endObject();
        }
    }

    public final TableAndColumns thisTable;
    public final TableAndColumns otherTable;

    public JsonNode asJson() {
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        result.put("source", this.thisTable.tableName.toString());
        ArrayNode columns = result.putArray("columns");
        for (SqlFragment column: this.thisTable.columnNames)
            columns.add(column.getString());
        result.put("refers", this.otherTable.tableName.toString());
        ArrayNode tocolumns = result.putArray("tocolumns");
        for (SqlFragment column: this.otherTable.columnNames)
            tocolumns.add(column.getString());
        return result;
    }

    @Override
    public void asJson(ToJsonInnerVisitor visitor) {
        IJson.toJsonStream(this.asJson(), visitor.stream);
    }

    public static ForeignKey fromJson(JsonNode node) {
        String tableName = Utilities.getStringProperty(node, "source");
        JsonNode cols = Utilities.getProperty(node, "columns");
        List<String> columns = Linq.list(Linq.map(cols.elements(), JsonNode::asText));
        TableAndColumns thisTable = new TableAndColumns(SourcePositionRange.INVALID,
                new SqlFragmentIdentifier(tableName),
                Linq.map(columns, SqlFragmentIdentifier::new));

        tableName = Utilities.getStringProperty(node, "refers");
        cols = Utilities.getProperty(node, "tocolumns");
        columns = Linq.list(Linq.map(cols.elements(), JsonNode::asText));
        TableAndColumns otherTable = new TableAndColumns(SourcePositionRange.INVALID,
                new SqlFragmentIdentifier(tableName),
                Linq.map(columns, SqlFragmentIdentifier::new));
        return new ForeignKey(thisTable, otherTable);
    }
}
