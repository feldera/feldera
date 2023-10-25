package org.dbsp.sqlCompiler.compiler.backend.jit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataType;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.json.simple.JSONObject;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode.jsonFactory;

public class JitCsvInputDescription extends JitIODescription {
    public static class Column {
        public final int position;
        public final int remappedPosition;
        @Nullable
        public final String format;

        public Column(int position, int remappedPosition, RelDataType type) {
            this.position = position;
            this.remappedPosition = remappedPosition;
            switch (type.getSqlTypeName()) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case DECIMAL:
                case FLOAT:
                case REAL:
                case DOUBLE:
                case CHAR:
                case VARCHAR:
                case NULL:
                    this.format = null;
                    break;
                case DATE:
                    this.format = "%F";
                    break;
                case TIME:
                    this.format = "%T";
                    break;
                case TIMESTAMP:
                    this.format = "%F %T";
                    break;
                case TIME_WITH_LOCAL_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                case INTERVAL_DAY:
                case INTERVAL_DAY_HOUR:
                case INTERVAL_DAY_MINUTE:
                case INTERVAL_DAY_SECOND:
                case INTERVAL_HOUR:
                case INTERVAL_HOUR_MINUTE:
                case INTERVAL_HOUR_SECOND:
                case INTERVAL_MINUTE:
                case INTERVAL_MINUTE_SECOND:
                case INTERVAL_SECOND:
                case BINARY:
                case VARBINARY:
                case UNKNOWN:
                case ANY:
                case SYMBOL:
                case MULTISET:
                case ARRAY:
                case MAP:
                case DISTINCT:
                case STRUCTURED:
                case ROW:
                case OTHER:
                case CURSOR:
                case COLUMN_LIST:
                case DYNAMIC_STAR:
                case GEOMETRY:
                case MEASURE:
                case SARG:
                default:
                    throw new UnsupportedException(new CalciteObject(type));
            }
        }

        JsonNode asJson() {
            ArrayNode result = jsonFactory().createArrayNode();
            result.add(this.position);
            result.add(this.remappedPosition);
            result.add(this.format);
            return result;
        }
    }

    final List<Column> columns;
    final List<Column> keyColumns;

    public JitCsvInputDescription(String relation, String path) {
        super(relation, path);
        this.columns = new ArrayList<>();
        this.keyColumns = new ArrayList<>();
    }

    public void addColumn(Column column) {
        this.columns.add(column);
    }

    public void addKeyColumn(Column column) {
        this.keyColumns.add(column);
    }

    public JsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        result.put("file", this.path);
        ObjectNode kind = result.putObject("kind");
        ObjectNode csv = kind.putObject("Csv");
        if (this.keyColumns.isEmpty()) {
            ArrayNode set = csv.putArray("Set");
            for (Column column: this.columns)
                set.add(column.asJson());
        } else {
            ArrayNode map = csv.putArray("Map");
            ArrayNode keys = map.addArray();
            for (Column column: this.keyColumns)
                keys.add(column.asJson());
            ArrayNode columns = map.addArray();
            for (Column column: this.columns)
                columns.add(column.asJson());
        }
        return result;
    }
}
