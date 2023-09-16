package org.dbsp.sqlCompiler.compiler.backend.jit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.util.TimeString;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPFloatLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.util.Utilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode.jsonFactory;

public class JitJsonOutputDescription extends JitIODescription {
    final List<String> columns;

    public JitJsonOutputDescription(String relation, String path) {
        super(relation, path);
        this.columns = new ArrayList<>();
    }

    public void addColumn(String column) {
        this.columns.add(column);
    }

    public JsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        result.put("file", this.path);
        ObjectNode kind = result.putObject("kind");
        ObjectNode json = kind.putObject("Json");
        ObjectNode mappings = json.putObject("mappings");
        for (int i = 0; i < this.columns.size(); i++)
            mappings.put(Integer.toString(i), this.columns.get(i));
        return result;
    }

    RuntimeException parseError(long lineNumber, String line, String message) {
        return new RuntimeException("Error parsing json, line " + lineNumber + "\n" +
                line + "\n" + message);
    }

    DBSPExpression deserialize(long lineNumber, String line,
            ObjectNode node, String property, DBSPType type) {
        JsonNode field = node.get(property);
        if (field == null || field.isNull()) {
            if (type.mayBeNull)
                return DBSPLiteral.none(type);
            throw this.parseError(lineNumber, line,
                    "null value for property " + Utilities.singleQuote(property));
        }
        try {
            switch (type.code) {
                case BOOL:
                    return new DBSPBoolLiteral(field.booleanValue(), type.mayBeNull);
                case DATE:
                    return new DBSPDateLiteral(field.textValue(), type.mayBeNull);
                case DECIMAL:
                    return new DBSPDecimalLiteral(type, field.decimalValue());
                case DOUBLE:
                    return new DBSPDoubleLiteral(field.doubleValue(), type.mayBeNull);
                case FLOAT:
                    return new DBSPFloatLiteral(field.floatValue(), type.mayBeNull);
                case INT8:
                    // TODO: check overflow?
                    return new DBSPI8Literal((byte) field.intValue(), type.mayBeNull);
                case INT16:
                    return new DBSPI16Literal((short) field.intValue(), type.mayBeNull);
                case INT32:
                    return new DBSPI32Literal(field.intValue(), type.mayBeNull);
                case INT64:
                    return new DBSPI64Literal(field.longValue(), type.mayBeNull);
                case INTERVAL_SHORT:
                    return new DBSPIntervalMillisLiteral(field.longValue(), type.mayBeNull);
                case INTERVAL_LONG:
                    return new DBSPIntervalMonthsLiteral(field.intValue(), type.mayBeNull);
                case STRING:
                    return new DBSPStringLiteral(field.textValue(), type.mayBeNull);
                case TIME:
                    return new DBSPTimeLiteral(CalciteObject.EMPTY, type, new TimeString(field.textValue()));
                case TIMESTAMP:
                    return new DBSPTimestampLiteral(field.textValue(), type.mayBeNull);
                case NULL:
                case ANY:
                case GEOPOINT:
                case STR:
                case DATE_TZ:
                case TIMESTAMP_TZ:
                case ISIZE:
                case KEYWORD:
                case UNIT:
                case UINT16:
                case UINT32:
                case UINT64:
                case USIZE:
                case VOID:
                case WEIGHT:
                case KV:
                case FUNCTION:
                case INDEXED_ZSET:
                case RAW_TUPLE:
                case REF:
                case SEMIGROUP:
                case STREAM:
                case STRUCT:
                case TUPLE:
                case USER:
                case VEC:
                case ZSET:
                default:
                    break;
            }
        } catch (Exception ex) {
            throw this.parseError(lineNumber, line, ex.getMessage());
        }
        throw this.parseError(lineNumber, line, "Unexpected type " + type);
    }

    DBSPExpression parseLine(ObjectMapper objectMapper, long lineNumber, String line, DBSPTypeTuple elementType)
            throws JsonProcessingException {
        // TODO: this will have to change when the JSON format will support weights
        JsonNode jsonNode = objectMapper.readTree(line);
        if (!jsonNode.isObject()) {
            throw this.parseError(lineNumber, line,
                    "Expected an object for each row");
        }
        ObjectNode node = (ObjectNode) jsonNode;
        DBSPExpression[] fields = new DBSPExpression[elementType.size()];
        int index = 0;
        for (String column: this.columns) {
            DBSPType type = elementType.getFieldType(index);
            DBSPExpression expression = this.deserialize(lineNumber, line, node, column, type);
            fields[index] = expression;
            index++;
        }
        return new DBSPTupleExpression(fields);
    }

    @Override
    public DBSPZSetLiteral.Contents parse(DBSPType elementType) throws IOException {
        File file = new File(this.path);
        List<String> lines = Files.readAllLines(file.toPath());
        DBSPZSetLiteral.Contents result = DBSPZSetLiteral.Contents.emptyWithElementType(elementType);
        DBSPTypeTuple tuple = elementType.to(DBSPTypeTuple.class);
        ObjectMapper objectMapper = jsonFactory();
        long lineNumber = 0;
        for (String line: lines) {
            DBSPExpression expression = this.parseLine(objectMapper, lineNumber, line, tuple);
            result.add(expression);
        }
        return result;
    }
}
