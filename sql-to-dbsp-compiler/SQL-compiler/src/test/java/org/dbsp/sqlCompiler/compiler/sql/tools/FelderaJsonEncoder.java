package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.IColumnMetadata;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPHandleErrorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariantExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLongIntervalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPRealLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPShortIntervalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampTzLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUuidLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVariantNullLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Encodes Z-set literals as the feldera JSON {@code insert_delete} records that
 * the pipeline's {@code /ingress} endpoint reads and its JSON output connector writes. */
public final class FelderaJsonEncoder {
    /** A row with a larger weight is refused: it expands into that many JSON records. */
    static final long MAX_ABS_WEIGHT = 10_000;

    private static final JsonNodeFactory NODES = JsonNodeFactory.withExactBigDecimals(true);

    private FelderaJsonEncoder() {}

    /** One {@code {"insert": row}} or {@code {"delete": row}} record per unit of weight.
     * Deletes come first, so a primary-key table sees an update as delete-then-insert. */
    static ArrayNode encodeZSet(DBSPZSetExpression zset, List<? extends IColumnMetadata> columns) {
        ArrayNode deletes = NODES.arrayNode();
        ArrayNode inserts = NODES.arrayNode();
        for (Map.Entry<DBSPExpression, Long> entry : zset.data.entrySet()) {
            long weight = entry.getValue();
            if (Math.abs(weight) > MAX_ABS_WEIGHT)
                throw new UnsupportedException("Row weight " + weight + " exceeds the export limit of "
                        + MAX_ABS_WEIGHT, zset.getNode());
            ObjectNode row = encodeRow(entry.getKey(), columns);
            String polarity = weight > 0 ? "insert" : "delete";
            ArrayNode records = weight > 0 ? inserts : deletes;
            for (long unit = 0; unit < Math.abs(weight); unit++) {
                ObjectNode record = NODES.objectNode();
                record.set(polarity, row);
                records.add(record);
            }
        }
        deletes.addAll(inserts);
        return deletes;
    }

    private static ObjectNode encodeRow(DBSPExpression rowExpression, List<? extends IColumnMetadata> columns) {
        if (!(rowExpression instanceof DBSPTupleExpression tuple) || tuple.fields == null)
            throw new UnsupportedException("Row is not a tuple literal", rowExpression.getNode());
        if (tuple.size() != columns.size())
            throw new UnsupportedException("Row has " + tuple.size() + " fields but the relation has "
                    + columns.size() + " columns", rowExpression.getNode());
        ObjectNode row = NODES.objectNode();
        for (int index = 0; index < columns.size(); index++) {
            IColumnMetadata column = columns.get(index);
            row.set(column.getColumnName().name(), encodeValue(tuple.fields[index], column.getType()));
        }
        return row;
    }

    /** {@code declared} carries the SQL field names of ROW values; the literal's own type does not.
     * The pipeline decodes a JSON value only into a matching column type, so a value cast from
     * another type is converted the way SQL's CAST converts it: to its text for a string
     * column, and from trimmed text to a number for a numeric column. */
    static JsonNode encodeValue(DBSPExpression value, @Nullable DBSPType declared) {
        JsonNode encoded = encodeLiteral(value, declared);
        boolean isStringColumn = declared != null && declared.is(DBSPTypeString.class);
        if (isStringColumn && encoded.isValueNode() && !encoded.isNull() && !encoded.isTextual())
            return NODES.textNode(encoded.asText());
        boolean isNumericColumn = declared != null && declared.is(IsNumericType.class);
        if (isNumericColumn && encoded.isTextual()) {
            try {
                return NODES.numberNode(new BigDecimal(encoded.asText().trim()));
            } catch (NumberFormatException notANumber) {
                // NaN and infinity stay text; the pipeline parses those spellings itself.
                return encoded;
            }
        }
        return encoded;
    }

    private static JsonNode encodeLiteral(DBSPExpression value, @Nullable DBSPType declared) {
        if (value instanceof DBSPSomeExpression some)
            return encodeValue(some.expression, declared);
        if (value instanceof DBSPHandleErrorExpression handled)
            return encodeValue(handled.source, declared);
        // The pipeline decodes each JSON value into its column type, which performs the cast.
        if (value instanceof DBSPCastExpression cast)
            return encodeValue(cast.source, declared);
        if (value instanceof DBSPLiteral literal && literal.isNull())
            return NODES.nullNode();
        if (value instanceof DBSPVariantNullLiteral)
            return NODES.nullNode();
        if (value instanceof DBSPBoolLiteral bool)
            return NODES.booleanNode(bool.value);
        if (value instanceof DBSPIntLiteral integer)
            return NODES.numberNode(integer.getValue());
        if (value instanceof DBSPDecimalLiteral decimal)
            return NODES.numberNode(decimal.value);
        if (value instanceof DBSPRealLiteral real)
            return encodeFloat(real.value.doubleValue(), NODES.numberNode(real.value));
        if (value instanceof DBSPDoubleLiteral fp)
            return encodeFloat(fp.value, NODES.numberNode(fp.value));
        if (value instanceof DBSPStringLiteral string)
            return NODES.textNode(string.value);
        if (value instanceof DBSPStrLiteral string)
            return NODES.textNode(string.value);
        if (value instanceof DBSPDateLiteral date)
            return NODES.textNode(date.getDateString().toString());
        if (value instanceof DBSPTimeLiteral time)
            return NODES.textNode(withoutTrailingDot(String.valueOf(time.value)));
        if (value instanceof DBSPTimestampLiteral timestamp)
            return NODES.textNode(withoutTrailingDot(timestamp.getTimestampString().toString()));
        if (value instanceof DBSPTimestampTzLiteral timestamp)
            return NODES.textNode(timestamp.getTimestampTzString().toString());
        if (value instanceof DBSPUuidLiteral uuid)
            return NODES.textNode(uuid.value.toString());
        if (value instanceof DBSPBinaryLiteral binary)
            return encodeBinary(binary.value);
        if (value instanceof DBSPShortIntervalLiteral || value instanceof DBSPLongIntervalLiteral)
            throw new UnsupportedException("INTERVAL values have no feldera JSON encoding", value.getNode());
        if (value instanceof DBSPVariantExpression variant)
            return variant.isSqlNull || variant.value == null
                    ? NODES.nullNode() : encodeValue(variant.value, variant.value.getType());
        if (value instanceof DBSPArrayExpression array)
            return encodeArray(array, declared);
        if (value instanceof DBSPMapExpression map)
            return encodeMap(map, declared);
        if (value instanceof DBSPTupleExpression tuple)
            return encodeStruct(tuple, declared);
        throw new UnsupportedException("Value is not a literal: " + value.getClass().getSimpleName(),
                value.getNode());
    }

    /** Calcite prints a time with an empty fraction as {@code 12:00:00.}, which the pipeline rejects. */
    private static String withoutTrailingDot(String temporal) {
        return temporal.endsWith(".") ? temporal.substring(0, temporal.length() - 1) : temporal;
    }

    /** JSON has no NaN or infinity, so those travel as the strings Rust's float parser accepts. */
    private static JsonNode encodeFloat(double value, JsonNode finite) {
        if (Double.isNaN(value))
            return NODES.textNode("NaN");
        if (Double.isInfinite(value))
            return NODES.textNode(value > 0 ? "inf" : "-inf");
        return finite;
    }

    private static JsonNode encodeBinary(byte[] bytes) {
        ArrayNode array = NODES.arrayNode();
        for (byte b : bytes)
            array.add(b & 0xff);
        return array;
    }

    private static JsonNode encodeArray(DBSPArrayExpression array, @Nullable DBSPType declared) {
        if (array.data == null)
            return NODES.nullNode();
        DBSPType element = declared instanceof DBSPTypeArray declaredArray ? declaredArray.getElementType() : null;
        ArrayNode result = NODES.arrayNode();
        for (DBSPExpression item : array.data)
            result.add(encodeValue(item, element));
        return result;
    }

    /** A JSON object key is always a string, so a non-string map key is written as its JSON text. */
    private static JsonNode encodeMap(DBSPMapExpression map, @Nullable DBSPType declared) {
        if (map.keys == null || map.values == null)
            return NODES.nullNode();
        DBSPTypeMap declaredMap = declared instanceof DBSPTypeMap m ? m : null;
        ObjectNode result = NODES.objectNode();
        for (int index = 0; index < map.keys.size(); index++) {
            JsonNode key = encodeValue(map.keys.get(index), declaredMap == null ? null : declaredMap.getKeyType());
            JsonNode entry = encodeValue(map.values.get(index),
                    declaredMap == null ? null : declaredMap.getValueType());
            result.set(key.isTextual() ? key.asText() : key.toString(), entry);
        }
        return result;
    }

    private static JsonNode encodeStruct(DBSPTupleExpression tuple, @Nullable DBSPType declared) {
        if (tuple.fields == null)
            return NODES.nullNode();
        if (!(declared instanceof DBSPTypeStruct struct))
            throw new UnsupportedException("ROW value without a declared struct type", tuple.getNode());
        List<DBSPTypeStruct.Field> fields = new ArrayList<>(struct.fields.values());
        if (fields.size() != tuple.size())
            throw new UnsupportedException("ROW value has " + tuple.size() + " fields but its type has "
                    + fields.size(), tuple.getNode());
        ObjectNode result = NODES.objectNode();
        for (int index = 0; index < fields.size(); index++) {
            DBSPTypeStruct.Field field = fields.get(index);
            result.set(field.name.name(), encodeValue(tuple.fields[index], field.type));
        }
        return result;
    }
}
