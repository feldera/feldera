package org.dbsp.sqlCompiler.compiler.frontend.connectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.config.*;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Validation helpers for connector format and transport configuration objects. */
public final class ConnectorValidator {
    private ConnectorValidator() {}

    /** Returns the comma-separated list of serialized names for all constants of an enum type. */
    private static String enumValidValues(Class<?> enumType) {
        Object[] constants = enumType.getEnumConstants();
        Utilities.enforce(constants != null, () -> "enumType must be an enum class");
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        return Arrays.stream(constants)
                .map(c -> mapper.convertValue(c, JsonNode.class).toString())
                .collect(Collectors.joining(", "));
    }

    /**
     * Converts a Jackson deserialization path to a JSON Pointer suffix
     * (e.g. {@code "/field1/field2"}).
     */
    private static String pathToPointer(List<JsonMappingException.Reference> path) {
        return path.stream()
                .map(ref -> ref.getFieldName() != null
                        ? "/" + ref.getFieldName()
                        : "/" + ref.getIndex())
                .collect(Collectors.joining());
    }

    /**
     * Converts a Jackson deserialization path to a dot-separated field name
     * (e.g. {@code "field1.field2"}).
     */
    private static String pathToDotted(List<JsonMappingException.Reference> path) {
        return path.stream()
                .map(ref -> ref.getFieldName() != null
                        ? ref.getFieldName() : "[" + ref.getIndex() + "]")
                .collect(Collectors.joining("."));
    }

    /** Emits a warning pointing at the key identified by {@code pointer}. */
    private static void warnAt(String outerJson, String pointer, SourcePosition outerStart,
                               IErrorReporter reporter, String category, String message) {
        SourcePosition pos = Utilities.jsonPointerLocation(outerJson, pointer, false);
        if (pos == null)
            pos = Utilities.jsonPointerLocation(outerJson, pointer, true);
        SourcePositionRange range = pos != null
                ? pos.relativeTo(outerStart).asRange()
                : SourcePositionRange.INVALID;
        reporter.reportWarning(range, category, message);
    }

    /**
     * Validates the {@code format.config} object of a single connector JSON node,
     * emitting warnings for unknown fields or wrong-typed values.
     *
     * @param connector     Parsed JSON object for one connector.
     * @param connectorPath JSON Pointer prefix for this connector within {@code outerJson}
     *                      (e.g. {@code "/0"}).
     * @param isTable       {@code true} for table (input) connectors, {@code false} for
     *                      view (output) connectors.
     * @param outerJson     The complete connectors JSON string.
     * @param outerStart    Absolute source position of {@code outerJson}'s first character
     *                      in the SQL program.
     * @param reporter      Error reporter used.
     */
    public static void validateFormatConfig(
            JsonNode connector, String connectorPath,
            boolean isTable, String outerJson, SourcePosition outerStart,
            IErrorReporter reporter) {
        JsonNode format = connector.get("format");
        if (format == null)
            return;
        if (!format.isObject()) {
            warnAt(outerJson, connectorPath + "/format", outerStart, reporter,
                    "Invalid connector", "\"format\" must be a JSON object");
            return;
        }
        JsonNode formatNameNode = format.get("name");
        if (formatNameNode == null) {
            warnAt(outerJson, connectorPath + "/format", outerStart, reporter,
                    "Invalid connector", "\"format\" must have a \"name\" field");
            return;
        }
        if (!formatNameNode.isTextual()) {
            warnAt(outerJson, connectorPath + "/format/name", outerStart, reporter,
                    "Invalid connector", "\"format.name\" must be a string");
            return;
        }
        JsonNode formatConfig = format.get("config");
        if (formatConfig == null)
            return;
        if (!formatConfig.isObject()) {
            warnAt(outerJson, connectorPath + "/format/config", outerStart, reporter,
                    "Invalid connector", "\"format.config\" must be a JSON object");
            return;
        }
        String configPointer = connectorPath + "/format/config";
        String formatName = formatNameNode.asText();
        if (isTable) {
            switch (formatName) {
                case "avro":
                    validateConfig(formatConfig, outerJson, configPointer,
                            outerStart, AvroParserConfig.class, reporter);
                    break;
                case "csv":
                    validateConfig(formatConfig, outerJson, configPointer,
                            outerStart, CsvParserConfig.class, reporter);
                    break;
                case "json":
                    validateConfig(formatConfig, outerJson, configPointer,
                            outerStart, JsonParserConfig.class, reporter);
                    break;
                case "parquet":
                    validateConfig(formatConfig, outerJson, configPointer,
                            outerStart, ParquetParserConfig.class, reporter);
                    break;
                case "raw":
                    validateConfig(formatConfig, outerJson, configPointer,
                            outerStart, RawParserConfig.class, reporter);
                    break;
                default:
                    warnAt(outerJson, connectorPath + "/format/name", outerStart, reporter,
                            "Unknown format", "\"format.name\" " + Utilities.doubleQuote(formatName, true) + " is not known");
                    break;
            }
        } else {
            switch (formatName) {
                case "avro":
                    validateConfig(formatConfig, outerJson, configPointer,
                            outerStart, AvroEncoderConfig.class, reporter);
                    break;
                case "csv":
                    validateConfig(formatConfig, outerJson, configPointer,
                            outerStart, CsvEncoderConfig.class, reporter);
                    break;
                case "json":
                    validateConfig(formatConfig, outerJson, configPointer,
                            outerStart, JsonEncoderConfig.class, reporter);
                    break;
                case "parquet":
                    validateConfig(formatConfig, outerJson, configPointer,
                            outerStart, ParquetEncoderConfig.class, reporter);
                    break;
                default:
                    warnAt(outerJson, connectorPath + "/format/name", outerStart, reporter,
                            "Unknown format", "\"format.name\" " + Utilities.doubleQuote(formatName, true) + " is not known");
                    break;
            }
        }
    }

    /**
     * Validates the {@code transport.config} object of a single connector JSON node,
     * emitting warnings for unknown fields or wrong-typed values.
     *
     * @param connector     Parsed JSON object for one connector.
     * @param connectorPath JSON Pointer prefix for this connector within {@code outerJson}.
     * @param isTable       {@code true} for table (input) connectors, {@code false} for
     *                      view (output) connectors.
     * @param outerJson     The complete connectors JSON string.
     * @param outerStart    Absolute source position of {@code outerJson}'s first character
     *                      in the SQL program.
     * @param reporter      Error reporter.
     */
    public static void validateTransportConfig(
            JsonNode connector, String connectorPath,
            boolean isTable, String outerJson, SourcePosition outerStart,
            IErrorReporter reporter) {
        JsonNode transport = connector.get("transport");
        if (transport == null)
            return;
        if (!transport.isObject()) {
            warnAt(outerJson, connectorPath + "/transport", outerStart, reporter,
                    "Invalid connector", "\"transport\" must be a JSON object");
            return;
        }
        JsonNode transportNameNode = transport.get("name");
        if (transportNameNode == null) {
            warnAt(outerJson, connectorPath + "/transport", outerStart, reporter,
                    "Invalid connector", "\"transport\" must have a \"name\" field");
            return;
        }
        if (!transportNameNode.isTextual()) {
            warnAt(outerJson, connectorPath + "/transport/name", outerStart, reporter,
                    "Invalid connector", "\"transport.name\" must be a string");
            return;
        }
        JsonNode transportConfig = transport.get("config");
        if (transportConfig == null)
            return;
        if (!transportConfig.isObject()) {
            warnAt(outerJson, connectorPath + "/transport/config", outerStart, reporter,
                    "Invalid connector", "\"transport.config\" must be a JSON object");
            return;
        }
        String configPointer = connectorPath + "/transport/config";
        String transportName = transportNameNode.asText();
        if (isTable) {
            switch (transportName) {
                case "delta_table_input":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, DeltaTableReaderConfig.class, reporter);
                    break;
                case "iceberg_input":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, IcebergReaderConfig.class, reporter);
                    break;
                case "kafka_input":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, KafkaInputConfig.class, reporter);
                    break;
                case "postgres_input":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, PostgresReaderConfig.class, reporter);
                    break;
                case "postgres_cdc_input":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, PostgresCdcReaderConfig.class, reporter);
                    break;
                case "pub_sub_input":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, PubSubInputConfig.class, reporter);
                    break;
                case "s3_input":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, S3InputConfig.class, reporter);
                    break;
                case "clock":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, ClockConfig.class, reporter);
                    break;
                case "file_input":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, FileInputConfig.class, reporter);
                    break;
                case "url_input":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, UrlInputConfig.class, reporter);
                    break;
                case "nats_input":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, NatsInputConfig.class, reporter);
                    break;
                case "rabbitmq_input":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, RabbitmqInputConfig.class, reporter);
                    break;
                case "datagen":
                case "nexmark":
                case "empty":
                case "adhoc":
                    break;
                default:
                    warnAt(outerJson, connectorPath + "/transport/name", outerStart, reporter,
                            "Unknown format", "\"transport.name\" " + Utilities.doubleQuote(transportName, true) + " is not known");
                    break;
            }
        } else {
            switch (transportName) {
                case "delta_table_output":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, DeltaTableWriterConfig.class, reporter);
                    break;
                case "http_output":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, HttpOutputConfig.class, reporter);
                    break;
                case "kafka_output":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, KafkaOutputConfig.class, reporter);
                    break;
                case "postgres_output":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, PostgresWriterConfig.class, reporter);
                    break;
                case "dynamodb_output":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, DynamoDBWriterConfig.class, reporter);
                    break;
                case "file_output":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, FileOutputConfig.class, reporter);
                    break;
                case "redis_output":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, RedisOutputConfig.class, reporter);
                    break;
                case "rabbitmq_output":
                    validateConfig(transportConfig, outerJson, configPointer,
                            outerStart, RabbitmqOutputConfig.class, reporter);
                    break;
                case "null":
                    break;
                default:
                    warnAt(outerJson, connectorPath + "/transport/name", outerStart, reporter,
                            "Unknown format", "\"transport.name\" " + Utilities.doubleQuote(transportName, true) + " is not known");
                    break;
            }
        }
    }

    /**
     * Deserializes a JSON config node into a typed POJO, reporting type errors
     * and unknown fields as warnings via the error reporter.
     *
     * @param configNode    The already-parsed JSON node to deserialize.
     * @param outerJson     The raw JSON string containing {@code configNode}.
     * @param configPointer JSON Pointer path to {@code configNode} within {@code outerJson}
     *                      (e.g. {@code "/0/format/config"}).
     * @param outerStart    Absolute source position of {@code outerJson}'s first character,
     *                      used to convert within-JSON positions to source positions.
     * @param clazz         Target class to deserialize into.
     * @param reporter      Error reporter.
     * @return Deserialized object, or {@code null} if validation errors were found.
     */
    @Nullable
    public static <T extends IValidateConfig> T validateConfig(
            JsonNode configNode,
            String outerJson,
            String configPointer,
            SourcePosition outerStart,
            Class<T> clazz,
            IErrorReporter reporter) {
        try {
            T result = Utilities.deterministicObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .treeToValue(configNode, clazz);
            var configReporter = new ConfigReporter(reporter, outerJson, configPointer, outerStart);
            result.validate(configReporter);
            return result;
        } catch (JsonMappingException e) {
            String fullPointer = configPointer + pathToPointer(e.getPath());

            // Always point at the field key so the user sees exactly what to fix.
            SourcePosition pos = Utilities.jsonPointerLocation(outerJson, fullPointer, false);
            if (pos == null)
                pos = Utilities.jsonPointerLocation(outerJson, configPointer, true);
            SourcePositionRange range = pos != null
                    ? pos.relativeTo(outerStart).asRange()
                    : SourcePositionRange.INVALID;

            String message;
            if (e instanceof UnrecognizedPropertyException upe) {
                message = "unknown field " + Utilities.doubleQuote(upe.getPropertyName(), false);
            } else if (e instanceof InvalidFormatException ife
                    && ife.getTargetType() != null && ife.getTargetType().isEnum()) {
                String fieldName = pathToDotted(e.getPath());
                String validValues = enumValidValues(ife.getTargetType());
                message = (fieldName.isEmpty() ? "" : "field " + Utilities.doubleQuote(fieldName, false) + ": ")
                        + "invalid value " + Utilities.doubleQuote(String.valueOf(ife.getValue()), false)
                        + "; valid values are: " + validValues;
            } else {
                String field = pathToDotted(e.getPath());
                message = (field.isEmpty() ? "" : "field " + Utilities.doubleQuote(field, false) + ": ")
                        + e.getOriginalMessage();
            }
            reporter.reportWarning(range, "Invalid configuration", message);
            return null;
        } catch (JsonProcessingException e) {
            SourcePosition pos = Utilities.jsonPointerLocation(outerJson, configPointer, true);
            SourcePositionRange range = pos != null
                    ? pos.relativeTo(outerStart).asRange()
                    : SourcePositionRange.INVALID;
            reporter.reportWarning(range, "Invalid configuration", e.getOriginalMessage());
            return null;
        }
    }
}
