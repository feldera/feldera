package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConnectorValidator;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;
import org.dbsp.util.Utilities;

import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

/** The production connector validator as an oracle. Internal failures are
 * recorded separately and must never become negative validation goldens. */
public final class ConnectorProbe {
    private static final ObjectMapper MAPPER = Utilities.deterministicObjectMapper();
    private static final String PACKAGE = "org.dbsp.sqlCompiler.compiler.frontend.connectors.config.";
    private static final Set<String> PROTOTYPE = Set.of("PostgresReaderConfig", "KafkaInputConfig",
            "IcebergReaderConfig", "AvroParserConfig", "AvroEncoderConfig", "HttpOutputConfig");

    private ConnectorProbe() { }

    private static void compileSql(JsonNode source, ObjectNode record) {
        CompilerOptions options = new CompilerOptions();
        options.ioOptions.quiet = true;
        options.ioOptions.inputCircuit = true;
        options.languageOptions.throwOnError = false;
        options.languageOptions.optimizationLevel = 0;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(source.path("program").asText());
        compiler.getFinalCircuit(false);
        record.put("accepted", !compiler.hasErrors());
        record.put("compiler_bug", compiler.messages.messages.stream().anyMatch(message ->
                message.errorType.equals("Compiler error") || message.errorType.startsWith("This is a bug")));
        ArrayNode warnings = record.putArray("warnings");
        for (var message: compiler.messages.messages) {
            ObjectNode warning = warnings.addObject();
            warning.put("warning", message.warning);
            warning.put("continuation", message.continuation);
            warning.put("error_type", message.errorType);
            warning.put("message", message.message);
            warning.set("position", message.range.asJson());
            warning.put("snippet", compiler.sources.getFragment(message.range, true));
        }
    }

    private static final class Reporter implements IErrorReporter {
        final ArrayNode warnings = MAPPER.createArrayNode();
        @Override public void setErrorContext(SourcePositionRange range) { }
        @Override public boolean hasErrors() { return false; }
        @Override public void reportProblem(SourcePositionRange range, boolean warning, boolean continuation,
                                            String errorType, String message) {
            ObjectNode record = this.warnings.addObject();
            record.put("warning", warning);
            record.put("continuation", continuation);
            record.put("error_type", errorType);
            record.put("message", message);
            record.set("position", range.asJson());
        }
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends IValidateConfig> configClass(String name) throws Exception {
        Class<?> type = Class.forName(PACKAGE + name);
        if (!IValidateConfig.class.isAssignableFrom(type))
            throw new IllegalArgumentException("Not a connector configuration: " + name);
        return (Class<? extends IValidateConfig>) type;
    }

    private static void inventory(Path sources, Path output) throws Exception {
        ObjectNode result = MAPPER.createObjectNode();
        ArrayNode classes = result.putArray("classes");
        try (var files = Files.list(sources)) {
            for (Path source: files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
                String name = source.getFileName().toString().replaceFirst("\\.java$", "");
                Class<?> type = Class.forName(PACKAGE + name);
                if (!IValidateConfig.class.isAssignableFrom(type))
                    continue;
                Object instance = type.getConstructor().newInstance();
                ObjectNode record = classes.addObject();
                record.put("class", name);
                record.put("prototype", PROTOTYPE.contains(name));
                record.put("accepts_unknown", Arrays.stream(type.getMethods())
                        .anyMatch(method -> method.isAnnotationPresent(JsonAnySetter.class)));
                ArrayNode fields = record.putArray("fields");
                for (var field: Arrays.stream(type.getFields()).sorted(Comparator.comparing(java.lang.reflect.Field::getName)).toList()) {
                    if (Modifier.isStatic(field.getModifiers()))
                        continue;
                    JsonProperty property = field.getAnnotation(JsonProperty.class);
                    if (property == null)
                        throw new IllegalStateException("Missing wire field name: " + field);
                    ObjectNode item = fields.addObject();
                    item.put("name", property.value());
                    item.put("java_type", field.getGenericType().getTypeName());
                    item.set("default", MAPPER.valueToTree(field.get(instance)));
                    if (field.getType().isEnum()) {
                        ArrayNode values = item.putArray("enum_values");
                        for (Object value: field.getType().getEnumConstants())
                            values.add(MAPPER.valueToTree(value));
                    }
                }
            }
        }
        Files.write(output, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(result), StandardOpenOption.CREATE_NEW);
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[0].equals("--inventory")) {
            inventory(Path.of(args[1]), Path.of(args[2]));
            return;
        }
        if (args.length != 2)
            throw new IllegalArgumentException("Usage: ConnectorProbe INPUT.jsonl OUTPUT.jsonl | --inventory SOURCE_DIR OUTPUT.json");
        Set<String> ids = new HashSet<>();
        try (var input = Files.newBufferedReader(Path.of(args[0]));
             var output = Files.newBufferedWriter(Path.of(args[1]), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonNode request = MAPPER.readTree(line);
                String name = request.path("config_class").asText();
                String id = request.path("id").asText();
                if (id.isEmpty() || !ids.add(id) || !PROTOTYPE.contains(name))
                    throw new IllegalArgumentException("Duplicate/empty identity or unsupported configuration class");
                String raw = request.path("json").asText();
                String pointer = request.path("pointer").asText();
                JsonNode document = MAPPER.readTree(raw);
                JsonNode config = document.at(pointer);
                if (!config.isObject())
                    throw new IllegalArgumentException("Probe must select a configuration object");
                Reporter reporter = new Reporter();
                ObjectNode record = MAPPER.createObjectNode();
                record.put("id", id);
                try {
                    if (request.has("source")) {
                        compileSql(request.get("source"), record);
                    } else {
                        ConnectorValidator.validateConfig(config, raw, pointer,
                                new SourcePosition(1, 0), configClass(name), reporter);
                        record.put("compiler_bug", false);
                    }
                } catch (RuntimeException | AssertionError error) {
                    record.put("compiler_bug", true);
                    record.put("exception", error.getClass().getName());
                    record.put("detail", error.getMessage());
                }
                if (!record.has("warnings"))
                    record.set("warnings", reporter.warnings);
                output.write(MAPPER.writeValueAsString(record));
                output.newLine();
                output.flush();
            }
        }
    }
}
