package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;

/** Test-only activation of the disposable Rust folder. A refusal leaves the
 * original expression for runtime; there is no fallback to Java evaluation. */
public final class RustFoldExecutor implements AutoCloseable, RexOptimize.Executor {
    private final Process process;
    private final BufferedWriter input;
    private final BufferedReader output;
    private final BufferedWriter journal;
    private final String semanticsProfile;
    private long nextIndex;

    private record Reduction(RexNode node, boolean runtimeError) { }
    private record MapEntry(@Nullable Object key, @Nullable Object value) { }
    private record MapEntries(List<MapEntry> entries) { }

    public RustFoldExecutor(String executable, Path journal) throws IOException {
        this.semanticsProfile = System.getProperty("feldera.fold.semantics", "runtime-v1");
        if (!this.semanticsProfile.equals("runtime-v1")
                && !this.semanticsProfile.equals("legacy-exp-log-libm-0.2.15-v1")
                && !this.semanticsProfile.equals("legacy-janino-v1"))
            throw new IllegalArgumentException("Unknown fold semantics profile: " + this.semanticsProfile);
        this.journal = Files.newBufferedWriter(journal, StandardOpenOption.CREATE_NEW);
        try {
            this.process = new ProcessBuilder(executable, "--semantics-profile", this.semanticsProfile, "--stream")
                    .redirectError(journal.resolveSibling(journal.getFileName() + ".stderr").toFile()).start();
        } catch (IOException error) {
            try { this.journal.close(); } catch (IOException close) { error.addSuppressed(close); }
            throw error;
        }
        this.input = new BufferedWriter(new OutputStreamWriter(this.process.getOutputStream(), StandardCharsets.UTF_8));
        this.output = new BufferedReader(new InputStreamReader(this.process.getInputStream(), StandardCharsets.UTF_8));
    }

    @Override
    public void reduce(RexBuilder builder, List<RexNode> expressions, List<RexNode> output) {
        if (!JaninoFoldEligibility.accepts(expressions)) {
            output.addAll(expressions);
            FoldTrace.record("Rust", expressions, expressions);
            return;
        }
        List<RexNode> values = new ArrayList<>();
        boolean failed = false;
        for (RexNode expression: expressions) {
            Reduction result = evaluateResult(builder, expression, false);
            values.add(result.node());
            failed |= result.runtimeError();
        }
        // Janino reduces a batch atomically: a runtime error in one member
        // leaves all members unchanged. Retain that boundary and provenance.
        var result = failed ? expressions : values;
        output.addAll(result);
        FoldTrace.record("Rust", expressions, result);
    }

    @Override
    public RexNode optimize(RexBuilder builder, RexCall call) {
        // Replace this pass's temporal folds only. Folding unrelated calls
        // here changes their source provenance before the normal executor.
        SqlTypeName type = call.getType().getSqlTypeName();
        if (SqlTypeName.INTERVAL_TYPES.contains(type)) {
            // optimizeInterval does not fold casts. Evaluating them here
            // merely because the Rust registry supports them changes later
            // constant-view warnings. Leave casts at the normal executor
            // boundary, where their value semantics are still exercised.
            return switch (call.getKind()) {
                case MINUS_PREFIX, CHECKED_PLUS, CHECKED_MINUS, MINUS, PLUS,
                     CHECKED_TIMES, TIMES, CHECKED_DIVIDE, DIVIDE ->
                        this.evaluate(builder, call);
                default -> call;
            };
        }
        if (type == SqlTypeName.DATE
                || type == SqlTypeName.TIMESTAMP || type == SqlTypeName.TIME) {
            // This pass does not round or truncate dates. Those functions
            // belong to the normal executor, even when the registry can
            // evaluate them here. Moving them changes constant-view warnings.
            return switch (call.getKind()) {
                case CAST, SAFE_CAST -> canonicalTemporalCast(call)
                        ? this.evaluate(builder, call) : call;
                case PLUS, MINUS -> type == SqlTypeName.TIME ? call :
                        this.evaluate(builder, call);
                default -> call;
            };
        }
        return call;
    }

    /** Shape/precision eligibility only; the Rust worker performs the cast.
     * This pass folds canonical TIME/TIMESTAMP strings, not every string the
     * runtime parser accepts. In particular, it leaves date-only timestamps
     * and unpadded time fields at the normal executor boundary. */
    static boolean canonicalTemporalCast(RexCall call) {
        if (call.getOperands().size() != 1
                || !(call.getOperands().get(0) instanceof RexLiteral literal)
                || !SqlTypeUtil.isCharacter(literal.getType()))
            return false;
        NlsString string = literal.getValueAs(NlsString.class);
        if (string == null)
            return false;
        String value = string.getValue();
        String clock = "[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,9})?";
        boolean canonical = switch (call.getType().getSqlTypeName()) {
            case TIME -> value.matches(clock);
            case TIMESTAMP -> value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2} " + clock)
                    && !value.startsWith("0000-");
            default -> false;
        };
        int dot = value.indexOf('.');
        int precision = dot < 0 ? 0 : value.length() - dot - 1;
        return canonical && precision <= call.getType().getPrecision();
    }

    private RexNode evaluate(RexBuilder builder, RexNode expression) {
        return evaluateResult(builder, expression, true).node();
    }

    private Reduction evaluateResult(RexBuilder builder, RexNode expression, boolean temporalPass) {
        try {
            return this.reduce(builder, expression, temporalPass);
        } catch (IOException e) {
            throw new UncheckedIOException("Rust fold worker failed", e);
        }
    }

    private Reduction reduce(RexBuilder builder, RexNode expression, boolean temporalPass) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var request = mapper.createObjectNode();
        request.set("expression", FoldTrace.expression(expression, 0));
        this.input.write(mapper.writeValueAsString(request));
        this.input.newLine();
        this.input.flush();
        String line = this.readResponse();
        if (line == null)
            throw new IOException("Fold worker terminated early");
        JsonNode response = mapper.readTree(line);
        if (!this.semanticsProfile.equals(response.path("semantics_profile").asText()))
            throw new IOException("Rust fold worker semantics profile differs from the requested profile");
        if (!response.path("index").isIntegralNumber() || response.get("index").asLong() != this.nextIndex++)
            throw new IOException("Fold worker response order differs");
        request.put("case", System.getProperty("feldera.fold.case", "unscoped"));
        request.set("response", response);
        this.journal.write(mapper.writeValueAsString(request));
        this.journal.newLine();
        String status = response.path("status").asText();
        if (status.equals("unsupported") || status.equals("runtime_error") || status.equals("budget"))
            return new Reduction(expression, status.equals("runtime_error"));
        if (!status.equals("value"))
            throw new IOException("Unknown fold worker status: " + status);
        JsonNode value = response.get("value");
        if (value == null)
            throw new IOException("Fold worker omitted value");
        // This Calcite fork stores approximate literals as Double, including
        // NaN and infinities. Refusing those values changes later simplification.
        // Decode outside the compatibility boundary: malformed worker responses
        // must still fail the test. RexExecutable also abandons the entire batch
        // when a computed value cannot be represented as a Rex literal (notably
        // an empty array whose element type cannot be inferred).
        Object decoded = decode(value, expression.getType());
        // RexOptimize refuses a timestamp result that would lose fractional
        // digits. Test before RexBuilder rounds it to the declared precision.
        if (temporalPass && decoded instanceof TimestampString timestamp
                && !timestampFits(timestamp, expression.getType().getPrecision()))
            return new Reduction(expression, false);
        RexNode result;
        try {
            result = makeLiteral(builder, decoded, expression.getType());
        } catch (RuntimeException error) {
            return new Reduction(expression, true);
        }
        return new Reduction(result.equals(expression) ? expression : result, false);
    }

    static boolean timestampFits(TimestampString timestamp, int precision) {
        String text = timestamp.toString();
        int dot = text.indexOf('.');
        return precision >= 0 && (dot < 0 || text.length() - dot - 1 <= precision);
    }

    /** Preserve entries that were distinct before Janino coerced their keys.
     * Rebuilding a Java Map after coercion would silently discard an entry. */
    private static RexNode makeLiteral(RexBuilder builder, @Nullable Object value, RelDataType type) {
        List<RexNode> operands = new ArrayList<>();
        RexNode result;
        if (value instanceof MapEntries map) {
            for (MapEntry entry: map.entries()) {
                operands.add(makeLiteral(builder, entry.key(), java.util.Objects.requireNonNull(type.getKeyType())));
                operands.add(makeLiteral(builder, entry.value(), java.util.Objects.requireNonNull(type.getValueType())));
            }
            result = builder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, operands);
        } else if (value instanceof List<?> array) {
            for (Object element: array)
                operands.add(makeLiteral(builder, element, java.util.Objects.requireNonNull(type.getComponentType())));
            result = builder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, operands);
        } else {
            return builder.makeLiteral(value, type, true);
        }
        return type.isNullable() ? builder.makeAbstractCast(type, result, false) : result;
    }

    private String readResponse() throws IOException {
        StringBuilder line = new StringBuilder();
        for (int next; (next = this.output.read()) != -1;) {
            if (next == '\n')
                return line.toString();
            if (line.length() >= 8 * 1024 * 1024)
                throw new IOException("Fold worker response exceeds 8 MiB");
            line.append((char) next);
        }
        throw new IOException("Fold worker terminated before a complete response");
    }

    @Nullable private static Object decode(JsonNode value, RelDataType type) throws IOException {
        String text = value.path("value").asText();
        return switch (value.path("type").asText()) {
            case "null" -> null;
            case "integer", "decimal", "months" -> new BigDecimal(text);
            case "micros" -> new BigDecimal(text).movePointLeft(3);
            case "float" -> Double.longBitsToDouble(Long.parseUnsignedLong(text, 16));
            case "boolean" -> value.get("value").booleanValue();
            case "string" -> text;
            case "uuid" -> UUID.fromString(text);
            case "binary" -> new ByteString(HexFormat.of().parseHex(text));
            case "date" -> new DateString(text);
            case "time" -> new TimeString(text);
            case "timestamp" -> new TimestampString(text);
            case "point" -> {
                var coordinates = value.path("value");
                if (!coordinates.isArray() || coordinates.size() != 2
                        || type.getSqlTypeName() != org.apache.calcite.sql.type.SqlTypeName.GEOMETRY)
                    throw new IOException("Invalid point fold result");
                double x = Double.longBitsToDouble(Long.parseUnsignedLong(coordinates.get(0).asText(), 16));
                double y = Double.longBitsToDouble(Long.parseUnsignedLong(coordinates.get(1).asText(), 16));
                if (!Double.isFinite(x) || !Double.isFinite(y))
                    throw new IOException("Nonfinite point fold result");
                yield new org.locationtech.jts.geom.GeometryFactory().createPoint(
                        new org.locationtech.jts.geom.Coordinate(x, y));
            }
            case "array" -> {
                RelDataType element = type.getComponentType();
                if (element == null || !value.path("value").isArray())
                    throw new IOException("Invalid array fold result");
                List<Object> elements = new ArrayList<>();
                for (JsonNode item: value.get("value"))
                    elements.add(decode(item, element));
                yield elements;
            }
            case "map" -> {
                RelDataType key = type.getKeyType();
                RelDataType element = type.getValueType();
                if (key == null || element == null || !value.path("value").isArray())
                    throw new IOException("Invalid map fold result");
                List<MapEntry> entries = new ArrayList<>();
                for (JsonNode pair: value.get("value")) {
                    if (!pair.isArray() || pair.size() != 2)
                        throw new IOException("Invalid map fold entry");
                    entries.add(new MapEntry(decode(pair.get(0), key), decode(pair.get(1), element)));
                }
                yield new MapEntries(entries);
            }
            default -> throw new IOException("Unknown fold value type");
        };
    }

    @Override public void close() throws IOException {
        try {
            this.input.close();
            if (!this.process.waitFor(10, TimeUnit.SECONDS)) {
                this.process.destroyForcibly();
                throw new IOException("Fold worker did not exit");
            }
            if (this.process.exitValue() != 0 || this.output.readLine() != null)
                throw new IOException("Fold worker failed or returned extra output");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.process.destroyForcibly();
            throw new IOException("Interrupted waiting for fold worker", e);
        } finally {
            if (this.process.isAlive())
                this.process.destroyForcibly();
            try {
                this.output.close();
            } finally {
                this.journal.close();
            }
        }
    }
}
