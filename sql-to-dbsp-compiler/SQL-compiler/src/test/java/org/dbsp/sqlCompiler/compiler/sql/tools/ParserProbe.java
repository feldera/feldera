package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.codec.digest.DigestUtils;
import org.dbsp.generated.parser.ParseException;
import org.dbsp.generated.parser.TokenMgrError;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.util.Static.RESOURCE;

/** Raw Java parser oracle for the P0 syntax experiment. This deliberately stops
 * before post-parse validation, binding, and relational lowering; a parse success
 * is not a compilation verdict or an S1 semantic-equivalence result. */
public final class ParserProbe {
    private static final ObjectMapper MAPPER = Utilities.deterministicObjectMapper();

    static ObjectNode parse(DBSPCompiler compiler, JsonNode submission) throws IOException {
        JsonNode sql = submission.get("sql");
        JsonNode many = submission.get("many");
        if (sql == null || !sql.isTextual() || many == null || !many.isBoolean())
            throw new IOException("A submission needs SQL text and a boolean parsing mode");
        ObjectNode result = MAPPER.createObjectNode();
        try {
            // DBSPCompiler.runParser treats an empty many-statement submission
            // as a source-line advance, without calling Calcite's list parser.
            if (many.asBoolean() && sql.asText().isEmpty()) {
                compiler.getSqlToRelCompiler().emptyStatement();
                result.put("verdict", "accepted");
                result.putArray("statements");
                return result;
            }
            List<SqlNode> statements = many.asBoolean()
                    ? compiler.getSqlToRelCompiler().parseStatements(sql.asText(), true, false).stream()
                        .map(statement -> statement.statement()).toList()
                    : List.of(compiler.getSqlToRelCompiler().parse(sql.asText(), true, false));
            result.put("verdict", "accepted");
            ArrayNode parsed = result.putArray("statements");
            for (SqlNode statement: statements) {
                ObjectNode node = parsed.addObject();
                node.put("node_class", statement.getClass().getSimpleName());
                node.put("kind", statement.getKind().toString());
                if (statement instanceof org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView view) {
                    node.put("view_kind", view.viewKind.name());
                    node.put("replace", view.getReplace());
                }
                try {
                    node.put("sql", statement.toString());
                } catch (ClassCastException error) {
                    // Parsing already succeeded. Calcite's table-hint printer
                    // cannot render a literal-chain call as a SqlLiteral.
                    // Preserve that distinct failure; never invent SQL text or
                    // turn it into a syntax rejection. Unknown failures abort.
                    if (!knownHintUnparseFailure(error))
                        throw error;
                    ObjectNode failure = node.putObject("unparse_failure");
                    failure.put("type", error.getClass().getName());
                    failure.put("message", error.getMessage());
                    ArrayNode stack = failure.putArray("stack");
                    for (StackTraceElement frame: error.getStackTrace())
                        stack.add(frame.toString());
                }
                var position = statement.getParserPosition();
                ObjectNode span = node.putObject("position");
                span.put("line", position.getLineNum());
                span.put("column", position.getColumnNum());
                span.put("end_line", position.getEndLineNum());
                span.put("end_column", position.getEndColumnNum());
            }
        } catch (SqlParseException error) {
            // Internal compiler failures must abort the run, never become negative goldens.
            requireSyntaxRejection(error);
            result.put("verdict", "rejected");
            result.put("error", error.getMessage());
            if (error.getCause() != null)
                result.put("cause_type", error.getCause().getClass().getName());
            if (error.getPos() != null) {
                result.put("line", error.getPos().getLineNum());
                result.put("column", error.getPos().getColumnNum());
            }
        }
        return result;
    }

    static boolean knownHintUnparseFailure(ClassCastException error) {
        String message = error.getMessage();
        if (message == null || !message.contains("org.apache.calcite.sql.SqlBasicCall")
                || !message.contains("org.apache.calcite.sql.SqlLiteral"))
            return false;
        return java.util.Arrays.stream(error.getStackTrace()).anyMatch(frame ->
                frame.getClassName().equals("org.apache.calcite.sql.SqlHint")
                        && frame.getMethodName().equals("getOptionList"));
    }

    static void requireSyntaxRejection(SqlParseException error) throws IOException {
        Throwable cause = error.getCause();
        if (cause != null && !(cause instanceof ParseException)
                && !(cause instanceof TokenMgrError) && !(cause instanceof CompilationError)
                && !expectedGrammarError(error))
            throw new IOException("Internal failure in Java parser", error);
    }

    private static boolean expectedGrammarError(SqlParseException error) {
        // Parser.jj deliberately raises resource errors for invalid datetime
        // subtraction, expressions in the wrong query/non-query contexts,
        // unsupported statement starts, and invalid ASOF match conditions.
        // normalizeException unwraps its CalciteContextException. Do not treat
        // arbitrary CalciteExceptions as syntax errors.
        Throwable cause = error.getCause();
        // SqlLiteral validates hexadecimal digits and Unicode escapes during
        // parsing, before the validator runs. Match only these resource errors.
        if (error.getPos() != null && cause != null
                && cause.getClass() == org.apache.calcite.sql.validate.SqlValidatorException.class
                && (RESOURCE.binaryLiteralInvalid().ex().getMessage().equals(cause.getMessage())
                    || RESOURCE.betweenWithoutAnd().ex().getMessage().equals(cause.getMessage())
                    || cause.getMessage().matches("Unicode escape sequence starting at character [0-9]+"
                            + " is not exactly four hex digits")))
            return true;
        return error.getPos() != null && cause != null
                && cause.getClass() == CalciteException.class
                && (RESOURCE.illegalMinusDate().ex().getMessage().equals(cause.getMessage())
                    || RESOURCE.illegalQueryExpression().ex().getMessage().equals(cause.getMessage())
                    || RESOURCE.illegalNonQueryExpression().ex().getMessage().equals(cause.getMessage())
                    || RESOURCE.expectedQueryOrJoinExpression().ex().getMessage().equals(cause.getMessage())
                    || RESOURCE.pipeClauseNotAllowed("WHERE", "|> WHERE").ex().getMessage()
                        .equals(cause.getMessage())
                    || emptyIntervalLiteral(error)
                    || integerLiteralOverflow(error)
                    || unknownCharacterSet(error)
                    || RESOURCE.invalidStatement().ex().getMessage().equals(cause.getMessage())
                    || RESOURCE.asofRequiresMatchCondition().ex().getMessage().equals(cause.getMessage())
                    || RESOURCE.matchConditionRequiresAsof().ex().getMessage().equals(cause.getMessage()));
    }

    private static boolean integerLiteralOverflow(SqlParseException error) {
        // Both generated productions parse a decimal magnitude as a Java int
        // and deliberately raise invalidLiteral on overflow. Require that
        // production and the complete resource message, not arbitrary failures.
        Throwable cause = java.util.Objects.requireNonNull(error.getCause());
        var matcher = java.util.regex.Pattern.compile("Literal '([0-9]+)' can not be parsed to type 'java\\.lang\\.Integer'")
                .matcher(cause.getMessage());
        if (!matcher.matches() || !RESOURCE.invalidLiteral(matcher.group(1), Integer.class.getCanonicalName())
                .ex().getMessage().equals(cause.getMessage()))
            return false;
        return java.util.Arrays.stream(cause.getStackTrace()).anyMatch(frame ->
                frame.getClassName().equals("org.dbsp.generated.parser.DbspParserImpl")
                        && (frame.getMethodName().equals("UnsignedIntLiteral")
                            || frame.getMethodName().equals("IntLiteral")));
    }

    private static boolean unknownCharacterSet(SqlParseException error) {
        Throwable cause = java.util.Objects.requireNonNull(error.getCause());
        var matcher = java.util.regex.Pattern.compile("Unknown character set '([^']*)'")
                .matcher(cause.getMessage());
        if (!matcher.matches() || !RESOURCE.unknownCharacterSet(matcher.group(1))
                .ex().getMessage().equals(cause.getMessage()))
            return false;
        // StringLiteral deliberately converts UnsupportedCharsetException to
        // this resource error before validation. Other producers stay fatal.
        return java.util.Arrays.stream(cause.getStackTrace()).anyMatch(frame ->
                frame.getClassName().equals("org.dbsp.generated.parser.DbspParserImpl")
                        && frame.getMethodName().equals("StringLiteral"));
    }

    private static boolean emptyIntervalLiteral(SqlParseException error) {
        // SqlParserUtil.parseIntervalLiteral rejects the empty string before
        // validation. Match the resource's complete message, including location;
        // unrelated CalciteExceptions still abort the experiment.
        for (String qualifier: List.of("YEAR", "YEAR TO MONTH", "MONTH", "DAY",
                "DAY TO HOUR", "DAY TO MINUTE", "DAY TO SECOND", "HOUR",
                "HOUR TO MINUTE", "HOUR TO SECOND", "MINUTE", "MINUTE TO SECOND", "SECOND")) {
            String expected = RESOURCE.illegalIntervalLiteral(" " + qualifier,
                    error.getPos().toString()).ex().getMessage();
            if (expected.equals(java.util.Objects.requireNonNull(error.getCause()).getMessage()))
                return true;
        }
        return false;
    }

    private static String digest(Path input) throws IOException {
        try (var stream = Files.newInputStream(input)) {
            return DigestUtils.sha256Hex(stream);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2)
            throw new IllegalArgumentException("Usage: ParserProbe INPUT.jsonl OUTPUT_DIR");
        Path inputPath = Path.of(args[0]);
        String inputDigest = digest(inputPath);
        Path directory = Path.of(args[1]);
        Files.createDirectory(directory);
        Set<String> identities = new HashSet<>();
        long acceptedCases = 0;
        long acceptedSubmissions = 0;
        long rejectedSubmissions = 0;
        long statementCount = 0;
        long unparseFailures = 0;
        try (var input = Files.newBufferedReader(inputPath);
             var output = Files.newBufferedWriter(directory.resolve("cases.jsonl"), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonNode source = MAPPER.readTree(line);
                JsonNode id = source.get("id");
                JsonNode submissions = source.get("submissions");
                if (id == null || !id.isTextual() || id.asText().isBlank()
                        || !identities.add(id.asText()) || submissions == null
                        || !submissions.isArray() || submissions.isEmpty())
                    throw new IOException("Invalid or duplicate parser case");
                CompilerOptions options = source.has("options")
                        ? MAPPER.treeToValue(source.get("options"), CompilerOptions.class)
                        : new CompilerOptions();
                DBSPCompiler compiler = new DBSPCompiler(options);
                ObjectNode result = MAPPER.createObjectNode();
                result.set("id", id);
                ArrayNode results = result.putArray("submissions");
                boolean accepted = true;
                for (JsonNode submission: submissions) {
                    ObjectNode parsed = parse(compiler, submission);
                    parsed.set("source", submission);
                    results.add(parsed);
                    if (parsed.get("verdict").asText().equals("accepted")) {
                        acceptedSubmissions++;
                        statementCount += parsed.get("statements").size();
                        for (JsonNode statement: parsed.get("statements")) {
                            if (statement.has("unparse_failure"))
                                unparseFailures++;
                        }
                    } else {
                        rejectedSubmissions++;
                        accepted = false;
                    }
                }
                result.put("accepted", accepted);
                if (accepted)
                    acceptedCases++;
                // Calcite permits identifiers containing lone UTF-16 surrogates.
                // Escape code units so the UTF-8 writer preserves that evidence
                // instead of failing or replacing the invalid scalar value.
                output.write(MAPPER.writer().with(JsonWriteFeature.ESCAPE_NON_ASCII)
                        .writeValueAsString(result));
                output.newLine();
            }
        }
        if (identities.isEmpty() || !inputDigest.equals(digest(inputPath)))
            throw new IOException("Parser input is empty or changed during measurement");
        ObjectNode complete = MAPPER.createObjectNode();
        complete.put("schema_version", 1);
        complete.put("stage", "raw_parser");
        complete.put("cases", identities.size());
        complete.put("accepted_cases", acceptedCases);
        complete.put("rejected_cases", identities.size() - acceptedCases);
        complete.put("accepted_submissions", acceptedSubmissions);
        complete.put("rejected_submissions", rejectedSubmissions);
        complete.put("statements_in_accepted_submissions", statementCount);
        complete.put("unparse_failures", unparseFailures);
        complete.put("rendering_complete", unparseFailures == 0);
        complete.put("input_sha256", inputDigest);
        complete.put("producer", System.getProperty("feldera.parser.producer", "unrecorded"));
        complete.put("calcite", System.getProperty("feldera.parser.calcite", "unrecorded"));
        complete.put("complete", true);
        Files.writeString(directory.resolve("complete.json"), complete.toPrettyString(), StandardOpenOption.CREATE_NEW);
    }
}
