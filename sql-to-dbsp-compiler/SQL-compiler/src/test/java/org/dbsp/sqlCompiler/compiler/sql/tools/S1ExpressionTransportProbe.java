package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.FelderaS1ExpressionFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Cross-language regression graphs for the contextual expression codec. These
 * constructor fixtures do not certify corpus coverage or whole-program S1. */
public final class S1ExpressionTransportProbe {
    private S1ExpressionTransportProbe() { }

    record Sample(String name, RelDataType input, List<RexNode> roots, S1ExpressionSources.Source source) {
        Sample(String name, RelDataType input, List<RexNode> roots) { this(name, input, roots, null); }
    }

    static List<Sample> samples(S1BuiltinOperatorRegistry registry) throws IOException {
        var factory = new S1TypeTable.Factory();
        var builder = new RexBuilder(factory);
        var integer = factory.createSqlType(SqlTypeName.INTEGER);
        var row = new RelRecordType(StructKind.FULLY_QUALIFIED,
                List.of(new RelDataTypeFieldImpl("X", 0, integer)), false);
        RexNode input = builder.makeInputRef(integer, 0);
        RexNode literal = builder.makeExactLiteral(BigDecimal.ONE, integer);
        RexNode call = builder.makeCall(integer, SqlStdOperatorTable.PLUS, List.of(input, literal));
        RexNode correlation = builder.makeCorrel(row, new CorrelationId(99));
        var declaration = new RexLambdaRef(0, "item", integer);
        var reference = new RexLambdaRef(0, "item", integer);
        RexNode lambda = builder.makeLambdaCall(reference, List.of(declaration));
        var samples = new ArrayList<Sample>();
        samples.add(new Sample("seven-forms", row, List.of(call, builder.makeFieldAccess(correlation, 0), lambda, call)));
        samples.add(new Sample("distinct-input-objects", row,
                List.of(input, builder.makeInputRef(integer, 0), input)));
        RexNode closedCall = builder.makeCall(integer, SqlStdOperatorTable.PLUS, List.of(literal, literal));
        RexNode closedLambda = builder.makeLambdaCall(closedCall, List.of(declaration));
        samples.add(new Sample("shared-closed-call", row, List.of(closedLambda, closedCall, literal)));
        var outer = new RexLambdaRef(0, "outer", integer);
        var inner = new RexLambdaRef(0, "inner", integer);
        RexNode capture = new RexLambdaRef(0, "outer", integer);
        RexNode innerBody = builder.makeCall(integer, SqlStdOperatorTable.PLUS, List.of(capture, inner));
        RexNode innerLambda = builder.makeLambdaCall(innerBody, List.of(inner));
        samples.add(new Sample("nested-lexical-capture", row,
                List.of(builder.makeLambdaCall(innerLambda, List.of(outer)))));
        // Equal but distinct type objects and expression objects must both survive.
        var equalInteger = factory.createSqlType(SqlTypeName.INTEGER);
        RexNode equalLiteral = builder.makeExactLiteral(BigDecimal.ONE, equalInteger);
        samples.add(new Sample("equal-distinct-types", row, List.of(literal, equalLiteral)));
        var customAbs = registry.entries().entrySet().stream()
                .filter(entry -> entry.getKey().registry().equals(S1BuiltinOperatorRegistry.CUSTOM_NAME)
                        && entry.getValue().getName().equals("abs"))
                .findFirst().orElseThrow(() -> new IOException("Missing Feldera ABS builtin"));
        samples.add(new Sample("custom-and-calcite-abs", row, List.of(
                FelderaS1ExpressionFactory.call(integer, customAbs.getValue(), List.of(input)),
                FelderaS1ExpressionFactory.call(integer, SqlStdOperatorTable.ABS, List.of(input)))));
        RexNode sourceCall = builder.makeCall(new org.apache.calcite.sql.parser.SqlParserPos(1, 4, 2, 5),
                integer, SqlStdOperatorTable.PLUS, List.of(input, literal));
        samples.add(new Sample("source-bearing-call", row, List.of(sourceCall),
                new S1ExpressionSources.Source("query.sql", "😀 + X\nX + 1")));
        RexNode generatedCall = builder.makeCall(new org.apache.calcite.sql.parser.SqlParserPos(0, 0, 2, 31),
                integer, SqlStdOperatorTable.PLUS, List.of(input, literal));
        samples.add(new Sample("generated-call-position", row, List.of(generatedCall),
                new S1ExpressionSources.Source("query.sql", "CREATE TABLE T(r INT, s INT);\nCREATE VIEW V AS SELECT r <=> s AS eq FROM T;")));
        var character = factory.createSqlType(SqlTypeName.VARCHAR, 20);
        var characterRow = new RelRecordType(StructKind.FULLY_QUALIFIED,
                List.of(new RelDataTypeFieldImpl("X", 0, character)), false);
        RexNode characterInput = new org.apache.calcite.rex.RexInputRef(0, character);
        samples.add(new Sample("character-input-reference", characterRow, List.of(characterInput, characterInput)));
        var zero = org.apache.calcite.sql.parser.SqlParserPos.ZERO;
        var constructorName = new org.apache.calcite.sql.SqlIdentifier(List.of("public", "Pair"), null,
                zero.withQuoting(true), List.of(zero, zero.withQuoting(true)));
        var firstConstructor = new org.apache.calcite.sql.SqlTypeConstructorFunction(constructorName, row);
        var equalConstructor = new org.apache.calcite.sql.SqlTypeConstructorFunction(constructorName, row);
        RexNode firstCall = FelderaS1ExpressionFactory.call(row, firstConstructor, List.of(input));
        RexNode sharedOperatorCall = FelderaS1ExpressionFactory.call(row, firstConstructor, List.of(input));
        RexNode equalOperatorCall = FelderaS1ExpressionFactory.call(row, equalConstructor, List.of(input));
        samples.add(new Sample("constructor-object-sharing", row, List.of(firstCall, sharedOperatorCall, equalOperatorCall, firstCall)));
        var pair = new RelRecordType(StructKind.FULLY_QUALIFIED,
                List.of(new RelDataTypeFieldImpl("left", 0, integer), new RelDataTypeFieldImpl("EXPR$1", 1, integer)), false);
        var namedRow = new org.apache.calcite.sql.fun.SqlRowOperator("ROW", java.util.Arrays.asList("left", null));
        var defaultRow = new org.apache.calcite.sql.fun.SqlRowOperator("ROW");
        samples.add(new Sample("row-constructor-fields", row, List.of(
                FelderaS1ExpressionFactory.call(pair, namedRow, List.of(input, literal)),
                FelderaS1ExpressionFactory.call(pair, defaultRow, List.of(input, literal)),
                FelderaS1ExpressionFactory.call(pair, SqlStdOperatorTable.ROW, List.of(input, literal)))));
        var parameter = new org.apache.calcite.rel.type.RelDataTypeFieldImpl("p", 0, integer);
        RexNode firstParameter = new org.apache.calcite.rex.RexInputRef(0, integer);
        RexNode functionBody = FelderaS1ExpressionFactory.call(integer, SqlStdOperatorTable.PLUS, List.of(firstParameter, literal));
        var function = new org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction(
                new org.apache.calcite.sql.SqlIdentifier("f", zero), integer, List.of(parameter), functionBody, false);
        RexNode secondParameter = new org.apache.calcite.rex.RexInputRef(0, integer);
        var dependent = new org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction(
                new org.apache.calcite.sql.SqlIdentifier("g", zero), integer, List.of(parameter),
                FelderaS1ExpressionFactory.call(integer, function, List.of(secondParameter)), false);
        samples.add(new Sample("shared-sql-functions", row, List.of(
                FelderaS1ExpressionFactory.call(integer, function, List.of(input)),
                FelderaS1ExpressionFactory.call(integer, dependent, List.of(input)), literal)));
        var external = new org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction(
                new org.apache.calcite.sql.SqlIdentifier("external", zero), integer, List.of(parameter), null, true);
        samples.add(new Sample("external-function", row, List.of(FelderaS1ExpressionFactory.call(integer, external, List.of(input)))));
        var unicode = builder.makeLiteral("🐍");
        var varchar = factory.createSqlType(SqlTypeName.VARCHAR);
        samples.add(new Sample("unicode-wrapper", row, List.of(FelderaS1ExpressionFactory.call(varchar,
                org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.NonBmpLiterals.Wrapper.INSTANCE, List.of(unicode)))));
        var window = new org.apache.calcite.rel.core.Window.RexWinAggCall(
                new org.apache.calcite.sql.parser.SqlParserPos(1, 1, 1, 6),
                SqlStdOperatorTable.SUM, integer, List.of(input), 7, true, true);
        var rowNumber = new org.apache.calcite.rel.core.Window.RexWinAggCall(
                SqlStdOperatorTable.ROW_NUMBER, integer, List.of(), 8, false, false);
        samples.add(new Sample("window-aggregate", row, List.of(window, rowNumber, window),
                new S1ExpressionSources.Source("window.sql", "SUM(X)")));
        return List.copyOf(samples);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3)
            throw new IOException("usage: S1ExpressionTransportProbe export OUTPUT | verify INPUT OUTPUT");
        boolean exporting = args[0].equals("export") && args.length == 2;
        if (!exporting && !(args[0].equals("verify") && args.length == 3))
            throw new IOException("Unknown expression transport mode");
        Path output = Path.of(args[args.length - 1]); Files.createDirectory(output);
        var registry = new S1BuiltinOperatorRegistry();
        var mapper = Utilities.deterministicObjectMapper();
        int expressions = 0, roots = 0;
        for (Sample sample: samples(registry)) {
            var wire = S1ExpressionEncoder.encode(sample.roots(), sample.input(), registry, sample.source());
            if (exporting) {
                mapper.writerWithDefaultPrettyPrinter().writeValue(output.resolve(sample.name() + ".json").toFile(), wire);
            } else {
                Path input = Path.of(args[1]).resolve(sample.name()).resolve("expressions.json");
                var imported = S1ExpressionTable.read(input, registry);
                if (!mapper.readTree(input.toFile()).equals(mapper.readTree(wire.toString())))
                    throw new IOException("Expression transport changed " + sample.name());
                if (imported.roots().size() != sample.roots().size()) throw new IOException("Root count changed");
            }
            expressions += wire.path("expressions").size(); roots += sample.roots().size();
        }
        var complete = mapper.createObjectNode();
        complete.put("schema_version", 1).put("complete", true).put("fixtures", samples(registry).size())
                .put("expressions", expressions).put("roots", roots).put("constructor_fixtures_only", true)
                .put("runtime_executed", false).put("S1_parity_proven", false);
        mapper.writerWithDefaultPrettyPrinter().writeValue(output.resolve("complete.json").toFile(), complete);
    }
}
