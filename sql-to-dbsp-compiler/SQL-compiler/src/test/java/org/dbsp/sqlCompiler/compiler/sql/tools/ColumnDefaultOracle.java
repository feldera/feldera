package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CreateRuntimeErrorWrappers;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EliminateStructs;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUuid;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Record both sides of the same transformation that the Java column-default printer calls. */
public final class ColumnDefaultOracle {
    private ColumnDefaultOracle() {}

    private static JsonNode encode(DBSPCompiler compiler, IDBSPInnerNode value) throws IOException {
        var stream = new JsonStream(new IndentStream(new StringBuilder()));
        value.accept(new ToJsonInnerVisitor(compiler, stream, 1, true));
        return Utilities.deterministicObjectMapper().readTree(stream.toString());
    }

    public static void main(String[] args) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        if (args.length == 1 && args[0].equals("--stdin")) {
            var compiler = DBSPCompiler.forCircuitReplay(new CompilerOptions());
            try (var reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    var entry = mapper.readTree(line);
                    DBSPVariablePath.reset();
                    var result = mapper.createObjectNode();
                    result.set("id", entry.required("id"));
                    var decoder = new JsonDecoder();
                    IDBSPInnerNode before;
                    IDBSPInnerNode after;
                    if (entry.path("kind").asText().equals("struct")) {
                        var row = decoder.decodeInner(entry.required("before"), DBSPTypeStruct.class);
                        before = row;
                        after = new EliminateStructs(compiler).apply(row);
                        result.put("kind", "struct");
                    } else {
                        var value = decoder.decodeInner(entry.required("before"), DBSPExpression.class);
                        before = value;
                        after = CreateRuntimeErrorWrappers.wrapCasts(compiler, value);
                    }
                    result.set("before", encode(compiler, before));
                    result.set("after", encode(compiler, after));
                    result.put("next_name", DBSPVariablePath.nextGeneratedName());
                    System.out.println(mapper.writeValueAsString(result));
                }
            }
            return;
        }
        var cases = mapper.createObjectNode();
        var node = CalciteObject.EMPTY;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var bigint = new DBSPTypeInteger(node, 64, true, false);
        if (args.length == 1 && args[0].equals("--siblings")) {
            var compiler = DBSPCompiler.forCircuitReplay(new CompilerOptions());
            for (int depth : new int[] {1, 2}) {
                DBSPType from = integer;
                DBSPType to = bigint;
                for (int level = 0; level < depth; level++) {
                    from = new DBSPTypeArray(from, false);
                    to = new DBSPTypeArray(to, false);
                }
                for (boolean safeFirst : new boolean[] {false, true}) {
                    DBSPVariablePath.reset();
                    var input = new DBSPVariablePath("value", from);
                    var safe = input.cast(node, to.withMayBeNull(true), DBSPCastExpression.CastType.SqlSafe);
                    var unsafe = input.cast(node, to, DBSPCastExpression.CastType.SqlUnsafe);
                    var tuple = safeFirst ? new DBSPRawTupleExpression(safe, unsafe, safe)
                            : new DBSPRawTupleExpression(unsafe, safe, unsafe);
                    var entry = cases.putObject("depth-" + depth + (safeFirst ? "-safe-first" : "-unsafe-first"));
                    entry.set("before", encode(compiler, tuple));
                    entry.set("after", encode(compiler, CreateRuntimeErrorWrappers.wrapCasts(compiler, tuple)));
                    entry.put("next_name", DBSPVariablePath.nextGeneratedName());
                }
            }
            System.out.println(mapper.writeValueAsString(cases));
            return;
        }
        if (args.length == 1 && args[0].equals("--literals")) {
            var compiler = DBSPCompiler.forCircuitReplay(new CompilerOptions());
            record Literal(String name, DBSPExpression value, DBSPType target) {}
            var unsigned = new DBSPTypeInteger(node, 8, false, false);
            var decimal = new DBSPTypeDecimal(node, 6, 2, false);
            for (var test : new Literal[] {
                    new Literal("integer-from-text", new DBSPStringLiteral("  +42 "), integer),
                    new Literal("integer-narrow", new DBSPI32Literal(42), unsigned),
                    new Literal("integer-overflow", new DBSPI32Literal(256), unsigned),
                    new Literal("integer-widen", new DBSPI32Literal(-7), bigint),
                    new Literal("unsigned-negative-zero", new DBSPStringLiteral("-0"), unsigned),
                    new Literal("non-ascii-integer", new DBSPStringLiteral("１２"), integer),
                    new Literal("boolean", new DBSPStringLiteral(" tRuE "), DBSPTypeBool.create(false)),
                    new Literal("date", new DBSPStringLiteral("2024-02-29"), DBSPTypeDate.create(false)),
                    new Literal("time", new DBSPStringLiteral("12:03:04.250000"), DBSPTypeTime.create(false)),
                    new Literal("timestamp", new DBSPStringLiteral("2020-01-01 01:02:03.010000"),
                            DBSPTypeTimestamp.create(false)),
                    new Literal("decimal-round-up", new DBSPStringLiteral("12.355"), decimal),
                    new Literal("decimal-round-down", new DBSPStringLiteral("-12.345"), decimal),
                    new Literal("decimal-overflow", new DBSPStringLiteral("9999.999"), decimal),
                    new Literal("decimal-from-integer", new DBSPI32Literal(-7), decimal),
                    new Literal("early-date", new DBSPStringLiteral("1500-02-25"), DBSPTypeDate.create(false)),
                    new Literal("early-timestamp", new DBSPStringLiteral("1500-02-25 01:02:03"),
                            DBSPTypeTimestamp.create(false)),
                    new Literal("time-precision", new DBSPStringLiteral("12:03:04.123456789012"), DBSPTypeTime.create(false)),
                    new Literal("timestamp-precision", new DBSPStringLiteral("2001-02-03 12:03:04.1234567"),
                            DBSPTypeTimestamp.create(false)),
                    new Literal("invalid-time", new DBSPStringLiteral("12:03:60"), DBSPTypeTime.create(false)),
                    new Literal("zero-decimal", new DBSPStringLiteral("0E-100"),
                            new DBSPTypeDecimal(node, 12, 9, false)),
                    new Literal("decimal-scientific", new DBSPStringLiteral("1.25E-7"),
                            new DBSPTypeDecimal(node, 12, 9, false)),
                    new Literal("uuid-compact", new DBSPStringLiteral("123456781234456780000123456789AB"),
                            DBSPTypeUuid.create(false)),
                    new Literal("uuid-braces", new DBSPStringLiteral("{1234-5678-1234-4567-8000-0123-4567-89AB}"),
                            DBSPTypeUuid.create(false)),
                    new Literal("uuid", new DBSPStringLiteral("12345678-1234-4567-8000-0123456789AB"),
                            DBSPTypeUuid.create(false)),
            }) {
                for (boolean safe : new boolean[] {false, true}) {
                    DBSPVariablePath.reset();
                    var cast = test.value().cast(node, test.target().withMayBeNull(safe),
                            safe ? DBSPCastExpression.CastType.SqlSafe : DBSPCastExpression.CastType.SqlUnsafe);
                    var entry = cases.putObject(test.name() + (safe ? "-safe" : "-unsafe"));
                    entry.set("before", encode(compiler, cast));
                    entry.set("after", encode(compiler, CreateRuntimeErrorWrappers.wrapCasts(compiler, cast)));
                    entry.put("next_name", DBSPVariablePath.nextGeneratedName());
                }
            }
            System.out.println(mapper.writeValueAsString(cases));
            return;
        }
        var recordType = new DBSPTypeStruct(node, new ProgramIdentifier("record_t"), List.of(
                new DBSPTypeStruct.Field(node, new ProgramIdentifier("x"), 0, integer),
                new DBSPTypeStruct.Field(node, new ProgramIdentifier("Quoted Field", true), 1, bigint)), false);
        record Example(String name, DBSPType from, DBSPType to) {}
        for (var example : new Example[] {
                new Example("integer", integer, bigint),
                new Example("array", new DBSPTypeArray(integer, false), new DBSPTypeArray(bigint, false)),
                new Example("map", new DBSPTypeMap(integer, bigint, false), new DBSPTypeMap(bigint, integer, false)),
                new Example("nullable-map", new DBSPTypeMap(integer, bigint, true),
                        new DBSPTypeMap(bigint, integer, true)),
                new Example("row", new DBSPTypeTuple(integer, bigint), new DBSPTypeTuple(bigint, integer)),
                new Example("nullable-row", new DBSPTypeTuple(integer, bigint).withMayBeNull(true),
                        new DBSPTypeTuple(bigint, integer)),
                new Example("nested-array", new DBSPTypeArray(new DBSPTypeArray(integer, true), false),
                        new DBSPTypeArray(new DBSPTypeArray(bigint, true), false)),
                new Example("variant-to-array", DBSPTypeVariant.create(false), new DBSPTypeArray(integer, false)),
                new Example("array-to-variant", new DBSPTypeArray(integer, false), DBSPTypeVariant.create(false)),
                new Example("variant-to-row", DBSPTypeVariant.create(false), new DBSPTypeTuple(integer, bigint)),
                new Example("variant-to-record", DBSPTypeVariant.create(true), recordType.toTuple()),
                new Example("record-to-variant", recordType.toTuple(), DBSPTypeVariant.create(false)),
                new Example("variant-to-nested-array", DBSPTypeVariant.create(false),
                        new DBSPTypeArray(new DBSPTypeArray(bigint, true), false)),
        }) {
            for (boolean safe : new boolean[] {false, true}) {
                var compiler = new DBSPCompiler(new CompilerOptions());
                DBSPVariablePath.reset();
                var source = new DBSPVariablePath("value", example.from());
                var cast = source.cast(node, example.to().withMayBeNull(safe),
                        safe ? DBSPCastExpression.CastType.SqlSafe : DBSPCastExpression.CastType.SqlUnsafe);
                var entry = cases.putObject(example.name() + (safe ? "-safe" : "-unsafe"));
                entry.set("before", encode(compiler, cast));
                try {
                    entry.set("after", encode(compiler, CreateRuntimeErrorWrappers.wrapCasts(compiler, cast)));
                } catch (InternalCompilerError error) {
                    entry.put("java_error", error.getMessage().lines().findFirst().orElseThrow());
                }
                entry.put("next_name", DBSPVariablePath.nextGeneratedName());
            }
        }
        System.out.println(mapper.writeValueAsString(cases));
    }
}
