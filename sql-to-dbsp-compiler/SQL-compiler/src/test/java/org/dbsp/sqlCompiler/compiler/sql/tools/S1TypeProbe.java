package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** File-boundary experiment over ordered compound/scalar type arenas. */
public final class S1TypeProbe {
    private S1TypeProbe() { }

    static List<RelDataType> roots() {
        List<RelDataType> roots = new ArrayList<>(S1TypeTableTests.roots());
        var builder = new RexBuilder(S1LiteralPayloadTests.factory());
        for (var literal: S1LiteralPayloadTests.fixtures(builder))
            roots.add(literal.getType());
        var factory = new S1TypeTable.Factory();
        for (var name: List.of(org.apache.calcite.sql.type.SqlTypeName.SYMBOL,
                org.apache.calcite.sql.type.SqlTypeName.VARIANT, org.apache.calcite.sql.type.SqlTypeName.UUID,
                org.apache.calcite.sql.type.SqlTypeName.ANY, org.apache.calcite.sql.type.SqlTypeName.UNKNOWN,
                org.apache.calcite.sql.type.SqlTypeName.CURSOR, org.apache.calcite.sql.type.SqlTypeName.COLUMN_LIST)) {
            var type = factory.createSqlType(name);
            roots.add(factory.createTypeWithNullability(type, false));
            roots.add(factory.createTypeWithNullability(type, true));
        }
        for (var name: List.of(org.apache.calcite.sql.type.SqlTypeName.TIME,
                org.apache.calcite.sql.type.SqlTypeName.TIME_TZ, org.apache.calcite.sql.type.SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE,
                org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP, org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_TZ,
                org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            var type = factory.createSqlType(name, 6);
            roots.add(factory.createTypeWithNullability(type, false));
            roots.add(factory.createTypeWithNullability(type, true));
        }
        for (var type: List.of(factory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DECIMAL, 38),
                new org.apache.calcite.sql.type.BasicSqlType(factory.getTypeSystem(), org.apache.calcite.sql.type.SqlTypeName.DECIMAL, 38),
                factory.createSqlIntervalType(new org.apache.calcite.sql.SqlIntervalQualifier(
                        org.apache.calcite.avatica.util.TimeUnit.QUARTER, null, org.apache.calcite.sql.parser.SqlParserPos.ZERO)),
                factory.createSqlIntervalType(new org.apache.calcite.sql.SqlIntervalQualifier(
                        org.apache.calcite.avatica.util.TimeUnit.WEEK, null, org.apache.calcite.sql.parser.SqlParserPos.ZERO)),
                factory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DECIMAL, 38, 0),
                factory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BINARY),
                factory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BINARY, -1),
                factory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BINARY, 1),
                factory.createJavaType(org.locationtech.jts.geom.Geometry.class))) {
            roots.add(factory.createTypeWithNullability(type, false));
            roots.add(factory.createTypeWithNullability(type, true));
        }
        return roots;
    }

    public static void main(String[] args) throws IOException {
        boolean export = args.length == 2 && args[0].equals("export");
        boolean verify = args.length == 3 && args[0].equals("verify");
        if (!export && !verify)
            throw new IllegalArgumentException("Usage: S1TypeProbe export OUTPUT_DIR | verify INPUT.json OUTPUT_DIR");
        Path output = Path.of(args[args.length - 1]);
        Files.createDirectory(output);
        var mapper = Utilities.deterministicObjectMapper();
        mapper.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        var original = roots();
        var expected = S1TypeTable.encode(original);
        if (export) {
            Files.writeString(output.resolve("types.json"), expected.toPrettyString(), StandardOpenOption.CREATE_NEW);
        } else {
            Path input = Path.of(args[1]);
            if (Files.size(input) > 16 * 1024 * 1024)
                throw new IOException("Type arena exceeds 16 MiB");
            byte[] bytes = Files.readAllBytes(input);
            if (bytes.length > 16 * 1024 * 1024)
                throw new IOException("Type arena grew beyond 16 MiB");
            var actual = mapper.readTree(bytes);
            if (!actual.equals(mapper.readTree(expected.toString())))
                throw new IOException("Type arena changed across transport");
            var decoded = S1TypeTable.decode(actual);
            for (int index = 0; index < original.size(); index++) {
                var before = original.get(index);
                var after = decoded.roots().get(index);
                if (!before.equals(after) || !before.getFullTypeString().equals(after.getFullTypeString()))
                    throw new IOException("Root type/digest changed: " + index);
                for (int other = 0; other < original.size(); other++) {
                    if ((before == original.get(other)) != (after == decoded.roots().get(other)))
                        throw new IOException("Root type identity changed: " + index + "/" + other);
                }
            }
            if (!mapper.readTree(S1TypeTable.encodeArena(decoded.types(), decoded.roots()).toString()).equals(actual))
                throw new IOException("Imported type arena changed identity, order or fields");
        }
        var complete = mapper.createObjectNode();
        complete.put("schema_version", 1);
        complete.put("complete", true);
        complete.put("types", expected.path("types").size());
        complete.put("roots", original.size());
        complete.put("mode", args[0]);
        complete.put("SQL_types_imported", verify);
        complete.put("runtime_executed", false);
        Files.writeString(output.resolve("complete.json"), complete.toPrettyString(), StandardOpenOption.CREATE_NEW);
    }
}
