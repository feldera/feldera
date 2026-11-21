package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** Strict primitives shared by the typed statement and catalog adapters. */
final class S1MetadataWire {
    private S1MetadataWire() { }
    static void check(boolean condition, String message) throws IOException {
        if (!condition) throw new IOException(message);
    }
    static void fields(JsonNode node, String... names) throws IOException {
        check(node.isObject(), "Missing metadata object");
        var actual = new HashSet<String>(); node.fieldNames().forEachRemaining(actual::add);
        check(actual.equals(Set.of(names)), "Unknown or missing metadata fields");
    }
    static String text(JsonNode node) throws IOException {
        check(node.isTextual() && node.textValue().length() <= 1_048_576, "Invalid or oversized metadata string");
        return node.textValue();
    }
    static boolean bool(JsonNode node) throws IOException {
        check(node.isBoolean(), "Invalid metadata boolean"); return node.booleanValue();
    }
    static int unsigned(JsonNode node) throws IOException {
        check(node.isIntegralNumber() && node.canConvertToInt() && node.intValue() >= 0, "Invalid metadata index");
        return node.intValue();
    }
    static int reference(JsonNode node, int count) throws IOException {
        int id = unsigned(node); check(id < count, "Metadata reference outside arena"); return id;
    }
    static void array(JsonNode node) throws IOException {
        check(node.isArray() && node.size() <= 100_000, "Invalid or oversized metadata array");
    }
    static ObjectNode name(ProgramIdentifier identifier) {
        return Utilities.deterministicObjectMapper().createObjectNode()
                .put("name", identifier.name()).put("quoted", identifier.isQuoted());
    }
    static ProgramIdentifier name(JsonNode wire) throws IOException {
        fields(wire, "name", "quoted");
        return new ProgramIdentifier(text(wire.path("name")), bool(wire.path("quoted")));
    }
}
