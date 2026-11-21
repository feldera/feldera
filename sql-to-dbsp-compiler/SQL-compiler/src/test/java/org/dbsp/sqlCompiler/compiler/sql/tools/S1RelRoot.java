package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.runtime.PairList;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.dbsp.sqlCompiler.compiler.sql.tools.S1MetadataWire.*;

/** A statement's result projection is distinct from its relational node. */
final class S1RelRoot {
    private S1RelRoot() { }
    static ObjectNode encode(RelRoot root, Map<RelNode, Integer> relations, S1TypeTable.Encoded types) throws IOException {
        Integer relation = relations.get(root.rel); check(relation != null, "Statement root is outside relation arena");
        var wire = Utilities.deterministicObjectMapper().createObjectNode().put("relation", relation)
                .put("validated_type", types.id(root.validatedRowType)).put("kind", root.kind.name());
        var fields = wire.putArray("fields");
        root.fields.forEach((index, name) -> fields.addObject().put("index", index).put("name", name));
        wire.set("collation", S1RelationMetadata.collation(root.collation));
        // RelRoot hints have the same contract as relation hints.
        wire.set("hints", S1RelationMetadata.hints(root.hints));
        return wire;
    }
    static RelRoot decode(JsonNode wire, List<RelNode> relations, List<RelDataType> types) throws IOException {
        fields(wire, "relation", "validated_type", "kind", "fields", "collation", "hints");
        var relation = relations.get(reference(wire.path("relation"), relations.size()));
        var row = types.get(reference(wire.path("validated_type"), types.size()));
        check(row.isStruct(), "Statement validated result requires a row type");
        SqlKind kind;
        try { kind = SqlKind.valueOf(text(wire.path("kind"))); }
        catch (IllegalArgumentException error) { throw new IOException("Unknown statement root kind", error); }
        array(wire.path("fields")); var fields = PairList.<Integer, String>builder();
        for (var field: wire.path("fields")) {
            fields(field, "index", "name");
            fields.add(reference(field.path("index"), relation.getRowType().getFieldCount()), text(field.path("name")));
        }
        check(wire.path("fields").size() == row.getFieldCount(), "Statement result projection width differs");
        return new RelRoot(relation, row, kind, fields.build(),
                S1RelationMetadata.collation(wire.path("collation"), relation.getRowType().getFieldCount()),
                S1RelationMetadata.hints(wire.path("hints")));
    }
}
