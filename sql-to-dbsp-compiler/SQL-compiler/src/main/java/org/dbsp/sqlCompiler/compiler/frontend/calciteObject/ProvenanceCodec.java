package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ViewOrigins;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

/** Internal seam codec for provenance. Its two identity spaces preserve both
 * relational references and sharing of composition nodes. Public dataflow JSON
 * remains the responsibility of {@link CalciteRelNode#asJson}. */
public final class ProvenanceCodec {
    private ProvenanceCodec() { }

    private static ObjectNode object() {
        return JsonNodeFactory.instance.objectNode();
    }

    public static final class Encoder {
        private final DBSPCompiler compiler;
        private final Map<RelNode, Integer> planIds;
        private final Map<CalciteRelNode, Integer> nodes = new IdentityHashMap<>();
        private final Map<RelReference, String> renderedSql;
        private final Map<RelReference, Integer> references = new HashMap<>();

        public Encoder(DBSPCompiler compiler, Map<RelNode, Integer> planIds, Map<RelReference, String> renderedSql) {
            this.compiler = compiler;
            this.planIds = planIds;
            this.renderedSql = renderedSql;
        }

        private ObjectNode reference(RelReference reference) {
            ObjectNode result = object();
            Integer existing = this.references.get(reference);
            if (existing != null)
                return result.put("ref", existing);
            int id = this.references.size();
            this.references.put(reference, id);
            result.put("id", id);
            result.put("relId", reference.id());
            int planId = reference instanceof RelReference.Live live
                    ? this.planIds.getOrDefault(live.node(), -1)
                    : ((RelReference.Snapshot) reference).planId();
            result.put("planId", planId);
            result.put("tableScan", reference.isTableScan());
            result.put("digest", reference.digest());
            result.put("text", reference.text());
            result.put("sql", this.renderedSql.computeIfAbsent(reference, RelReference::sql));
            result.put("statefulSqlName", reference.statefulSqlName());
            ViewOrigins.ViewSourcePosition origin = reference.origin(this.compiler);
            if (origin != null) {
                ObjectNode savedOrigin = result.putObject("origin");
                savedOrigin.put("name", origin.view().name());
                savedOrigin.put("caseSensitive", origin.view().isQuoted());
                savedOrigin.set("position", origin.position().asJson());
            }
            return result;
        }

        public ObjectNode encode(CalciteRelNode node) {
            ObjectNode result = object();
            Integer existing = this.nodes.get(node);
            if (existing != null)
                return result.put("ref", existing);
            int id = this.nodes.size();
            this.nodes.put(node, id);
            result.put("id", id);
            result.put("kind", node.getClass().getSimpleName());
            result.set("position", node.getPositionRange().asJson());
            ArrayNode positions = result.putArray("positions");
            for (SourcePositionRange position: node.positions)
                positions.add(position.asJson());
            if (node instanceof LastRel last) {
                result.set("relation", this.reference(last.relNode));
            } else if (node instanceof IntermediateRel partial) {
                result.set("relation", this.reference(partial.relNode));
            } else if (node instanceof RelSequence sequence) {
                ArrayNode children = result.putArray("children");
                for (CalciteRelNode child: sequence.nodes)
                    children.add(this.encode(child));
            } else if (node instanceof RelAnd and) {
                ArrayNode children = result.putArray("children");
                for (LastRel child: and.sort())
                    children.add(this.encode(child));
            } else if (node instanceof PositionOnlyRel) {
                result.put("empty", node.isEmpty());
            } else if (!(node instanceof CalciteEmptyRel)) {
                throw new IllegalArgumentException("Unsupported provenance " + node.getClass().getSimpleName());
            }
            return result;
        }
    }

    public static final class Decoder {
        private final Map<Integer, CalciteRelNode> nodes = new HashMap<>();
        private final Map<Integer, RelReference> references = new HashMap<>();

        private RelReference reference(JsonNode json) {
            if (json.has("ref"))
                return Utilities.getExists(this.references, Utilities.getIntProperty(json, "ref"));
            int id = Utilities.getIntProperty(json, "id");
            ViewOrigins.ViewSourcePosition origin = null;
            if (json.has("origin")) {
                JsonNode saved = json.get("origin");
                origin = new ViewOrigins.ViewSourcePosition(new ProgramIdentifier(
                        Utilities.getStringProperty(saved, "name"),
                        Utilities.getBooleanProperty(saved, "caseSensitive")),
                        SourcePositionRange.fromJson(Utilities.getProperty(saved, "position")));
            }
            JsonNode sqlName = Utilities.getProperty(json, "statefulSqlName");
            RelReference result = new RelReference.Snapshot(
                    Utilities.getIntProperty(json, "relId"), Utilities.getIntProperty(json, "planId"),
                    Utilities.getBooleanProperty(json, "tableScan"),
                    Utilities.getStringProperty(json, "digest"), Utilities.getStringProperty(json, "text"),
                    Utilities.getStringProperty(json, "sql"), sqlName.isNull() ? null : sqlName.textValue(), origin);
            Utilities.putNew(this.references, id, result);
            return result;
        }

        public CalciteRelNode decode(JsonNode json) {
            if (json.has("ref"))
                return Utilities.getExists(this.nodes, Utilities.getIntProperty(json, "ref"));
            int id = Utilities.getIntProperty(json, "id");
            SourcePositionRange position = SourcePositionRange.fromJson(Utilities.getProperty(json, "position"));
            var positions = new ArrayList<SourcePositionRange>();
            JsonNode ranges = Utilities.getProperty(json, "positions");
            if (!ranges.isArray())
                throw new IllegalArgumentException("Provenance positions must be an array");
            for (JsonNode range: ranges)
                positions.add(SourcePositionRange.fromJson(range));
            String kind = Utilities.getStringProperty(json, "kind");
            CalciteRelNode result = switch (kind) {
                case "CalciteEmptyRel" -> CalciteEmptyRel.INSTANCE;
                case "PositionOnlyRel" -> new PositionOnlyRel(position, positions,
                        Utilities.getBooleanProperty(json, "empty"));
                case "LastRel" -> new LastRel(this.reference(Utilities.getProperty(json, "relation")), position);
                case "IntermediateRel" -> new IntermediateRel(
                        this.reference(Utilities.getProperty(json, "relation")), position);
                case "RelAnd", "RelSequence" -> {
                    JsonNode children = Utilities.getProperty(json, "children");
                    if (!children.isArray())
                        throw new IllegalArgumentException("Provenance children must be an array");
                    var decoded = new ArrayList<CalciteRelNode>();
                    for (JsonNode child: children)
                        decoded.add(this.decode(child));
                    if (kind.equals("RelSequence"))
                        yield new RelSequence(decoded);
                    RelAnd and = new RelAnd(true);
                    for (CalciteRelNode child: decoded)
                        and.add(child.to(LastRel.class));
                    yield and;
                }
                default -> throw new IllegalArgumentException("Unsupported provenance " + kind);
            };
            if (result instanceof CalciteEmptyRel) {
                if (position.isValid() || !positions.isEmpty())
                    throw new IllegalArgumentException("Empty provenance cannot carry positions");
            } else {
                // Restore the stored list exactly, without constructors duplicating a primary range.
                result.positions.clear();
                result.positions.addAll(positions);
            }
            Utilities.putNew(this.nodes, id, result);
            return result;
        }
    }
}
