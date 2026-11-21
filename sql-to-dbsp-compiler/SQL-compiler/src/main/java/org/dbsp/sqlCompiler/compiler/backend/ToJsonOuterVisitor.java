package org.dbsp.sqlCompiler.compiler.backend;

import javax.annotation.Nullable;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.ProvenanceCodec;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.RelReference;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRankOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWeightValidatorOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.IJson;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.NullIndentStream;
import org.apache.calcite.rel.RelNode;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.dbsp.sqlCompiler.circuit.operator.DBSPInputMapWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryDistinctOperator;

/** Serializes an outer node as a JSON string */
public class ToJsonOuterVisitor extends CircuitVisitor {
    @Nullable private ProvenanceCodec.Encoder provenance;
    private boolean retainProgramMetadata;
    public final JsonStream stream;
    final int verbosity;
    final Set<Long> serialized;
    protected final ToJsonInnerVisitor innerVisitor;

    public ToJsonOuterVisitor(DBSPCompiler compiler, int verbosity, ToJsonInnerVisitor innerVisitor) {
        super(compiler);
        this.stream = innerVisitor.stream;
        this.verbosity = verbosity;
        this.serialized = new HashSet<>();
        this.innerVisitor = innerVisitor;
    }

    public static ToJsonOuterVisitor create(DBSPCompiler compiler, int verbosity) {
        return create(compiler, verbosity, false);
    }

    /** Internal seam encoding. Do not use this for Gen-2 payloads or Merkle hashes. */
    public static ToJsonOuterVisitor createForReplay(DBSPCompiler compiler) {
        return createForReplay(compiler, new HashMap<>());
    }

    /** Calcite allocates correlation names while printing SQL. A series of snapshots must
     * reuse the rendering of each relation to preserve its provenance between passes. */
    public static ToJsonOuterVisitor createForReplay(DBSPCompiler compiler, Map<RelReference, String> renderedSql) {
        return createForReplay(compiler, compiler.getPlans(new NullIndentStream()), renderedSql);
    }

    static ToJsonOuterVisitor createForReplay(DBSPCompiler compiler, Map<RelNode, Integer> plans,
                                               Map<RelReference, String> renderedSql) {
        ToJsonOuterVisitor writer = create(compiler, 1, true);
        writer.provenance = new ProvenanceCodec.Encoder(compiler, plans, renderedSql);
        return writer;
    }

    /** The front end has finished building the SQL plans before this optimizer starts.
     * Keep their numbering and SQL rendering for the lifetime of that one optimization.
     * Each graph still has its own inner, outer, and provenance identity tables. */
    public static final class OptimizerSnapshots {
        private final DBSPCompiler compiler;
        private final Map<RelReference, String> renderedSql = new HashMap<>();
        @Nullable private Map<RelNode, Integer> plans;

        public OptimizerSnapshots(DBSPCompiler compiler) {
            this.compiler = compiler;
        }

        public ToJsonOuterVisitor writer() {
            if (this.plans == null)
                this.plans = this.compiler.getPlans(new NullIndentStream());
            var inner = new ToJsonInnerVisitor(this.compiler, JsonStream.compactCircuit(), 1, true);
            var writer = new ToJsonOuterVisitor(this.compiler, 1, inner);
            writer.retainProgramMetadata = true;
            writer.innerVisitor.liveShuffles = new HashMap<>();
            writer.provenance = new ProvenanceCodec.Encoder(this.compiler, this.plans, this.renderedSql);
            return writer;
        }
    }

    private static ToJsonOuterVisitor create(DBSPCompiler compiler, int verbosity, boolean replayMetadata) {
        IndentStream stream = new IndentStream(new StringBuilder());
        stream.setIndentAmount(1);
        JsonStream json = new JsonStream(stream, replayMetadata);
        ToJsonInnerVisitor inner = new ToJsonInnerVisitor(compiler, json, 1, replayMetadata);
        return new ToJsonOuterVisitor(compiler, verbosity, inner);
    }

    final List<Integer> propertyIndexStack = new ArrayList<>();

    int propertyIndex = 0;

    @Override
    public void push(IDBSPOuterNode node) {
        this.propertyIndexStack.add(this.propertyIndex);
        this.propertyIndex = 0;
        super.push(node);
    }

    @Override
    public void pop(IDBSPOuterNode node) {
        this.propertyIndex = Utilities.removeLast(this.propertyIndexStack);
        super.pop(node);
    }

    @Override
    public void endArrayProperty(String property) {
        this.stream.endArray();
    }

    @Override
    public void startArrayProperty(String property) {
        this.label(property);
        this.stream.beginArray();
    }

    @Override
    public void label(String name) {
        this.property(name);
    }

    public void property(String name) {
        this.stream.label(name);
    }

    @SuppressWarnings("SameParameterValue")
    boolean checkDone(IDBSPOuterNode node, boolean silent) {
        if (this.serialized.contains(node.getId())) {
            if (silent)
                return true;
            this.stream.beginObject()
                    .label("node")
                    .append(node.getId())
                    .endObject();
            return true;
        }
        return false;
    }

    @Override
    public VisitDecision preorder(IDBSPOuterNode node) {
        if (!this.checkDone(node, false)) {
            this.stream.beginObject().appendClass(node);
            this.property("id");
            this.stream.append(node.getId());
            this.serialized.add(node.getId());
            return VisitDecision.CONTINUE;
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPOperator operator) {
        if (this.preorder(operator.to(IDBSPOuterNode.class)).stop())
            return VisitDecision.STOP;
        if (this.provenance != null) {
            this.label("provenance");
            IJson.toJsonStream(this.provenance.encode(operator.getRelNode()), this.stream);
        } else if (this.innerVisitor.replayMetadata) {
            this.stream.label("sourceEmpty").append(operator.getRelNode().isEmpty());
            this.label("sourcePosition");
            IJson.toJsonStream(operator.getNode().getPositionRange().asJson(), this.stream);
            this.label("sourcePositions");
            this.stream.beginArray();
            for (var position: operator.getSourcePositions())
                IJson.toJsonStream(position.asJson(), this.stream);
            this.stream.endArray();
        }
        this.label("annotations");
        operator.annotations.asJson(this.stream, this.innerVisitor.replayMetadata);
        this.label("inputs");
        this.stream.beginArray();
        for (OutputPort port: operator.inputs) {
            port.asJson(this);
        }
        this.stream.endArray();
        operator.accept(this.innerVisitor);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPNestedOperator operator) {
        this.label("internalOutputs");
        this.stream.beginArray();
        int index = 0;
        for (OutputPort port: operator.internalOutputs) {
            this.propertyIndex(index);
            index++;
            if (port == null)
                this.stream.appendNull();
            else
                port.asJson(this);
        }
        this.stream.endArray();

        this.label("outputViews");
        this.stream.beginArray();
        index = 0;
        for (ProgramIdentifier view: operator.outputViews) {
            this.propertyIndex(index);
            index++;
            this.asJsonInner(view);
        }
        this.stream.endArray();
        super.postorder(operator);
    }

    @Override
    public VisitDecision preorder(DBSPIndexedTopKOperator operator) {
        VisitDecision decision = this.preorder(operator.to(DBSPUnaryOperator.class));
        if (decision.stop())
            return VisitDecision.STOP;
        this.label("numbering");
        this.stream.append(operator.numbering.name());
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPRankOperator operator) {
        VisitDecision decision = this.preorder(operator.to(DBSPUnaryOperator.class));
        if (decision.stop())
            return VisitDecision.STOP;
        this.label("numbering");
        this.stream.append(operator.numbering.name());
        return VisitDecision.CONTINUE;
    }

    protected void asJsonInner(IJson value) {
        value.asJson(this.innerVisitor);
    }

    @Override
    public VisitDecision preorder(DBSPViewBaseOperator operator) {
        if (this.preorder(operator.to(DBSPUnaryOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("viewName");
        this.asJsonInner(operator.viewName);
        if (this.innerVisitor.replayMetadata) {
            this.property("query");
            this.stream.append(operator.query);
        }
        this.property("metadata");
        this.asJsonInner(operator.metadata);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPSourceBaseOperator operator) {
        if (this.preorder(operator.to(DBSPSimpleOperator.class)).stop())
            return VisitDecision.STOP;
        this.label("tableName");
        this.asJsonInner(operator.tableName);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPInputMapWithWaterlineOperator operator) {
        if (this.preorder(operator.to(DBSPOperator.class)).stop())
            return VisitDecision.STOP;
        this.label("tableName");
        this.asJsonInner(operator.tableName);
        this.label("metadata");
        this.asJsonInner(operator.metadata);
        this.label("keyFields");
        this.stream.beginArray();
        int index = 0;
        for (int i: operator.keyFields) {
            this.propertyIndex(index);
            index++;
            this.stream.append(i);
        }
        this.stream.endArray();
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPSourceTableOperator operator) {
        if (this.preorder(operator.to(DBSPSourceBaseOperator.class)).stop())
            return VisitDecision.STOP;
        if (this.innerVisitor.replayMetadata) {
            this.label("sourceNamePosition");
            IJson.toJsonStream(operator.sourceName.getPositionRange().asJson(), this.stream);
            this.stream.label("sourceNameEmpty").append(operator.sourceName.isEmpty());
        }
        this.label("metadata");
        this.asJsonInner(operator.metadata);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPSourceMultisetOperator operator) {
        return this.preorder(operator.to(DBSPSourceTableOperator.class));
    }

    @Override
    public VisitDecision preorder(DBSPWeightValidatorOperator operator) {
        if (this.preorder(operator.to(DBSPSimpleOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("message");
        this.stream.append(operator.message);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPSourceMapOperator operator) {
        if (this.preorder(operator.to(DBSPSourceTableOperator.class)).stop())
            return VisitDecision.STOP;
        this.label("keyFields");
        this.stream.beginArray();
        int index = 0;
        for (int i: operator.keyFields) {
            this.propertyIndex(index);
            index++;
            this.stream.append(i);
        }
        this.stream.endArray();
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPLagOperator operator) {
        if (this.preorder(operator.to(DBSPUnaryOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("offset");
        this.stream.append(operator.offset);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPConstantOperator operator) {
        VisitDecision decision = this.preorder(operator.to(DBSPSimpleOperator.class));
        if (decision.stop())
            return VisitDecision.STOP;
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPJoinBaseOperator operator) {
        if (this.preorder(operator.to(DBSPBinaryOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("balanced");
        this.stream.append(operator.balanced);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPConcreteAsofJoinOperator operator) {
        if (this.preorder(operator.to(DBSPJoinBaseOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("isLeft");
        this.stream.append(operator.isLeft);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPAsofJoinOperator operator) {
        if (this.preorder(operator.to(DBSPJoinBaseOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("isLeft");
        this.stream.append(operator.isLeft);
        this.property("leftTimestampIndex");
        this.stream.append(operator.leftTimestampIndex);
        this.property("rightTimestampIndex");
        this.stream.append(operator.rightTimestampIndex);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPWindowOperator operator) {
        if (this.preorder(operator.to(DBSPBinaryOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("lowerInclusive");
        this.stream.append(operator.lowerInclusive);
        this.property("upperInclusive");
        this.stream.append(operator.upperInclusive);
        this.property("lowerUnbounded");
        this.stream.append(operator.lowerUnbounded);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPHopOperator operator) {
        if (this.preorder(operator.to(DBSPUnaryOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("timestampIndex");
        this.stream.append(operator.timestampIndex);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPChainOperator operator) {
        if (this.preorder(operator.to(DBSPUnaryOperator.class)).stop())
            return VisitDecision.STOP;
        this.label("kinds");
        this.stream.beginArray();
        int index = 0;
        for (var computation: operator.chain.computations()) {
            this.propertyIndex(index);
            index++;
            this.stream.append(computation.kind().name());
        }
        this.stream.endArray();
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPBinaryDistinctOperator operator) {
        if (this.preorder(operator.to(DBSPBinaryOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("positive");
        this.stream.append(operator.positive);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPIntegrateTraceRetainKeysOperator operator) {
        if (this.preorder(operator.to(DBSPBinaryOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("accumulate");
        this.stream.append(operator.accumulate);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
        if (this.preorder(operator.to(DBSPBinaryOperator.class)).stop())
            return VisitDecision.STOP;
        this.property("which");
        this.stream.append(operator.which.name());
        this.property("n");
        this.stream.append(operator.n);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPOperator operator) {
        this.stream.endObject();
    }

    @Override
    public VisitDecision preorder(DBSPDeclaration declaration) {
        if (this.preorder(declaration.to(IDBSPOuterNode.class)).stop())
            return VisitDecision.STOP;
        declaration.accept(this.innerVisitor);
        this.stream.endObject();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        super.preorder(circuit);
        this.preorder(circuit.to(IDBSPOuterNode.class));
        this.property("metadata");
        // The live JVM owns the schema throughout each graph rewrite. RemoveTable
        // exchanges selected input IDs separately when it needs to edit the schema.
        if (this.retainProgramMetadata)
            this.stream.appendNull();
        else
            this.asJsonInner(circuit.metadata);
        this.property("incremental");
        this.stream.append(circuit.incremental);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPCircuit circuit) {
        super.postorder(circuit);
        this.stream.endObject();
    }

    public String getJsonString() {
        return this.stream.toString();
    }
}
