package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinBaseOperator;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.type.CollectionShape;
import org.dbsp.sqlCompiler.ir.type.IndexedShape;
import org.dbsp.util.JsonStream;

import javax.annotation.Nullable;
import java.math.BigInteger;

/** Serialize the layouts, primary keys and row functions read by candidate-key analysis. */
final class CandidateKeyInputs {
    private CandidateKeyInputs() {}

    private static void shape(JsonStream json, @Nullable CollectionShape shape, String width, String index) {
        if (shape != null)
            json.label(width).append(shape.width());
        if (shape instanceof IndexedShape indexed)
            json.label(index).append(indexed.indexFields());
    }

    static void write(JsonStream json, DBSPOperator operator, ToJsonInnerVisitor inner) {
        if (!(operator instanceof DBSPSimpleOperator op))
            return;
        shape(json, op.outputPort().getShape(), "width", "indexWidth");
        if (!op.inputs.isEmpty())
            shape(json, op.inputs.get(0).getShape(), "inputWidth", "inputIndexWidth");
        if (op instanceof DBSPSourceMapOperator source) {
            json.label("sourceWidth").append(source.metadata.getColumns().size()).label("keyFields").beginArray();
            for (int field : source.getKeyFields())
                json.append(field);
            json.endArray();
        } else if (op instanceof DBSPSourceMultisetOperator source) {
            int field = 0;
            json.label("keyFields").beginArray();
            for (var column : source.metadata.getColumns()) {
                if (column.isPrimaryKey)
                    json.append(field);
                field++;
            }
            json.endArray();
        }
        if (op instanceof DBSPIndexedTopKOperator ranked) {
            json.label("singleRank").append(ranked.numbering == DBSPIndexedTopKOperator.Numbering.ROW_NUMBER
                    && ranked.limit instanceof DBSPUSizeLiteral literal && BigInteger.ONE.equals(literal.value));
            if (ranked.outputProducer != null) {
                json.label("function");
                ranked.outputProducer.accept(inner);
            }
        } else if ((op instanceof DBSPMapOperator || op instanceof DBSPMapIndexOperator || op instanceof DBSPFilterOperator
                || op instanceof DBSPJoinBaseOperator || op instanceof DBSPStarJoinBaseOperator)
                && op.function instanceof DBSPClosureExpression function) {
            json.label("function");
            function.accept(inner);
        }
    }
}
