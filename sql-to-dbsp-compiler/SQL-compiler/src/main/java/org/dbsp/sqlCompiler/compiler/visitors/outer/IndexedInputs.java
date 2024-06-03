package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts DBSPSourceMultisetOperators that have a primary key
 * into DBSPSourceSetOperator followed by a DBSPDeindexOperator.
 */
public class IndexedInputs extends CircuitCloneVisitor {
    public IndexedInputs(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator node) {
        List<DBSPType> keyFields = new ArrayList<>();
        List<Integer> keyColumnFields = new ArrayList<>();
        int i = 0;
        for (InputColumnMetadata inputColumnMetadata: node.metadata.getColumns()) {
            if (inputColumnMetadata.isPrimaryKey) {
                keyColumnFields.add(i);
                keyFields.add(inputColumnMetadata.type);
            }
            i++;
        }
        if (keyColumnFields.isEmpty()) {
            super.postorder(node);
            return;
        }

        DBSPType keyType = new DBSPTypeTuple(keyFields);
        DBSPTypeZSet inputType = node.outputType.to(DBSPTypeZSet.class);
        DBSPTypeIndexedZSet ix = new DBSPTypeIndexedZSet(node.getNode(), keyType, inputType.elementType);
        DBSPSourceMapOperator set = new DBSPSourceMapOperator(
                node.getNode(), node.sourceName, keyColumnFields,
                ix, node.originalRowType, node.comment,
                node.metadata, node.tableName);
        this.addOperator(set);
        DBSPDeindexOperator deindex = new DBSPDeindexOperator(node.getNode(), set);
        this.map(node, deindex);
    }
}
