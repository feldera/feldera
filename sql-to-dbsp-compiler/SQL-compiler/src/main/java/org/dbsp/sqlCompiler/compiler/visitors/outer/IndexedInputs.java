package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** Converts {@link DBSPSourceMultisetOperator}s that have a primary key
 * into {@link DBSPSourceMapOperator} followed by a {@link DBSPDeindexOperator}. */
public class IndexedInputs extends CircuitCloneVisitor {
    public IndexedInputs(DBSPCompiler compiler) {
        super(compiler, false);
    }

    /** Given a source node, return the type of the indexed Z-set that has as keys
     * the key fields, and as value the output value.  Return null if there are no key fields. */
    @Nullable
    public static DBSPTypeIndexedZSet getIndexedType(DBSPSourceMultisetOperator node) {
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
            return null;
        }

        DBSPType keyType = new DBSPTypeTuple(keyFields);
        DBSPTypeZSet inputType = node.outputType.to(DBSPTypeZSet.class);
        return new DBSPTypeIndexedZSet(node.getNode(), keyType, inputType.elementType);
    }

    public static List<Integer> getKeyFields(DBSPSourceMultisetOperator node) {
        List<Integer> keyColumnFields = new ArrayList<>();
        int i = 0;
        for (InputColumnMetadata inputColumnMetadata: node.metadata.getColumns()) {
            if (inputColumnMetadata.isPrimaryKey) {
                keyColumnFields.add(i);
            }
            i++;
        }
        return keyColumnFields;
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator node) {
        DBSPTypeIndexedZSet ix = IndexedInputs.getIndexedType(node);
        if (ix == null) {
            super.postorder(node);
            return;
        }

        List<Integer> keyColumnFields = getKeyFields(node);
        DBSPSourceMapOperator set = new DBSPSourceMapOperator(
                node.getRelNode(), node.sourceName, keyColumnFields,
                ix, node.originalRowType, node.metadata, node.tableName, node.comment);
        this.addOperator(set);
        DBSPDeindexOperator deindex = new DBSPDeindexOperator(node.getRelNode(), node.getFunctionNode(), set.outputPort());
        this.map(node, deindex);
    }
}
