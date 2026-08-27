package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionTranslator;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindCommonProjections;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.ReplaceCommonProjections;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Invokes {@link IndexedInputs} and then optimizes the circuit a bit */
public class ExpandIndexedInputs extends Passes {
    public ExpandIndexedInputs(DBSPCompiler compiler) {
        super("ExpandIndexedInputs", compiler);
        this.add(new IndexedInputs(compiler));
        Graph graph = new Graph(compiler);
        this.add(graph);
        FindCommonProjections fcp = new FindCommonProjections(compiler, graph.getGraphs());
        this.add(fcp);
        this.add(new ReplaceCommonProjections(compiler, fcp));
        this.add(new DeadCode(compiler, true));
    }

    /**
     * Given a source node, return the type of the indexed Z-set that has as keys
     * the key fields, and as value the output value.  Return null if there are no key fields.
     */
    @Nullable
    public static DBSPTypeIndexedZSet getIndexedType(DBSPSourceMultisetOperator node) {
        return getIndexedType(node, false);
    }

    /** As {@link #getIndexedType(DBSPSourceMultisetOperator)}, but when {@code dedupKeys} the
     * value drops the primary-key columns, so the indexed z-set holds each column once
     * ({@code key ++ value}, no duplicate).  The whole row is rebuilt downstream by
     * interleaving the key back at its original positions. */
    @Nullable
    public static DBSPTypeIndexedZSet getIndexedType(DBSPSourceMultisetOperator node, boolean dedupKeys) {
        List<DBSPType> keyFields = new ArrayList<>();
        List<DBSPType> valueFields = new ArrayList<>();
        List<Integer> keyColumnFields = new ArrayList<>();
        int i = 0;
        for (InputColumnMetadata inputColumnMetadata : node.metadata.getColumns()) {
            if (inputColumnMetadata.isPrimaryKey) {
                keyColumnFields.add(i);
                keyFields.add(inputColumnMetadata.type);
            } else {
                valueFields.add(inputColumnMetadata.type);
            }
            i++;
        }
        if (keyColumnFields.isEmpty()) {
            return null;
        }

        DBSPType keyType = new DBSPTypeTuple(keyFields);
        DBSPTypeZSet inputType = node.outputType.to(DBSPTypeZSet.class);
        DBSPType valueType = dedupKeys ? new DBSPTypeTuple(valueFields) : inputType.elementType;
        return new DBSPTypeIndexedZSet(node.getNode(), keyType, valueType);
    }

    public static List<Integer> getKeyFields(DBSPSourceMultisetOperator node) {
        List<Integer> keyColumnFields = new ArrayList<>();
        int i = 0;
        for (InputColumnMetadata inputColumnMetadata : node.metadata.getColumns()) {
            if (inputColumnMetadata.isPrimaryKey) {
                keyColumnFields.add(i);
            }
            i++;
        }
        return keyColumnFields;
    }

    /**
     * Converts {@link DBSPSourceMultisetOperator}s that have a primary key
     * into {@link DBSPSourceMapOperator} followed by a {@link DBSPDeindexOperator}.
     */
    static class IndexedInputs extends CircuitCloneVisitor {
        public IndexedInputs(DBSPCompiler compiler) {
            super(compiler, false);
        }

        @Override
        public void postorder(DBSPSourceMultisetOperator node) {
            // Under --gen2 the value drops the primary-key columns (the indexed z-set holds
            // each column once); otherwise the value is the whole row.
            boolean dedupKeys = this.compiler.options.ioOptions.gen2;
            DBSPTypeIndexedZSet ix = getIndexedType(node, dedupKeys);
            if (ix == null) {
                super.postorder(node);
                return;
            }

            List<Integer> keyColumnFields = getKeyFields(node);
            DBSPSourceMapOperator set = new DBSPSourceMapOperator(
                    node.getRelNode(), node.sourceName, keyColumnFields,
                    ix, node.originalRowType, node.metadata, node.tableName, node.comment);
            this.addOperator(set);
            if (dedupKeys) {
                // The value no longer contains the key, so rebuild the whole row by
                // interleaving the key at its original positions instead of a plain deindex.
                DBSPClosureExpression reconstruct = interleaveKeyValue(node, ix, keyColumnFields);
                DBSPMapOperator map = new DBSPMapOperator(node.getRelNode(), reconstruct, set.outputPort());
                this.map(node, map);
            } else {
                DBSPDeindexOperator deindex = new DBSPDeindexOperator(
                        node.getRelNode(), node.getFunctionNode(), set.outputPort());
                this.map(node, deindex);
            }
        }

        /** Rebuild the whole row from a deduped {@code (key, value)} pair: field {@code i}
         * comes from the key when column {@code i} is a primary-key column, else from the
         * value. */
        private DBSPClosureExpression interleaveKeyValue(
                DBSPSourceMultisetOperator node, DBSPTypeIndexedZSet ix, List<Integer> keyColumnFields) {
            Set<Integer> keyColumns = new HashSet<>(keyColumnFields);
            DBSPVariablePath w = ix.getKVRefType().var();
            int columns = node.metadata.getColumns().size();
            DBSPExpression[] rowFields = new DBSPExpression[columns];
            int keySlot = 0;
            int valueSlot = 0;
            for (int i = 0; i < columns; i++) {
                if (keyColumns.contains(i)) {
                    rowFields[i] = w.field(0).deref().field(keySlot++).applyCloneIfNeeded();
                } else {
                    rowFields[i] = w.field(1).deref().field(valueSlot++).applyCloneIfNeeded();
                }
            }
            return new DBSPTupleExpression(rowFields).closure(w.asParameter()).to(DBSPClosureExpression.class);
        }


        @Override
        public void postorder(DBSPMapIndexOperator node) {
            // Note: map is the ORIGINAL input of this node
            var multiset = node.input().node().as(DBSPSourceMultisetOperator.class);
            if (multiset == null) {
                super.postorder(node);
                return;
            }

            List<Integer> keyColumnFields = getKeyFields(multiset);
            if (keyColumnFields.isEmpty()) {
                super.postorder(node);
                return;
            }

            // Find the translation, it must already exist
            var deindex = this.mapped(multiset.outputPort());
            DBSPSourceMapOperator map = deindex.node().inputs.get(0).node().to(DBSPSourceMapOperator.class);
            DBSPClosureExpression rewritten = this.rewriteClosure(node.getClosureFunction(), map, keyColumnFields);
            var mx = new DBSPMapIndexOperator(node.getRelNode(), rewritten, map.outputPort());
            this.map(node, mx);
        }

        private DBSPClosureExpression rewriteClosure(
                DBSPClosureExpression closure, DBSPSourceMapOperator map, List<Integer> keyColumnFields) {
            DBSPVariablePath w = map.getOutputIndexedZSetType().getKVRefType().var();
            boolean dedupKeys = this.compiler.options.ioOptions.gen2;
            IndexFunctionRewriter rewriter = new IndexFunctionRewriter(
                    this.compiler, closure.parameters[0], w, keyColumnFields, dedupKeys);
            var result = rewriter.apply(closure);
            return result.to(DBSPClosureExpression.class);
        }
    }

    static class IndexFunctionRewriter extends ExpressionTranslator {
        // closure is a closure of the form |v: &TupN<>| -> (TupK<>, TupM<>)
        // map is an operator with an output of type (TupR<>, TupN<>), N >= R
        // keyColumnFields is the list of fields of map which are keys.
        // There are R such fields, and TupR<> is composed of these fields of the TupL<> tuple
        // This rewrites the closure to have the form |w: (&TupR<>, &TupN<>)| -> (TupK<>, TupM<>)
        // and to use the key fields as much as possible.  So
        // (*v.X) is rewritten as *(w.0).Y if Y is the X-th key field or as
        // *(w.1).X otherwise
        final ResolveReferences resolver;
        private final DBSPParameter parameter;
        private final DBSPVariablePath w;
        private final List<Integer> keyColumnFields;
        private final boolean dedupKeys;

        public IndexFunctionRewriter(
                DBSPCompiler compiler, DBSPParameter parameter,
                DBSPVariablePath w, List<Integer> keyColumnFields, boolean dedupKeys) {
            super(compiler);
            this.resolver = new ResolveReferences(compiler, false);
            this.parameter = parameter;
            this.w = w;
            this.keyColumnFields = keyColumnFields;
            this.dedupKeys = dedupKeys;
        }

        @Override
        public void startVisit(IDBSPInnerNode node) {
            super.startVisit(node);
            this.resolver.apply(node);
        }

        @Override
        public void postorder(DBSPVariablePath var) {
            var decl = this.resolver.reference.getDeclaration(var);
            if (decl == this.parameter) {
                this.map(var, this.w.field(1));
            } else {
                super.postorder(var);
            }
        }

        @Override
        public void postorder(DBSPFieldExpression field) {
            if (field.expression.is(DBSPDerefExpression.class)) {
                var deref = field.expression.to(DBSPDerefExpression.class);
                if (deref.expression.is(DBSPVariablePath.class)) {
                    var var = deref.expression.to(DBSPVariablePath.class);
                    var decl = this.resolver.reference.getDeclaration(var);
                    if (decl == this.parameter) {
                        // field is of the form (*v.X)
                        int keyField = this.keyColumnFields.indexOf(field.fieldNo);
                        if (keyField < 0) {
                            // A non-key column.
                            int valueField = field.fieldNo;
                            if (this.dedupKeys) {
                                // The value dropped the key columns, so address the column by
                                // its compacted slot rather than its original row position.
                                int keysBefore = 0;
                                for (int key : this.keyColumnFields)
                                    if (key < field.fieldNo)
                                        keysBefore++;
                                valueField = field.fieldNo - keysBefore;
                            }
                            this.map(field, w.field(1).deref().field(valueField));
                        } else {
                            this.map(field, w.field(0).deref().field(keyField));
                        }
                        return;
                    }
                }
            }
            super.postorder(field);
        }

        @Override
        public void postorder(DBSPClosureExpression closure) {
            if (this.context.isEmpty()) {
                DBSPExpression body = this.getE(closure.body);
                this.map(closure, body.closure(this.w.asParameter()));
            } else {
                super.postorder(closure);
            }
        }
    }
}