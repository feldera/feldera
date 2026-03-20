package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
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
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

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
        List<DBSPType> keyFields = new ArrayList<>();
        List<Integer> keyColumnFields = new ArrayList<>();
        int i = 0;
        for (InputColumnMetadata inputColumnMetadata : node.metadata.getColumns()) {
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
            DBSPTypeIndexedZSet ix = getIndexedType(node);
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
            IndexFunctionRewriter rewriter = new IndexFunctionRewriter(
                    this.compiler, closure.parameters[0], w, keyColumnFields);
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

        public IndexFunctionRewriter(
                DBSPCompiler compiler, DBSPParameter parameter,
                DBSPVariablePath w, List<Integer> keyColumnFields) {
            super(compiler);
            this.resolver = new ResolveReferences(compiler, false);
            this.parameter = parameter;
            this.w = w;
            this.keyColumnFields = keyColumnFields;
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
                            this.map(field, w.field(1).deref().field(field.fieldNo));
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