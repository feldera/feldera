package org.dbsp.sqlCompiler.compiler.backend.jit;

import org.dbsp.sqlCompiler.circuit.IDBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ReferenceMap;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.util.ICastable;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.StringPrintStream;
import org.dbsp.util.ToIndentableString;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * If a closure has a parameter that is a nested tuple p = ((a, b), c), replace it
 * with a flattened tuple, p = (a, b, c).
 */
public class FlattenParameter extends InnerRewriteVisitor {
    /**
     * Represent a nested tuple type as a tree.  Each tuple
     * is a Parent, each base type is a Leaf.
     */
    static abstract class TreeNode implements ICastable, ToIndentableString {
        abstract void addTypes(List<DBSPType> types);

        final int size;

        TreeNode(int size) {
            this.size = size;
        }

        /**
         * Given a path through the tree, return the index of the
         * leaf reached by the path if numbering leaves in postorder.
         */
        abstract int newIndex(List<Integer> indexes, int level);

        DBSPType getType() {
            if (this.is(Leaf.class))
                return this.to(Leaf.class).type;
            List<DBSPType> types = new ArrayList<>();
            this.addTypes(types);
            return new DBSPTypeRawTuple(null, types);
        }

        int newIndex(List<Integer> indexes) {
            return this.newIndex(indexes, 0);
        }

        @Override
        public String toString() {
            StringPrintStream str = new StringPrintStream();
            IndentStream stream = new IndentStream(str.getPrintStream());
            stream.append(this);
            return str.toString();
        }
    };

    static class Leaf extends TreeNode {
        final DBSPType type;

        Leaf(DBSPType type) {
            super(1);
            this.type = type;
        }

        @Override
        void addTypes(List<DBSPType> types) {
            types.add(this.type);
        }

        @Override
        int newIndex(List<Integer> indexes, int level) {
            if (level != indexes.size())
                throw new RuntimeException("Illegal level " + level + " in " + indexes);
            return 0;
        }

        @Override
        public IIndentStream toString(IIndentStream builder) {
            return builder.append(this.type.toString());
        }
    }

    static class Parent extends TreeNode {
        final TreeNode[] children;
        Parent(TreeNode[] children) {
            super(Linq.reduce(Linq.map(children, c -> c.size, Integer.class), 0, Integer::sum));
            this.children = children;
        }

        @Override
        void addTypes(List<DBSPType> types) {
            for (TreeNode child: this.children)
                child.addTypes(types);
        }

        @Override
        int newIndex(List<Integer> indexes, int level) {
            if (level > indexes.size() - 1)
                throw new RuntimeException("Illegal level " + level + " in " + indexes);
            int index = indexes.get(level);
            int newIndex = 0;
            for (int i = 0; i < index; i++)
                newIndex += this.children[i].size;
            int childIndex = this.children[index].newIndex(indexes, level + 1);
            return newIndex + childIndex;
        }

        @Override
        public IIndentStream toString(IIndentStream builder) {
            return builder.append("+")
                    .increase()
                    .intercalateI("\n", this.children)
                    .decrease();
        }
    }

    final ReferenceMap referenceMap;
    final Map<DBSPParameter, TreeNode> parameterRepresentation;
    final Map<DBSPParameter, DBSPParameter> parameterMap;

    public FlattenParameter(IErrorReporter reporter, ReferenceMap referenceMap) {
        super(reporter);
        this.parameterRepresentation = new HashMap<>();
        this.parameterMap = new HashMap<>();
        this.referenceMap = referenceMap;
    }

    @Override
    public VisitDecision preorder(DBSPType node) {
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldExpression field) {
        this.push(field);
        DBSPExpression source = this.transform(field.expression);
        this.pop(field);
        IDBSPInnerNode parent = this.getParent();
        boolean skip = parent != null && parent.is(DBSPFieldExpression.class);

        DBSPParameter parameter = null;
        List<Integer> indexes = new ArrayList<>();
        DBSPVariablePath sourceVar = null;
        if (!skip) {
            indexes.add(field.fieldNo);
            while (source.is(DBSPFieldExpression.class)) {
                DBSPFieldExpression sourceField = source.to(DBSPFieldExpression.class);
                indexes.add(sourceField.fieldNo);
                source = sourceField.expression;
            }
            sourceVar = source.as(DBSPVariablePath.class);
            if (sourceVar != null) {
                IDBSPDeclaration declaration = this.referenceMap.getDeclaration(sourceVar);
                parameter = declaration.as(DBSPParameter.class);
            }
        }
        if (parameter == null) {
            DBSPExpression result = field;
            if (source != field.expression)
                result = source.field(field.fieldNo);
            this.map(field, result);
            return VisitDecision.STOP;
        }

        // This is a chain of field references ending in a reference to a parameter
        // e.g., param.0.1.0.  Replace it with a reference to the same field in the
        // flattened parameter.
        Collections.reverse(indexes);
        DBSPExpression result = field;
        TreeNode tree = Utilities.getExists(this.parameterRepresentation, parameter);
        DBSPParameter newParameter = Utilities.getExists(this.parameterMap, parameter);
        int newIndex = tree.newIndex(indexes);
        source = newParameter.asVariableReference();
        if (source != field.expression || newIndex != field.fieldNo)
            result = source.field(newIndex);
        this.map(field, result);
        return VisitDecision.STOP;
    }

    TreeNode flattenType(DBSPType type) {
        DBSPTypeTupleBase tuple = type.as(DBSPTypeTupleBase.class);
        if (tuple == null)
            return new Leaf(type);
        TreeNode[] children = Linq.map(tuple.tupFields, this::flattenType, TreeNode.class);
        return new Parent(children);
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression closure) {
        this.push(closure);
        DBSPParameter[] newParameters = new DBSPParameter[closure.parameters.length];
        boolean changes = false;
        int index = 0;
        for (DBSPParameter parameter: closure.parameters) {
            DBSPType type = parameter.getNonVoidType();
            boolean isRef = type.is(DBSPTypeRef.class);
            if (isRef) {
                type = type.to(DBSPTypeRef.class).type;
            }
            TreeNode tree = this.flattenType(type);
            Utilities.putNew(this.parameterRepresentation, parameter, tree);
            DBSPParameter newParameter = parameter;
            DBSPType newType = tree.getType();
            if (!newType.sameType(type)) {
                if (isRef)
                    newType = newType.ref();
                newParameter = new DBSPParameter(parameter.name, newType);
                changes = true;
            }
            newParameters[index] = newParameter;
            Utilities.putNew(this.parameterMap, parameter, newParameter);
            index++;
        }
        if (changes) {
            DBSPExpression newBody = this.transform(closure.body);
            DBSPClosureExpression result = new DBSPClosureExpression(newBody, newParameters);
            this.map(closure, result);
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("FlattenParameter replaces")
                    .newline()
                    .append((Supplier<String>) closure::toString)
                    .newline()
                    .append("with")
                    .newline()
                    .append((Supplier<String>) result::toString);
        } else {
            super.preorder(closure);
        }
        this.pop(closure);

        for (DBSPParameter parameter: closure.parameters) {
            Utilities.removeExists(this.parameterRepresentation, parameter);
        }
        return VisitDecision.STOP;
    }
}
