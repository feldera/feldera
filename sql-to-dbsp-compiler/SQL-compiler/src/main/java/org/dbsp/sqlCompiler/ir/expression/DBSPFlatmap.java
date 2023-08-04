package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.compiler.backend.rust.LowerCircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.ICollectionType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents an expression of the form
 * |data| { data.field0.map(|e| { Tuple::new(data.field1, data.field2, ..., e )} ) }
 * If 'withOrdinality' is true, the output also contains indexes (1-based) of the
 * elements in the collection, i.e.:
 * |data| { data.field0.enumerate().map(|e| { Tuple::new(data.field1, data.field2, ..., e.1, cast(e.0+1) )} ) }
 * (The last +1 is because in SQL indexes are 1-based)
 * This is used within a flatmap operation.
 * - data is a "row"
 * - indexType is null if there is no ordinality, and is the
 *   type of the ORDINALITY column otherwise.
 * - field0 is a collection-typed field
 * - field1, field2, etc. are other fields that are being selected.
 * - e iterates over the elements of the data.field0 collection.
 * This represents a closure including another closure.
 * The type of this expression is FunctionType.
 * The argument type is the type of data.
 * The result type is not represented (we can't represent the type produced by an iterator).
 */
public class DBSPFlatmap extends DBSPExpression {
    /**
     * Type of the input row.
     */
    public final DBSPTypeTuple inputElementType;
    /**
     * Index of the collection field within the row.
     * We have inputElementType[collectionFieldIndex] is a collection type.
     */
    public final int collectionFieldIndex;
    /**
     * Fields in the output that come from the input row.
     * The values are encoded as follows:
     * - COLLECTION_INDEX indicates the fact that the field
     * does not come from the input tuple, but it is the INDEX within the
     * collection (used only WITH ORDINALITY).
     * - ITERATED_ELEMENT indicates that the field is the element in the collection (e)
     * - A value >= 0 indicates an index in the 'data' field.
     */
    public final List<Integer> outputFieldIndexes;
    /**
     * The type of the index field, if the operator is invoked WITH ORDINALITY.
     * In this case every element of the collection is output together with its index.
     * If the index is not needed, this field is null.
     */
    @Nullable
    public final DBSPType indexType;
    /**
     * The type of the elements in the collection field.
     */
    public final DBSPType collectionElementType;

    public static final int ITERATED_ELEMENT = -1;
    public static final int COLLECTION_INDEX = -2;

    public DBSPFlatmap(CalciteObject node, DBSPTypeTuple inputElementType,
                       int collectionFieldIndex, List<Integer> outputFieldIndexes,
                       @Nullable DBSPType indexType) {
        super(node, new DBSPTypeAny());
        this.inputElementType = inputElementType;
        this.collectionFieldIndex = collectionFieldIndex;
        this.outputFieldIndexes = outputFieldIndexes;
        DBSPType iterable = this.inputElementType.getFieldType(collectionFieldIndex);
        this.collectionElementType = iterable.to(ICollectionType.class).getElementType();
        this.indexType = indexType;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        this.inputElementType.accept(visitor);
        if (this.indexType != null)
            this.indexType.accept(visitor);
        this.collectionElementType.accept(visitor);
        this.type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    public DBSPFlatmap project(Projection.Description description) {
        List<Integer> outputFields = new ArrayList<>();
        for (int i = 0; i < description.fields.size(); i++) {
            int index = description.fields.get(i);
            outputFields.add(this.outputFieldIndexes.get(index));
        }
        return new DBSPFlatmap(this.getNode(), this.inputElementType,
                this.collectionFieldIndex, outputFields, this.indexType);
    }

    @Override
    public String toString() {
        DBSPExpression converted = LowerCircuitVisitor.rewriteFlatmap(this);
        return converted.toString();
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPFlatmap o = other.as(DBSPFlatmap.class);
        if (o == null)
            return false;
        return this.inputElementType == o.inputElementType &&
                this.collectionFieldIndex == o.collectionFieldIndex &&
                Linq.same(this.outputFieldIndexes, o.outputFieldIndexes) &&
                this.indexType == o.indexType &&
                this.collectionElementType == o.collectionElementType &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        // |data| { data.field0.map(|e| { Tuple::new(data.field1, data.field2, ..., e )} ) }
        // or
        // |data| { data.field0.enumerate().map(|e| { Tuple::new(data.field1, data.field2, ..., e.1, cast(e.0+1) )} ) }
        builder.append("|data| { data.")
                .append(this.collectionFieldIndex);
        if (this.indexType != null)
            builder.append(".enumerate()");
        builder.append("map(|e| { Tuple")
                .append(this.outputFieldIndexes.size())
                .append("::new(");
        boolean first = true;
        for (int index: this.outputFieldIndexes) {
            if (!first)
                builder.append(", ");
            first = false;
            if (index >= 0)
                builder.append("data.")
                        .append(index);
            else if (index == ITERATED_ELEMENT) {
                builder.append("e");
                if (this.indexType != null)
                    builder.append(".1");
            }
            else if (index == COLLECTION_INDEX)
                builder.append("(e.0 + 1)");
        }
        return builder.append(")} )}");
    }
}
