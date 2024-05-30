package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.backend.rust.LowerCircuitVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.ICollectionType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Shuffle;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents an expression of the form
 * |data| { data.field0.map(|e| { Tup::new(data.field1, data.field2, ..., e )} ) }
 * If 'withOrdinality' is true, the output also contains indexes (1-based) of the
 * elements in the collection, i.e.:
 * |data| { data.field0.enumerate().map(|e| { Tup::new(data.field1, data.field2, ..., e.1, cast(e.0+1) )} ) }
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
public final class DBSPFlatmap extends DBSPExpression {
    /** Type of the input row. */
    public final DBSPTypeTuple inputElementType;
    /** A closure which, applied to 'data', produces the
     * collection that is being flatmapped. */
    public final DBSPClosureExpression collectionExpression;
    /** True whether the iterated element itself has to be
     * emitted as part of the output.  If it is emitted, it
     * comes right after all field values and before the collectionIndex. */
    public final boolean emitIteratedElement;
    /** Fields of 'data' emitted in the output.
     * We represent them explicitly, so we can optimize them
     * when combining with a subsequent projection. */
    public final List<Integer> leftCollectionIndexes;
    /** A list of closure expressions that are applied to the 'e'
     * variable to produce some of the output fields. */
    @Nullable
    public final List<DBSPClosureExpression> rightProjections;
    /** The type of the index field, if the operator is invoked WITH ORDINALITY.
     * In this case every element of the collection is output together with its index.
     * If the index is not needed, this field is null.  This field is emitted in the
     * output last. */
    @Nullable
    public final DBSPType collectionIndexType;
    /** The type of the elements in the collection field. */
    public final DBSPType collectionElementType;
    /** Shuffle to apply to elements in the produced tuple */
    public final Shuffle shuffle;

    public DBSPFlatmap(CalciteObject node, DBSPTypeTuple inputElementType,
                       DBSPClosureExpression collectionExpression,
                       List<Integer> leftCollectionIndexes,
                       @Nullable
                       List<DBSPClosureExpression> rightProjections,
                       boolean emitIteratedElement,
                       @Nullable DBSPType collectionIndexType,
                       Shuffle shuffle) {
        super(node, DBSPTypeAny.getDefault());
        this.inputElementType = inputElementType;
        this.rightProjections = rightProjections;
        this.emitIteratedElement = emitIteratedElement;
        this.collectionExpression = collectionExpression;
        this.leftCollectionIndexes = leftCollectionIndexes;
        DBSPType iterable = this.collectionExpression.getResultType();
        this.collectionElementType = iterable.to(ICollectionType.class).getElementType();
        assert collectionExpression.parameters.length == 1;
        assert collectionExpression.parameters[0].type.sameType(this.inputElementType.ref())
                : "Collection expression expects " + collectionExpression.parameters[0].type
                + " but input element type is " + this.inputElementType.ref();
        this.collectionIndexType = collectionIndexType;
        this.shuffle = shuffle;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPFlatmap(this.getNode(),
                this.inputElementType, this.collectionExpression,
                this.leftCollectionIndexes, this.rightProjections,
                this.emitIteratedElement, this.collectionIndexType, this.shuffle);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPFlatmap otherExpression = other.as(DBSPFlatmap.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.collectionExpression, otherExpression.collectionExpression) &&
                this.emitIteratedElement == otherExpression.emitIteratedElement &&
                Linq.same(this.leftCollectionIndexes, otherExpression.leftCollectionIndexes) &&
                this.shuffle.equals(otherExpression.shuffle) &&
                new EquivalenceContext().equivalent(this.rightProjections, otherExpression.rightProjections);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.inputElementType.accept(visitor);
        if (this.collectionIndexType != null)
            this.collectionIndexType.accept(visitor);
        this.collectionElementType.accept(visitor);
        this.type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
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
                this.collectionExpression == o.collectionExpression &&
                Linq.same(this.leftCollectionIndexes, o.leftCollectionIndexes) &&
                Linq.same(this.rightProjections, o.rightProjections) &&
                this.collectionIndexType == o.collectionIndexType &&
                this.collectionElementType == o.collectionElementType &&
                this.shuffle == o.shuffle &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        // |data| { data.field0.map(|e| { Tuple::new(data.field1, data.field2, ..., e )} ) }
        // or
        // |data| { data.field0.enumerate().map(|e| { Tuple::new(data.field1, data.field2, ..., e.1, cast(e.0+1) )} ) }
        builder.append("|data| { ");
        DBSPVariablePath var = new DBSPVariablePath(
                "data", this.collectionExpression.parameters[0].getType());
        DBSPExpression array = this.collectionExpression.call(var);
        builder.append(array);
        if (this.collectionIndexType != null)
            builder.append(".enumerate()");
        int outputCount = this.leftCollectionIndexes.size() +
                (this.emitIteratedElement ? 1 : 0) +
                (this.collectionIndexType != null ? 1 : 0);
        DBSPTypeTupleBase tuple = this.collectionElementType.as(DBSPTypeTupleBase.class);
        if (this.collectionIndexType == null && tuple != null) {
            // See the semantics of UNNEST in calcite: unpack structs in the vector.
            outputCount += tuple.size() - 1;
        }
        builder.append("map(|e| { ")
                .append(DBSPTypeCode.TUPLE.rustName)
                .append(outputCount)
                .append("::new(");
        boolean first = true;

        List<String> expressions = new ArrayList<>();
        for (int index: this.leftCollectionIndexes) {
            if (!first)
                builder.append(", ");
            first = false;
            expressions.add("data." + index);
        }

        if (this.emitIteratedElement) {
            if (this.collectionIndexType != null) {
                expressions.add("e.1");
            } else {
                if (this.rightProjections != null) {
                    DBSPVariablePath e = new DBSPVariablePath("e", this.collectionElementType.ref());
                    for (DBSPClosureExpression clo: this.rightProjections) {
                        expressions.add(clo.call(e).toString());
                    }
                } else if (tuple != null) {
                    // unpack the fields of e.
                    for (int i = 0; i < tuple.size(); i++) {
                        expressions.add("e." + i);
                    }
                } else {
                    expressions.add("e");
                }
            }
        }

        if (this.collectionIndexType != null)
            expressions.add("(e.0 + 1)");
        expressions = this.shuffle.shuffle(expressions);
        builder.join(", ", expressions);
        return builder.append(")} )}");
    }
}
