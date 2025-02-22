package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.visitors.outer.LowerCircuitVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.ICollectionType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Shuffle;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Represents an expression of the form
 * |data| { data.field0.map(|e| {
 *    Tup::new(data.field1, data.field2, ..., rightProj1(Tup1(e)), rightProj2(Tup1(e)), ... )
 * } ) }
 * If 'withOrdinality' is true, the output also contains indexes (1-based) of the
 * elements in the collection, i.e.:
 * |data| { data.field0.enumerate().map(|e| {
 *   Tup::new(data.field1, data.field2, ..., rightProj1(e.1, cast(e.0+1)), rightProj2(e.1, cast(e.0+1)), ... )
 * } ) }
 * (The last +1 is because in SQL indexes are 1-based)
 * If there is no rightProj, the above becomes:
 * |data| { data.field0.enumerate().map(|e| {
 *  Tup::new(data.field1, data.field2, ..., e.1, cast(e.0+1))
 * } ) }
 *
 * <p>This is used within a flatmap operation.
 * - data is a "row"
 * - indexType is null if there is no ordinality, and is the
 *   type of the ORDINALITY column otherwise.
 * - field0 is a collection-typed field
 * - field1, field2, etc. are other fields that are being selected.
 * - e iterates over the elements of the data.field0 collection.
 * - rightProjN are functions applied to e
 * This represents a closure including another closure.
 * The type of this expression is FunctionType.
 * The argument type is the type of data.
 * The result type is the output element type.
 */
public final class DBSPFlatmap extends DBSPExpression {
    /** Type of the input row. */
    public final DBSPTypeTuple inputElementType;
    /** A closure which, applied to 'data', produces the
     * collection that is being flatmapped. */
    public final DBSPClosureExpression collectionExpression;
    /** Fields of 'data' emitted in the output.
     * We represent them explicitly, so we can optimize them
     * when combining with a subsequent projection. */
    public final List<Integer> leftInputIndexes;
    /** A list of closure expressions that are applied to the unnest result
     * to produce some of the output fields (argument is either e, or (e, index)),
     * depending on ORDINALITY. */
    @Nullable
    public final List<DBSPClosureExpression> rightProjections;
    /** The type of the index field, if the operator is invoked WITH ORDINALITY.
     * In this case every element of the collection is output together with its index.
     * If the index is not needed, this field is null.  This field is emitted in the
     * output last if there are no rightProjections. */
    @Nullable
    public final DBSPType ordinalityIndexType;
    /** Shuffle to apply to elements in the produced tuple */
    public final Shuffle shuffle;

    public DBSPFlatmap(CalciteObject node,
                       DBSPTypeFunction resultElementType,
                       DBSPTypeTuple inputElementType,
                       DBSPClosureExpression collectionExpression,
                       List<Integer> leftInputIndexes,
                       @Nullable
                       List<DBSPClosureExpression> rightProjections,
                       @Nullable DBSPType ordinalityIndexType,
                       Shuffle shuffle) {
        super(node, resultElementType);
        this.inputElementType = inputElementType;
        this.rightProjections = rightProjections;
        this.collectionExpression = collectionExpression;
        this.leftInputIndexes = leftInputIndexes;
        assert collectionExpression.parameters.length == 1;
        assert collectionExpression.parameters[0].type.sameType(this.inputElementType.ref())
                : "Collection expression expects " + collectionExpression.parameters[0].type
                + " but input element type is " + this.inputElementType.ref();
        assert resultElementType.resultType.is(DBSPTypeTuple.class);
        this.ordinalityIndexType = ordinalityIndexType;
        assert this.ordinalityIndexType == null || this.ordinalityIndexType.is(DBSPTypeBaseType.class);
        this.shuffle = shuffle;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPFlatmap(this.getNode(),
                this.getType().to(DBSPTypeFunction.class), this.inputElementType, this.collectionExpression,
                this.leftInputIndexes, this.rightProjections,
                this.ordinalityIndexType, this.shuffle);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPFlatmap otherExpression = other.as(DBSPFlatmap.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.collectionExpression, otherExpression.collectionExpression) &&
                Linq.same(this.leftInputIndexes, otherExpression.leftInputIndexes) &&
                this.shuffle.equals(otherExpression.shuffle) &&
                this.type.sameType(other.getType()) &&
                new EquivalenceContext().equivalent(this.rightProjections, otherExpression.rightProjections);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("inputElementType");
        this.inputElementType.accept(visitor);
        if (this.ordinalityIndexType != null) {
            visitor.property("ordinalityIndexType");
            this.ordinalityIndexType.accept(visitor);
        }
        if (this.rightProjections != null) {
            visitor.startArrayProperty("rightProjections");
            int index = 0;
            for (DBSPClosureExpression proj: this.rightProjections) {
                visitor.propertyIndex(index);
                index++;
                proj.accept(visitor);
            }
            visitor.endArrayProperty("rightProjections");
        }
        visitor.property("type");
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
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPFlatmap o = other.as(DBSPFlatmap.class);
        if (o == null)
            return false;
        return this.type == o.type &&
                this.inputElementType == o.inputElementType &&
                this.collectionExpression == o.collectionExpression &&
                Linq.same(this.leftInputIndexes, o.leftInputIndexes) &&
                Linq.same(this.rightProjections, o.rightProjections) &&
                this.ordinalityIndexType == o.ordinalityIndexType &&
                this.shuffle == o.shuffle &&
                this.hasSameType(o);
    }

    /** The type of the values in the collection */
    public DBSPType getCollectionElementType() {
        DBSPType iterable = this.collectionExpression.getResultType();
        return iterable.to(ICollectionType.class).getElementType();
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        // This is a simplified version of what LowerCircuitVisitor.rewriteFlatmap does
        builder.append("|data| { ");
        DBSPVariablePath var = new DBSPVariablePath(
                "data", this.collectionExpression.parameters[0].getType());
        DBSPExpression array = this.collectionExpression.call(var);
        builder.append(array);
        if (this.ordinalityIndexType != null)
            builder.append(".enumerate()");

        DBSPType collectionElementType = this.getCollectionElementType();
        List<String> expressions = new ArrayList<>();
        for (int index: this.leftInputIndexes) 
            expressions.add("data." + index);

        DBSPTypeTupleBase collectionAsTuple = collectionElementType.as(DBSPTypeTupleBase.class);
        int eSize = collectionAsTuple != null ? collectionAsTuple.size() : 1;
        String e0plus1 = "e+1";
        if (this.rightProjections != null) {
            for (DBSPClosureExpression clo: this.rightProjections) {
                List<String> fields;
                final String base;
                if (this.ordinalityIndexType != null)
                    base = "e.1";
                else {
                    base = "e";
                }
                if (collectionElementType.is(DBSPTypeTupleBase.class)) {
                    // apply closure to (e.1.0, e.1.1, ..., e.0+1)
                    fields = Linq.map(Linq.list(IntStream.range(0, eSize)), v -> base + "." + v);
                } else {
                    // apply closure to (e.1, e.0+1)
                    fields = Linq.list(base);
                }
                if (this.ordinalityIndexType != null)
                    fields.add(e0plus1);
                String argument = "(Tup" + fields.size() + "(" + String.join(", ", fields) + "))";
                expressions.add(clo.toString() + "(&" + argument + ")");
            }
        } else {
            if (this.ordinalityIndexType != null) {
                // e.1, as produced by the iterator
                if (collectionElementType.is(DBSPTypeTupleBase.class)) {
                    for (int i = 0; i < collectionElementType.to(DBSPTypeTupleBase.class).size(); i++)
                        expressions.add("e.1." + i);
                } else {
                    expressions.add("e.1");
                }
            } else if (collectionElementType.is(DBSPTypeTupleBase.class)) {
                // Calcite's UNNEST has a strange semantics:
                // If e is a tuple type, unpack its fields here
                DBSPTypeTupleBase tuple = collectionElementType.to(DBSPTypeTupleBase.class);
                for (int ei = 0; ei < tuple.size(); ei++)
                    expressions.add("e." + ei);
            } else {
                expressions.add("e");
            }
            if (this.ordinalityIndexType != null) {
                expressions.add(e0plus1);
            }
        }
        expressions = this.shuffle.shuffle(expressions);
        return builder.append("map(|e| { ")
                .append(DBSPTypeCode.TUPLE.rustName)
                .append(expressions.size())
                .append("::new(")
                .join(", ", expressions)
                .append(")} )}");
    }
}
