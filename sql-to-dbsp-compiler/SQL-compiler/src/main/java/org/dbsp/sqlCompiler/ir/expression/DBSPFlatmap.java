package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.ICollectionType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents an expression of the form
 * |data| { data.field0.map(|e| { Tuple::new(data.field1, data.field2, ..., e )} ) }
 * If 'withOrdinality' is true, the output also contains indexes (1-based) of the
 * elements in the collection, i.e.:
 * |data| { data.field0.enumerate().map(|e| { Tuple::new(data.field1, data.field2, ..., e.1, cast(e.0+1) )} ) }
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
    public final DBSPTypeTuple inputElementType;
    public final int collectionFieldIndex;
    public final List<Integer> outputFields;
    public final DBSPTypeTuple outputElementType;
    @Nullable
    public final DBSPType indexType;
    public final DBSPType collectionElementType;

    public DBSPFlatmap(@Nullable Object node, DBSPTypeTuple inputElementType,
                       int collectionFieldIndex, List<Integer> outputFields,
                       @Nullable DBSPType indexType) {
        super(node, DBSPTypeAny.INSTANCE);
        this.inputElementType = inputElementType;
        this.collectionFieldIndex = collectionFieldIndex;
        this.outputFields = outputFields;
        List<DBSPType> outputFieldTypes = new ArrayList<>();
        for (int i : outputFields)
            outputFieldTypes.add(inputElementType.getFieldType(i));
        DBSPType iterable = this.inputElementType.getFieldType(collectionFieldIndex);
        this.collectionElementType = iterable.to(ICollectionType.class).getElementType();
        outputFieldTypes.add(this.collectionElementType);
        if (indexType != null)
            outputFieldTypes.add(indexType);
        this.outputElementType = new DBSPTypeTuple(outputFieldTypes);
        this.indexType = indexType;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        this.inputElementType.accept(visitor);
        this.outputElementType.accept(visitor);
        if (this.indexType != null)
            this.indexType.accept(visitor);
        this.collectionElementType.accept(visitor);
        if (this.type != null)
            this.type.accept(visitor);
        visitor.postorder(this);
    }
}
