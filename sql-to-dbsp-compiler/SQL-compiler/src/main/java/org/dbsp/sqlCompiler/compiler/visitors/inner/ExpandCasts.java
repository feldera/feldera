package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsDateType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBinary;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** Rewrites casts between complex types into sequences of operations. */
public class ExpandCasts extends InnerRewriteVisitor {
    // This pass may be iterated many times, depending on the complexity of the expressions involved.
    // Expanding some casts generates other casts.
    public ExpandCasts(DBSPCompiler compiler) {
        super(compiler, false);
    }

    void unsupported(DBSPExpression source, DBSPType type) {
        throw new UnsupportedException("Casting of value with type '" +
                source.getType().asSqlString() +
                "' to the target type '" + type.asSqlString() + "' not supported", source.getNode());
    }

    @Nullable DBSPExpression convertToVariant(DBSPExpression source, boolean mayBeNull) {
        DBSPExpression expression;
        if (source.type.is(DBSPTypeTuple.class)) {
            // Convert a tuple to a VARIANT MAP indexed by the field names
            DBSPTypeTuple tuple = source.getType().to(DBSPTypeTuple.class);
            DBSPTypeMap type = new DBSPTypeMap(
                    DBSPTypeString.varchar(false),
                    DBSPTypeVariant.create(false),
                    source.getType().mayBeNull);

            if (tuple.originalStruct == null) {
                throw new UnimplementedException("Cast between Tuple type and " +
                        tuple.asSqlString() + " not implemented", source.getNode());
            }
            List<DBSPExpression> keys = new ArrayList<>();
            List<DBSPExpression> values = new ArrayList<>();
            List<ProgramIdentifier> names = Linq.list(tuple.originalStruct.getFieldNames());
            for (int i = 0; i < tuple.size(); i++) {
                ProgramIdentifier fieldName = names.get(i);
                keys.add(new DBSPStringLiteral(fieldName.toString()));

                DBSPExpression field = source.field(i).simplify();
                if (!field.getType().hasCopy())
                    field = field.applyCloneIfNeeded();
                DBSPExpression rec = field.cast(source.getNode(), DBSPTypeVariant.create(false), false);
                values.add(rec);
            }
            expression = new DBSPMapExpression(type, keys, values);
        } else if (source.type.is(DBSPTypeArray.class)) {
            // Convert a vector by converting all elements to Variant
            DBSPTypeArray vecType = source.type.to(DBSPTypeArray.class);
            DBSPType elementType = vecType.getElementType();
            if (elementType.is(DBSPTypeVariant.class)) {
                // nothing to do
                return null;
            }
            DBSPVariablePath var = elementType.ref().var();
            // This expression may need to be recursively converted
            DBSPExpression converter = var.deref()
                    .applyCloneIfNeeded().cast(source.getNode(), DBSPTypeVariant.create(false), false)
                    .closure(var);
            expression = new DBSPBinaryExpression(source.getNode(),
                    new DBSPTypeArray(DBSPTypeVariant.create(false), vecType.mayBeNull),
                    DBSPOpcode.ARRAY_CONVERT, source, converter);
        } else {
            return null;
        }
        return new DBSPCastExpression(source.getNode(), expression, DBSPTypeVariant.create(mayBeNull), false);
    }

    DBSPExpression convertToStruct(DBSPExpression source, DBSPTypeTuple type) {
        List<DBSPExpression> fields = new ArrayList<>();
        Utilities.enforce(type.originalStruct != null);
        DBSPType sourceType = source.getType();
        List<ProgramIdentifier> names = Linq.list(type.originalStruct.getFieldNames());
        for (int i = 0; i < type.size(); i++) {
            ProgramIdentifier fieldName = names.get(i);
            DBSPType fieldType = type.getFieldType(i);
            DBSPExpression index;
            if (sourceType.is(DBSPTypeTupleBase.class)) {
                index = source.field(i);
            } else {
                index = new DBSPBinaryExpression(
                        // Result of index is always nullable
                        source.getNode(), DBSPTypeVariant.create(true),
                        DBSPOpcode.VARIANT_INDEX, source.applyCloneIfNeeded(),
                        new DBSPStringLiteral(fieldName.toString()));
            }
            DBSPExpression expression;
            if (fieldType.is(DBSPTypeTuple.class)) {
                expression = this.convertToStruct(index.applyClone(), fieldType.to(DBSPTypeTuple.class));
            } else {
                expression = index.applyCloneIfNeeded().cast(source.getNode(), fieldType, false);
            }
            fields.add(expression);
        }
        return new DBSPTupleExpression(source.getNode(), type, fields);
    }

    DBSPExpression convertToTuple(DBSPExpression source, DBSPTypeRawTuple type) {
        List<DBSPExpression> fields = new ArrayList<>();
        DBSPType sourceType = source.getType();
        if (!sourceType.is(DBSPTypeRawTuple.class))
            throw new InternalCompilerError("Cast to RAW tuple from " + sourceType);
        for (int i = 0; i < type.size(); i++) {
            DBSPType fieldType = type.getFieldType(i);
            DBSPExpression index = source.field(i);
            DBSPExpression expression;
            if (fieldType.is(DBSPTypeTuple.class)) {
                expression = this.convertToStruct(index.applyClone(), fieldType.to(DBSPTypeTuple.class));
            } else {
                expression = index.applyCloneIfNeeded().cast(source.getNode(), fieldType, false);
            }
            fields.add(expression);
        }
        return new DBSPRawTupleExpression(fields);
    }

    @Nullable DBSPExpression convertToVector(DBSPExpression source, DBSPTypeArray type, boolean safe) {
        DBSPType sourceType = source.getType();
        if (sourceType.is(DBSPTypeVariant.class)) {
            if (type.getElementType().is(DBSPTypeBaseType.class)) {
                // Default conversion is fine
                return null;
            } else {
                // Convert to a Vector of VARIANT, and then...
                DBSPTypeArray vecVType = new DBSPTypeArray(DBSPTypeVariant.create(false), sourceType.mayBeNull);
                DBSPExpression vecV = source.cast(source.getNode(), vecVType, safe);
                // ...convert each element recursively to the target element type
                DBSPVariablePath var = vecVType.getElementType().ref().var();
                DBSPExpression convert = var.deref().cast(source.getNode(), type.getElementType(), safe).closure(var);
                source = new DBSPBinaryExpression(source.getNode(),
                        new DBSPTypeArray(type.getElementType(), sourceType.mayBeNull),
                        DBSPOpcode.ARRAY_CONVERT, vecV, convert);
            }
            return source.cast(source.getNode(), type, safe);
        } else if (sourceType.is(DBSPTypeArray.class)) {
            DBSPTypeArray sourceVecType = sourceType.to(DBSPTypeArray.class);
            // If the element type does not match, need to convert all elements
            if (!type.getElementType().equals(sourceVecType.getElementType())) {
                if (sourceVecType.getElementType().is(DBSPTypeAny.class)) {
                    // This can only happen if the source is an empty vector
                    return new DBSPArrayExpression(type, false);
                }
                DBSPVariablePath var = sourceVecType.getElementType().ref().var();
                DBSPExpression convert = var.deref();
                if (convert.getType().is(DBSPTypeBaseType.class))
                    convert = convert.applyCloneIfNeeded();
                convert = convert.cast(source.getNode(), type.getElementType(), safe).closure(var);
                source = new DBSPBinaryExpression(source.getNode(),
                        new DBSPTypeArray(type.getElementType(), sourceType.mayBeNull),
                        DBSPOpcode.ARRAY_CONVERT, source, convert);
            } else {
                this.unsupported(source, type);
            }
            return source.cast(source.getNode(), type, safe);
        } else if (sourceType.is(DBSPTypeNull.class)) {
            return new DBSPArrayExpression(type, true);
        } else {
            this.unsupported(source, type);
            // unreachable
            return source;
        }
    }

    DBSPClosureExpression converter(DBSPType source, DBSPType dest, boolean safe) {
        DBSPVariablePath var = source.ref().var();
        DBSPExpression convertValue = var.deref();
        if (convertValue.getType().is(DBSPTypeBaseType.class))
            convertValue = convertValue.applyCloneIfNeeded();
        return convertValue.cast(source.getNode(), dest, safe).closure(var);
    }

    @Nullable DBSPExpression convertToMap(DBSPExpression source, DBSPTypeMap type, boolean safe) {
        DBSPType sourceType = source.getType();
        if (sourceType.is(DBSPTypeMap.class)) {
            DBSPTypeMap sourceMap = sourceType.to(DBSPTypeMap.class);
            DBSPClosureExpression convertKey = converter(sourceMap.getKeyType(), type.getKeyType(), safe);
            DBSPClosureExpression convertValue = converter(sourceMap.getValueType(), type.getValueType(), safe);
            DBSPType convertedType = new DBSPTypeMap(type.getKeyType(), type.getValueType(), sourceType.mayBeNull);
            source = new DBSPBinaryExpression(source.getNode(),
                    convertedType, DBSPOpcode.MAP_CONVERT, source, new DBSPRawTupleExpression(convertKey, convertValue));
            return source.cast(source.getNode(), type, safe);
        }
        // Everything else use the default conversion
        return null;
    }

    @Override
    public VisitDecision preorder(DBSPCastExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.source);
        DBSPType sourceType = source.getType();
        DBSPType type = this.transform(expression.getType());
        DBSPExpression result = null;
        if (type.sameTypeIgnoringNullability(sourceType)) {
            if (type.sameType(sourceType)) {
                if (type.is(DBSPTypeDecimal.class))
                    // Do not remove such casts
                    result = expression;
                else
                    result = expression.source;
            } else if (type.mayBeNull) {
                // Cast from T to Option<T>
                result = expression.source.applyCloneIfNeeded().someIfNeeded();
            } else if (sourceType.mayBeNull && !type.is(DBSPTypeVariant.class)) {
                // Cast from Option<T> to T
                // Only VARIANT has a different implementation
                result = expression.source.unwrap().applyCloneIfNeeded();
            }
        } else if (type.is(DBSPTypeVariant.class)) {
            result = this.convertToVariant(source, type.mayBeNull);
        } else if (type.is(DBSPTypeArray.class)) {
            result = this.convertToVector(source, type.to(DBSPTypeArray.class), expression.safe);
        } else if (type.is(DBSPTypeTuple.class)) {
            result = this.convertToStruct(source, type.to(DBSPTypeTuple.class));
        } else if (type.is(DBSPTypeRawTuple.class)) {
            result = this.convertToTuple(source, type.to(DBSPTypeRawTuple.class));
        } else if (type.is(DBSPTypeMap.class)) {
            result = this.convertToMap(source, type.to(DBSPTypeMap.class), expression.safe);
        } else if (type.is(IsDateType.class) && source.getType().is(DBSPTypeBinary.class)) {
            throw new UnsupportedException(
                    "Cast function cannot convert BINARY value to " + type.asSqlString(), expression.getNode());
        }
        if (result == null)
            // Default implementation
            result = source.cast(expression.getNode(), type, expression.safe);
        this.pop(expression);
        Utilities.enforce(expression.hasSameType(result));
        this.map(expression, result);
        return VisitDecision.STOP;
    }
}
