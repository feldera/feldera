package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsDateType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
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

    static void unsupported(DBSPExpression source, DBSPType type) {
        throw new UnsupportedException("Casting of value with type '" +
                source.getType().asSqlString() +
                "' to the target type '" + type.asSqlString() + "' not supported", source.getNode());
    }

    @Override
    public VisitDecision preorder(DBSPType type) {
        return VisitDecision.STOP;
    }

    static @Nullable DBSPExpression convertToVariant(CalciteObject node, DBSPExpression source, boolean mayBeNull) {
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
                DBSPExpression rec = field.cast(node, DBSPTypeVariant.create(false), false);
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
                    .applyCloneIfNeeded().cast(node, DBSPTypeVariant.create(false), false)
                    .closure(var);
            expression = new DBSPBinaryExpression(node,
                    new DBSPTypeArray(DBSPTypeVariant.create(false), vecType.mayBeNull),
                    DBSPOpcode.ARRAY_CONVERT, source, converter);
        } else {
            return null;
        }
        return new DBSPCastExpression(node, expression, DBSPTypeVariant.create(mayBeNull), false);
    }

    public static DBSPExpression convertToStructOrTuple(CalciteObject node, DBSPExpression source, DBSPTypeTuple type) {
        List<DBSPExpression> fields = new ArrayList<>();
        DBSPTypeStruct struct = type.originalStruct;
        List<ProgramIdentifier> names = null;
        if (struct != null)
            names = Linq.list(type.originalStruct.getFieldNames());

        DBSPType sourceType = source.getType();
        for (int i = 0; i < type.size(); i++) {
            DBSPType fieldType = type.getFieldType(i);
            DBSPExpression index;
            if (sourceType.is(DBSPTypeTupleBase.class)) {
                index = source.field(i);
            } else if (sourceType.is(DBSPTypeVariant.class)) {
                if (struct == null) {
                    index = new DBSPBinaryExpression(
                            // Result of index is always nullable
                            node, DBSPTypeVariant.create(true),
                            DBSPOpcode.RUST_INDEX, source.applyCloneIfNeeded(),
                            new DBSPUSizeLiteral(i));
                } else {
                    ProgramIdentifier fieldName = names.get(i);
                    index = new DBSPBinaryExpression(
                            // Result of index is always nullable
                            node, DBSPTypeVariant.create(true),
                            DBSPOpcode.VARIANT_INDEX, source.applyCloneIfNeeded(),
                            new DBSPStringLiteral(fieldName.toString()));
                }
            } else {
                 throw new InternalCompilerError("Unexpected source type " + sourceType);
            }
            DBSPExpression expression;
            if (fieldType.is(DBSPTypeTuple.class)) {
                expression = convertToStructOrTuple(node, index.applyClone(), fieldType.to(DBSPTypeTuple.class));
            } else {
                expression = index.applyCloneIfNeeded().cast(node, fieldType, false);
            }
            fields.add(expression);
        }

        DBSPExpression result = new DBSPTupleExpression(source.getNode(), type, fields);
        if (source.getType().mayBeNull) {
            if (type.mayBeNull) {
                result = new DBSPIfExpression(node, source.is_null(), type.nullValue(), result);
            } else {
                result = new DBSPIfExpression(node, source.is_null(),
                        // Causes a runtime panic
                        type.withMayBeNull(true).nullValue().unwrap(), result);
            }
        }
        return result;
    }

    public static DBSPExpression convertToTuple(CalciteObject node, DBSPExpression source, DBSPTypeRawTuple type) {
        List<DBSPExpression> fields = new ArrayList<>();
        DBSPType sourceType = source.getType();
        if (!sourceType.is(DBSPTypeRawTuple.class))
            throw new InternalCompilerError("Cast to RAW tuple from " + sourceType);
        for (int i = 0; i < type.size(); i++) {
            DBSPType fieldType = type.getFieldType(i);
            DBSPExpression index = source.field(i);
            DBSPExpression expression;
            if (fieldType.is(DBSPTypeTuple.class)) {
                expression = convertToStructOrTuple(node, index.applyClone(), fieldType.to(DBSPTypeTuple.class));
            } else {
                expression = index.applyCloneIfNeeded().cast(node, fieldType, false);
            }
            fields.add(expression);
        }
        return new DBSPRawTupleExpression(fields);
    }

    static @Nullable DBSPExpression convertToVector(CalciteObject node, DBSPExpression source, DBSPTypeArray type, boolean safe) {
        DBSPType sourceType = source.getType();
        if (sourceType.is(DBSPTypeVariant.class)) {
            if (type.getElementType().is(DBSPTypeBaseType.class)) {
                // Default conversion is fine
                return null;
            } else {
                // Convert to a Vector of VARIANT, and then...
                DBSPTypeArray vecVType = new DBSPTypeArray(DBSPTypeVariant.create(false), sourceType.mayBeNull);
                DBSPExpression vecV = source.cast(node, vecVType, safe);
                // ...convert each element recursively to the target element type
                DBSPVariablePath var = vecVType.getElementType().ref().var();
                DBSPExpression convert = var.deref().cast(node, type.getElementType(), safe).closure(var);
                source = new DBSPBinaryExpression(node,
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
                convert = convert.cast(node, type.getElementType(), safe).closure(var);
                source = new DBSPBinaryExpression(node,
                        new DBSPTypeArray(type.getElementType(), sourceType.mayBeNull),
                        DBSPOpcode.ARRAY_CONVERT, source, convert);
            } else {
                unsupported(source, type);
            }
            return source.cast(node, type, safe);
        } else if (sourceType.is(DBSPTypeNull.class)) {
            return new DBSPArrayExpression(type, true);
        } else {
            unsupported(source, type);
            // unreachable
            return source;
        }
    }

    static DBSPClosureExpression converter(DBSPType source, DBSPType dest, boolean safe) {
        DBSPVariablePath var = source.ref().var();
        DBSPExpression convertValue = var.deref();
        if (convertValue.getType().is(DBSPTypeBaseType.class))
            convertValue = convertValue.applyCloneIfNeeded();
        return convertValue.cast(source.getNode(), dest, safe).closure(var);
    }

    static @Nullable DBSPExpression convertToMap(CalciteObject node, DBSPExpression source, DBSPTypeMap type, boolean safe) {
        DBSPType sourceType = source.getType();
        if (sourceType.is(DBSPTypeMap.class)) {
            DBSPTypeMap sourceMap = sourceType.to(DBSPTypeMap.class);
            DBSPClosureExpression convertKey = converter(sourceMap.getKeyType(), type.getKeyType(), safe);
            DBSPClosureExpression convertValue = converter(sourceMap.getValueType(), type.getValueType(), safe);
            DBSPType convertedType = new DBSPTypeMap(type.getKeyType(), type.getValueType(), sourceType.mayBeNull);
            source = new DBSPBinaryExpression(node,
                    convertedType, DBSPOpcode.MAP_CONVERT, source, new DBSPRawTupleExpression(convertKey, convertValue));
            return source.cast(node, type, safe);
        }
        // Everything else use the default conversion
        return null;
    }

    @Override
    public VisitDecision preorder(DBSPCastExpression expression) {
        CalciteObject node = expression.getNode();
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
            result = convertToVariant(node, source, type.mayBeNull);
        } else if (type.is(DBSPTypeArray.class)) {
            result = convertToVector(node, source, type.to(DBSPTypeArray.class), expression.safe);
        } else if (type.is(DBSPTypeTuple.class)) {
            result = convertToStructOrTuple(node, source, type.to(DBSPTypeTuple.class));
        } else if (type.is(DBSPTypeRawTuple.class)) {
            result = convertToTuple(node, source, type.to(DBSPTypeRawTuple.class));
        } else if (type.is(DBSPTypeMap.class)) {
            result = convertToMap(node, source, type.to(DBSPTypeMap.class), expression.safe);
        } else if (type.is(IsDateType.class) && source.getType().is(DBSPTypeBinary.class)) {
            throw new UnsupportedException(
                    "Cast function cannot convert BINARY value to " + type.asSqlString(), expression.getNode());
        }
        if (result == null)
            // Default implementation
            result = source.cast(node, type, expression.safe);
        this.pop(expression);
        Utilities.enforce(expression.hasSameType(result));
        this.map(expression, result);
        return VisitDecision.STOP;
    }
}
