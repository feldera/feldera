package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.ICastable;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** Given a value with a type keeps track of which fields of the type are being used. */
public class FieldUseMap {
    public static abstract class FieldInfo implements ICastable {
        static long crtId = 0;
        final long id;
        protected final DBSPType type;

        FieldInfo(DBSPType type) {
            this.id = crtId++;
            this.type = type;
        }

        DBSPType getType() {
            return this.type;
        }

        abstract boolean anyUsed();
        abstract boolean anyUnused();
        abstract int size();
        /** Mark all bits as used */
        abstract void setUsed();
        abstract int getCompressedIndex(int originalIndex);
        /** Type compressed up to the specified depth.
         * E.g, [X, [X, _], _]
         * - compressed to depth 1 is [X, [X, _]]
         * - compressed to depth 2 is [X, [X]] */
        abstract @Nullable DBSPType compressedType(int depth);
        /** Get an expression that contains all used fields
         *
         * @param from has a type of this.type.
         * @param depth Depth up to which unused fields are trimmed. */
        abstract @Nullable DBSPExpression allUsedFields(DBSPExpression from, int depth);

        /** Factory */
        static FieldInfo create(DBSPType type, boolean used) {
            if (type.is(DBSPTypeRef.class)) {
                DBSPType ref = type.to(DBSPTypeRef.class);
                FieldInfo field = FieldInfo.create(ref.deref(), used);
                return new Ref(type, field);
            } else if (type.is(DBSPTypeTupleBase.class)) {
                DBSPTypeTupleBase tuple = type.to(DBSPTypeTupleBase.class);
                List<FieldInfo> fields = Linq.map(Linq.list(tuple.tupFields), t -> FieldInfo.create(t, used));
                return new BitList(tuple, fields);
            } else {
                return new Atomic(type, used);
            }
        }

        public abstract FieldInfo reduce(FieldInfo fieldInfo);

        public abstract FieldInfo project(int depth);
    }

    /** A reference to a FieldInfo */
    static final class Ref extends FieldInfo {
        private final FieldInfo field;

        public Ref(DBSPType type, FieldInfo field) {
            super(type);
            this.field = field;
        }

        @Override
        public boolean anyUsed() {
            return this.field.anyUsed();
        }

        @Override
        public boolean anyUnused() {
            return this.field.anyUnused();
        }

        @Override
        public int size() {
            return this.field.size();
        }

        @Override
        public void setUsed() {
            this.field.setUsed();
        }

        @Override
        public int getCompressedIndex(int originalIndex) {
            throw new InternalCompilerError("Should not be called");
        }

        @Nullable @Override
        public DBSPType compressedType(int depth) {
            DBSPType type = this.field.compressedType(depth);
            if (type == null)
                return null;
            return type.ref();
        }

        @Nullable @Override
        public DBSPExpression allUsedFields(DBSPExpression from, int depth) {
            return this.field.allUsedFields(from.deref(), depth);
        }

        @Override
        public FieldInfo reduce(FieldInfo fieldInfo) {
            assert this.type.sameType(fieldInfo.type);
            return new Ref(this.type, this.field.reduce(fieldInfo.to(Ref.class).field));
        }

        @Override
        public FieldInfo project(int depth) {
            return new Ref(this.type, this.field.project(depth));
        }

        @Override
        public String toString() {
            return "Ref(" + this.field + ")";
        }
    }

    /** An atomic value */
    static final class Atomic extends FieldInfo {
        private boolean used;

        private Atomic(DBSPType type, boolean used) {
            super(type);
            this.used = used;
        }

        public void setUsed() {
            this.used = true;
        }

        @Override
        public DBSPType getType() {
            return this.type;
        }

        @Override
        public boolean anyUsed() {
            return this.used;
        }

        @Override
        public boolean anyUnused() {
            return !this.used;
        }

        @Override
        public String toString() {
            return this.used ? "X" : "_";
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public int getCompressedIndex(int originalIndex) {
            throw new RuntimeException("Compressed index of scalar type");
        }

        @Override
        public FieldInfo project(int depth) {
            return this;
        }

        @Nullable @Override
        public DBSPType compressedType(int depth) {
            if (!this.used)
                return null;
            return this.type;
        }

        @Override @Nullable
        public DBSPExpression allUsedFields(DBSPExpression from, int depth) {
            if (!this.used)
                return null;
            return from.applyCloneIfNeeded();
        }

        @Override
        public FieldInfo reduce(FieldInfo other) {
            assert this.type.sameType(other.type);
            Atomic oa = other.to(Atomic.class);
            return new Atomic(this.type, this.used || oa.used);
        }
    }

    static final class BitList extends FieldInfo {
        private final List<FieldInfo> fields;

        private BitList(DBSPTypeTupleBase type, List<FieldInfo> fields) {
            super(type);
            this.fields = fields;
        }

        boolean isRaw() {
            return this.getTupleType().isRaw();
        }

        public FieldInfo field(int index) {
            return this.fields.get(index);
        }

        @Override
        public DBSPType getType() {
            return this.type;
        }

        public void setUsed(int index) {
            this.fields.get(index).setUsed();
        }

        @Override
        public boolean anyUsed() {
            // Special case: pretend that an empty tuple is used
            if (this.size() == 0)
                return true;
            return Linq.any(this.fields, FieldInfo::anyUsed);
        }

        @Override
        public boolean anyUnused() {
            return Linq.any(this.fields, FieldInfo::anyUnused);
        }

        @Override
        public String toString() {
            return (this.isRaw() ? "R" : "") + Linq.map(this.fields, Object::toString);
        }

        @Override
        public int size() {
            return this.fields.size();
        }

        public int getCompressedSize() {
            int compressedSize = 0;
            for (int i = 0; i < this.size(); i++) {
                if (this.fields.get(i).anyUsed() || this.isRaw())
                    compressedSize++;
            }
            return compressedSize;
        }

        @Override
        public void setUsed() {
            for (FieldInfo info: this.fields) {
                info.setUsed();
            }
        }

        @Override
        public int getCompressedIndex(int originalIndex) {
            assert originalIndex < this.size();
            assert originalIndex >= 0;
            int index = originalIndex;
            for (int i = 0; i < originalIndex; i++) {
                if (!this.fields.get(i).anyUsed() && !this.isRaw()) {
                    index--;
                }
            }
            return index;
        }

        public DBSPTypeTupleBase getTupleType() {
            return this.type.to(DBSPTypeTupleBase.class);
        }

        @Override
        public DBSPType compressedType(int depth) {
            if (depth == 0)
                return this.type;
            List<DBSPType> fields = new ArrayList<>();
            for (int i = 0; i < this.size(); i++) {
                if (this.fields.get(i).anyUsed() || this.isRaw()) {
                    DBSPType type = this.fields.get(i).compressedType(depth - 1);
                    fields.add(type);
                }
            }
            return this.getTupleType().makeRelatedTupleType(fields);
        }

        @Override
        public DBSPExpression allUsedFields(DBSPExpression from, int depth) {
            boolean isRaw = this.getTupleType().isRaw();
            int size = depth <= 0 || isRaw ? this.size() : this.getCompressedSize();
            DBSPExpression[] fields = new DBSPExpression[size];
            int index = 0;
            for (int i = 0; i < this.size(); i++) {
                if (depth <= 0) {
                    fields[index++] = from.field(i).applyCloneIfNeeded();
                } else if (this.fields.get(i).anyUsed() || isRaw) {
                    // For raw tuples never discard fields, rather return Tup0.
                    fields[index++] = this.fields.get(i).allUsedFields(from.field(i), depth - 1);
                }
            }
            if (isRaw)
                return new DBSPRawTupleExpression(fields);
            else
                return new DBSPTupleExpression(this.type.mayBeNull, fields);
        }

        public List<Integer> allUsedFields() {
            List<Integer> result = new ArrayList<>();
            for (int i = 0; i < this.size(); i++)
                if (this.fields.get(i).anyUsed())
                    result.add(i);
            return result;
        }

        @Override
        public FieldInfo reduce(FieldInfo other) {
            assert this.type.sameType(other.type);
            BitList ol = other.to(BitList.class);
            assert this.size() == ol.size();
            List<FieldInfo> bits = Linq.zip(this.fields, ol.fields, FieldInfo::reduce);
            return new BitList(this.getTupleType(), bits);
        }

        @Override
        public FieldInfo project(int depth) {
            List<FieldInfo> fields = new ArrayList<>(this.size());
            for (int i = 0; i < this.size(); i++) {
                FieldInfo info = this.fields.get(i);
                if (depth > 0)
                    info = info.project(depth-1);
                else {
                    if (info.anyUsed())
                        info = FieldInfo.create(info.type, true);
                }
                fields.add(info);
            }
            return new BitList(this.getTupleType(), fields);
        }

        public FieldInfo slice(int start, int endExclusive) {
            assert (endExclusive >= start);
            DBSPTypeTupleBase type = this.getTupleType().slice(start, endExclusive);
            List<FieldInfo> fields = this.fields.subList(start, endExclusive);
            return new BitList(type, fields);
        }
    }

    private final FieldInfo fieldInfo;

    /** Crate a FieldUseMap where all the bits have the same value.
     *
     * @param type  Type of the fields represented
     * @param used  IF true all fields are used, else they are all unused. */
    public FieldUseMap(DBSPType type, boolean used) {
        this.fieldInfo = FieldInfo.create(type, used);
    }

    private FieldUseMap(FieldInfo info) {
        this.fieldInfo = info;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean hasUnusedFields(int depth) {
        return this.fieldInfo.project(depth).anyUnused();
    }

    public boolean isEmpty() {
        return !this.fieldInfo.anyUsed();
    }

    static FieldUseMap identity(DBSPType type) {
        return new FieldUseMap(type, true);
    }

    public int getNewIndex(int originalIndex) {
        return this.fieldInfo.getCompressedIndex(originalIndex);
    }

    public FieldUseMap field(int fieldNo) {
        return new FieldUseMap(this.fieldInfo.to(BitList.class).field(fieldNo));
    }

    /** Compress type only to the specified depth */
    @Nullable DBSPType compressedType(int depth) {
        return this.fieldInfo.compressedType(depth);
    }

    public DBSPType getType() {
        return this.fieldInfo.getType();
    }

    /** Get a projection that only preserves the used bits */
    @Nullable
    public DBSPClosureExpression getProjection(int depth) {
        DBSPVariablePath var = this.getType().var();
        DBSPExpression body = this.fieldInfo.allUsedFields(var, depth);
        if (body == null)
            return null;
        return body.closure(var.asParameter());
    }

    @Override
    public String toString() {
        return this.fieldInfo.toString();
    }

    public int size() {
        return this.fieldInfo.size();
    }

    public void setUsed() {
        this.fieldInfo.setUsed();
    }

    public void setUsed(int index) {
        this.fieldInfo.to(BitList.class).setUsed(index);
    }

    public boolean isUsed(int index) {
        return this.fieldInfo.to(BitList.class).fields.get(index).anyUsed();
    }

    public List<Integer> getUsedFields() {
        return this.fieldInfo.to(BitList.class).allUsedFields();
    }

    public FieldUseMap borrow() {
        return new FieldUseMap(new Ref(this.getType().ref(), this.fieldInfo));
    }

    public FieldUseMap deref() {
        return new FieldUseMap(this.fieldInfo.to(Ref.class).field);
    }

    public FieldUseMap reduce(FieldUseMap with) {
        return new FieldUseMap(this.fieldInfo.reduce(with.fieldInfo));
    }

    public static FieldUseMap reduce(List<FieldUseMap> maps) {
        assert !maps.isEmpty();
        FieldUseMap current = maps.get(0);
        for (int i = 1; i < maps.size(); i++) {
            current = current.reduce(maps.get(i));
        }
        return current;
    }

    public FieldUseMap slice(int start, int endExclusive) {
        return new FieldUseMap(this.fieldInfo.to(BitList.class).slice(start, endExclusive));
    }
}
