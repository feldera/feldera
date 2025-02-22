package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI128Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPKeywordLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPRealLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU128Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUuidLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBinary;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.util.JsonStream;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Serializes an inner node as a JSON string */
public class ToJsonInnerVisitor extends InnerVisitor {
    final JsonStream stream;
    final int verbosity;
    final Set<Long> serialized;

    public ToJsonInnerVisitor(DBSPCompiler compiler, JsonStream stream, int verbosity) {
        super(compiler);
        this.stream = stream;
        this.verbosity = verbosity;
        this.serialized = new HashSet<>();
    }

    boolean checkDone(IDBSPInnerNode node, boolean silent) {
        if (this.serialized.contains(node.getId())) {
            if (silent)
                return true;
            this.stream.beginObject()
                    .label("node")
                    .append(node.getId())
                    .endObject();
            return true;
        }
        return false;
    }

    @Override
    public void startArrayProperty(String property) {
        this.stream.label(property).beginArray();
    }

    @Override
    public void endArrayProperty(String property) {
        this.stream.endArray();
    }

    @Override
    public void property(String name) {
        this.stream.label(name);
    }

    @Override
    public void push(IDBSPInnerNode node) {
        if (!this.checkDone(node, true)) {
            this.property("class");
            this.stream.append(node.getClass().getSimpleName());
            this.property("id");
            this.stream.append(node.getId());
            this.serialized.add(node.getId());
        }
        super.push(node);
    }

    @Override
    public VisitDecision preorder(IDBSPInnerNode node) {
        if (this.checkDone(node, false))
            return VisitDecision.STOP;
        this.stream.beginObject();
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(IDBSPInnerNode node) {
        this.stream.endObject();
    }

    @Override
    public void postorder(DBSPAggregate node) {
        this.property("isLinear");
        this.stream.append(node.isLinear());
        this.stream.endObject();
    }

    @Override
    public void postorder(DBSPBinaryLiteral node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(Arrays.toString(node.value));
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPBoolLiteral node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value);
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPComment node) {
        this.property("comment");
        this.stream.append(node.comment);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPDateLiteral node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value);
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPDecimalLiteral node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPDirectComparatorExpression node) {
        this.property("ascending");
        this.stream.append(node.ascending);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPDoubleLiteral node) {
        if (node.value != null) {
            this.property("value");
            long exact = Double.doubleToRawLongBits(node.value);
            this.stream.append(Long.toUnsignedString(exact));
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPEnumValue node) {
        this.property("enumName");
        this.stream.append(node.enumName);
        this.property("constructor");
        this.stream.append(node.constructor);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPFieldExpression node) {
        this.property("fieldNo");
        this.stream.append(node.fieldNo);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPFlatmap node) {
        this.startArrayProperty("leftInputIndexes");
        int index = 0;
        for (int i: node.leftInputIndexes) {
            this.propertyIndex(index);
            index++;
            this.stream.append(i);
        }
        this.endArrayProperty("leftInputIndexes");
        this.property("shuffle");
        this.stream.append(node.shuffle.toString());
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPFunction node) {
        this.property("name");
        this.stream.append(node.name);
        this.startArrayProperty("annotations");
        for (int i = 0; i < node.annotations.size(); i++) {
            this.propertyIndex(i);
            this.stream.append(node.annotations.get(i));
        }
        this.endArrayProperty("annotations");
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPI128Literal node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPI16Literal node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPI32Literal node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPI64Literal node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPI8Literal node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPIdentifierPattern node) {
        this.property("identifier");
        this.stream.append(node.identifier);
        this.property("mutable");
        this.stream.append(node.mutable);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPIntervalMillisLiteral node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPISizeLiteral node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPKeywordLiteral node) {
        this.property("keyword");
        this.stream.append(node.keyword);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPLetStatement node) {
        this.property("mutable");
        this.stream.append(node.mutable);
        this.property("variable");
        this.stream.append(node.variable);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPParameter node) {
        this.property("name");
        this.stream.append(node.name);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPRealLiteral node) {
        if (node.value != null) {
            this.property("value");
            int exact = Float.floatToRawIntBits(node.value);
            this.stream.append(exact);
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPSimplePathSegment node) {
        this.property("identifier");
        this.stream.append(node.identifier);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPStringLiteral node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value);
            this.property("charset");
            this.stream.append(node.charset.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPStrLiteral node) {
        this.property("value");
        this.stream.append(node.value);
        this.property("raw");
        this.stream.append(node.raw);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPTimeLiteral node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPTimestampLiteral node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPTypeBinary node) {
        this.property("precision");
        this.stream.append(node.precision);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPType node) {
        this.property("code");
        this.stream.append(node.code.toString());
        this.property("mayBeNull");
        this.stream.append(node.mayBeNull);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPTypeDecimal node) {
        this.property("precision");
        this.stream.append(node.precision);
        this.property("scale");
        this.stream.append(node.scale);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPTypeInteger node) {
        this.property("width");
        this.stream.append(node.getWidth());
        this.property("signed");
        this.stream.append(node.signed);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPTypeMillisInterval node) {
        this.property("units");
        this.stream.append(node.units.toString());
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPTypeStruct node) {
        this.property("sanitizedName");
        this.stream.append(node.sanitizedName);
        this.property("name");
        this.stream.append(node.name.toString());
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPU128Literal node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPU16Literal node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPU32Literal node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPU64Literal node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    void typeSequence(DBSPUnsignedWrapExpression.TypeSequence sequence) {
        this.property("dataType");
        sequence.dataType.accept(this);
        this.property("intermediateType");
        sequence.intermediateType.accept(this);
        this.property("dataConvertedType");
        sequence.dataConvertedType.accept(this);
        this.property("unsignedType");
        sequence.unsignedType.accept(this);
    }

    @Override
    public void postorder(DBSPUnsignedUnwrapExpression node) {
        this.property("nullsLast");
        this.stream.append(node.nullsLast);
        this.property("ascending");
        this.stream.append(node.ascending);
        this.typeSequence(node.sequence);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPUnsignedWrapExpression node) {
        this.property("nullsLast");
        this.stream.append(node.nullsLast);
        this.property("ascending");
        this.stream.append(node.ascending);
        this.typeSequence(node.sequence);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPUSizeLiteral node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPUuidLiteral node) {
        if (node.value != null) {
            this.property("value");
            this.stream.append(node.value.toString());
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPVariablePath node) {
        this.property("variable");
        this.stream.append(node.variable);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPVariantExpression node) {
        this.property("isSqlNull");
        this.stream.append(node.isSqlNull);
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPWindowBoundExpression node) {
        this.property("isPreceding");
        this.stream.append(node.isPreceding);
        super.postorder(node);
    }

    public String getJsonString() {
        return this.stream.toString();
    }
}
