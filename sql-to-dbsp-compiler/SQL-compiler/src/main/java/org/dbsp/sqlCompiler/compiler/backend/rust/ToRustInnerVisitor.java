/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.apache.calcite.util.TimeString;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAsExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalAggregateExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEnumValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPForExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPQualifyTypeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPQuestionExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSortExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPGeoPointLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI128Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIndexedZSetLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
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
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPPathSegment;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPConstItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStream;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

import java.util.Map;
import java.util.Objects;

/** This visitor generates a Rust implementation of the program. */
public class ToRustInnerVisitor extends InnerVisitor {
    protected final IndentStream builder;
    /** If set use a more compact display, which is not necessarily compilable. */
    protected final boolean compact;
    protected final CompilerOptions options;

    public ToRustInnerVisitor(IErrorReporter reporter, IndentStream builder,
                              CompilerOptions options, boolean compact) {
        super(reporter);
        this.builder = builder;
        this.compact = compact;
        this.options = options;
    }

    @SuppressWarnings("SameReturnValue")
    VisitDecision doNullExpression(DBSPExpression expression) {
        this.builder.append("None::<");
        expression.getType().setMayBeNull(false).accept(this);
        this.builder.append(">");
        return VisitDecision.STOP;
    }

    public VisitDecision doNull(DBSPLiteral literal) {
        if (!literal.isNull)
            throw new UnsupportedException(literal.getNode());
        return this.doNullExpression(literal);
    }

    @Override
    public VisitDecision preorder(DBSPNullLiteral literal) {
        this.builder.append("None::<()>");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSortExpression expression) {
        /*
        move |(k, v): (&(), &Vec<Tup<...>>, ), | -> Vec<Tup<...>> {
            let comp = ...;    // comparator
            let mut ec: _ = move |a: &Tup<...>, b: &Tup<...>, | -> _ {
                comp.compare(a, b)
            };
            let mut v = v.clone();
            v.sort_by(ec);
            v
        }
         */
        this.builder.append("move |(k, v): (&(), &Vec<");
        expression.elementType.accept(this);
        this.builder.append(">)| -> Vec<");
        expression.elementType.accept(this);
        this.builder.append("> {").increase();
        this.builder.append("let ec = ");
        expression.comparator.accept(this);
        this.builder.append(";").newline();
        this.builder.append("let comp = move |a: &");
        expression.elementType.accept(this);
        this.builder.append(", b: &");
        expression.elementType.accept(this);
        this.builder.append("| { ec.compare(a, b) };");
        this.builder.append("let mut v = v.clone();").newline()
                // we don't use sort_unstable_by because it is
                // non-deterministic
                .append("v.sort_by(comp);").newline()
                .append("v").newline()
                .decrease()
                .append("}");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSomeExpression expression) {
        this.builder.append("Some(");
        expression.expression.accept(this);
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    void codegen(DBSPUnsignedWrapExpression.TypeSequence sequence) {
        this.builder.append("<");
        // In the type parameter we do not put the Option<>
        sequence.dataType.setMayBeNull(false).accept(this);
        this.builder.append(", ");
        sequence.dataConvertedType.accept(this);
        this.builder.append(", ");
        sequence.intermediateType.accept(this);
        this.builder.append(", ");
        sequence.unsignedType.accept(this);
        this.builder.append(">");
    }

    @Override
    public VisitDecision preorder(DBSPUnsignedWrapExpression expression) {
        this.builder.append("UnsignedWrapper")
                .append("::")
                .append(expression.getMethod())
                .append("::");
        this.codegen(expression.sequence);
        this.builder.append("(");
        expression.source.accept(this);
        this.builder.append(", ")
                .append(Boolean.toString(expression.ascending))
                .append(", ")
                .append(Boolean.toString(expression.nullsLast))
                .append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnsignedUnwrapExpression expression) {
        this.builder.append("UnsignedWrapper")
                .append("::")
                .append(expression.getMethod())
                .append("::");
        this.codegen(expression.sequence);
        this.builder.append("(");
        expression.source.accept(this);
        this.builder.append(", ")
                .append(Boolean.toString(expression.ascending))
                .append(", ")
                .append(Boolean.toString(expression.nullsLast))
                .append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldComparatorExpression expression) {
        expression.source.accept(this);
        boolean hasSource = expression.source.is(DBSPFieldComparatorExpression.class);
        if (hasSource)
            this.builder.append(".then(");
        this.builder.append("Extract::new(move |r: &");
        expression.tupleType().accept(this);
        this.builder.append("| r.")
                .append(expression.fieldNo);
        if (!expression.tupleType().to(DBSPTypeTuple.class).getFieldType(expression.fieldNo).hasCopy())
            this.builder.append(".clone()");
        this.builder.append(")");
        if (!expression.ascending)
            this.builder.append(".rev()");
        if (hasSource)
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTimestampLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("Timestamp::new(")
                .append(Long.toString(Objects.requireNonNull(literal.value)))
                .append("/*")
                .append(Objects.requireNonNull(literal.getTimestampString()).toString())
                .append("*/")
                .append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDateLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("Date::new(")
                .append(Integer.toString(Objects.requireNonNull(literal.value)))
                .append("/*")
                .append(Objects.requireNonNull(literal.getDateString()).toString())
                .append("*/")
                .append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTimeLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        TimeString ts = Objects.requireNonNull(literal.value);
        this.builder.append("Time::new(")
                .append(Utilities.timeStringToNanoseconds(ts))
                .append("/*")
                .append(ts.toString())
                .append("*/")
                .append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntervalMonthsLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("LongInterval::new(")
                .append(Integer.toString(Objects.requireNonNull(literal.value)))
                .append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntervalMillisLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("ShortInterval::new(")
                .append(Long.toString(Objects.requireNonNull(literal.value)))
                .append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPGeoPointLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("GeoPoint::new(");
        Objects.requireNonNull(literal.left).accept(this);
        this.builder.append(", ");
        Objects.requireNonNull(literal.right).accept(this);
        this.builder.append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBoolLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        this.builder.append(literal.wrapSome(Boolean.toString(Objects.requireNonNull(literal.value))));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVecLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("vec!(")
                .increase();
        for (DBSPExpression exp: Objects.requireNonNull(literal.data)) {
            exp.accept(this);
            this.builder.append(", ");
        }
        this.builder.decrease().append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBinaryLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("ByteArray::new(&vec!(")
                .increase();
        for (byte b: Objects.requireNonNull(literal.value)) {
            this.builder.append(b & 0xff)
                    .append(" as u8, ");
        }
        this.builder.decrease().append("))");
        if (literal.mayBeNull())
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPZSetLiteral literal) {
        this.builder.append("zset!(")
                .increase();
        for (Map.Entry<DBSPExpression, Long> e: literal.data.entrySet()) {
            e.getKey().accept(this);
            this.builder.append(" => ")
                    .append(e.getValue())
                    .append(",\n");
        }
        this.builder.decrease().append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIndexedZSetLiteral literal) {
        this.builder.append("indexed_zset!(")
                .append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPRealLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        float value = Objects.requireNonNull(literal.value);
        String val = Float.toString(value);
        if (Float.isNaN(value))
            val = "std::f32::NAN";
        else if (Float.isInfinite(value)) {
            if (value < 0)
                val = "std::f32::NEG_INFINITY";
            else
                val = "std::f32::INFINITY";
        }
        int exact = Float.floatToRawIntBits(value);
        String f32 = "f32::from_bits(" +
            Integer.toUnsignedString(exact) + "u32)";

        String out = literal.raw ? f32 : literal.wrapSome("F32::new(" + f32 + ")");
        this.builder.append(out);
        this.builder.append("/*")
                .append(val)
                .append("*/");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPComment comment) {
        if (comment.comment.contains("\n")) {
            this.builder.append("/* ")
                    .newline()
                    .append(comment.comment)
                    .append(" */");
        } else {
            this.builder.append("// ")
                    .append(comment.comment)
                    .newline();
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDoubleLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        double value = Objects.requireNonNull(literal.value);
        String val = Double.toString(value);
        if (Double.isNaN(value))
            val = "std::f64::NAN";
        else if (Double.isInfinite(value)) {
            if (value < 0)
                val = "std::f64::NEG_INFINITY";
            else
                val = "std::f64::INFINITY";
        }
        long exact = Double.doubleToRawLongBits(value);
        String f64 = "f64::from_bits(" +
                Long.toUnsignedString(exact) + "u64)";
        String out = literal.raw ? f64 : literal.wrapSome("F64::new(" + f64 + ")");
        this.builder.append(out);
        this.builder.append("/*")
                .append(val)
                .append("*/");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUSizeLiteral literal) {
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + "usize"));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPISizeLiteral literal) {
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + "isize"));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI8Literal literal) {
        if (literal.isNull)
            return this.doNull(literal);
        String val = Byte.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }


    @Override
    public VisitDecision preorder(DBSPI16Literal literal) {
        if (literal.isNull)
            return this.doNull(literal);
        String val = Short.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI32Literal literal) {
        if (literal.isNull)
            return this.doNull(literal);
        String val = Integer.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU16Literal literal) {
        String val = Integer.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU32Literal literal) {
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI64Literal literal) {
        if (literal.isNull)
            return this.doNull(literal);
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI128Literal literal) {
        if (literal.isNull)
            return this.doNull(literal);
        String val = Objects.requireNonNull(literal.value).toString();
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU64Literal literal) {
        String val = Objects.requireNonNull(literal.value).toString();
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU128Literal literal) {
        String val = Objects.requireNonNull(literal.value).toString();
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStringLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        Objects.requireNonNull(literal.value);
        byte[] bytes = literal.value.getBytes(literal.charset);
        String decoded = new String(bytes, literal.charset);
        decoded = Utilities.doubleQuote(decoded);
        this.builder.append(literal.wrapSome(
        "String::from(" + decoded + ")"));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStrLiteral literal) {
        if (literal.isNull) {
            this.builder.append("None");
            return VisitDecision.STOP;
        }
        Objects.requireNonNull(literal.value);
        if (literal.raw) {
            String contents = "r#\"" + literal.value + "\"#";
            this.builder.append(literal.wrapSome(contents));
        } else {
            this.builder.append(literal.wrapSome(Utilities.doubleQuote(literal.value)));
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDecimalLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        DBSPTypeDecimal type = literal.getType().to(DBSPTypeDecimal.class);
        if (type.mayBeNull)
            this.builder.append("Some(");
        String value = Objects.requireNonNull(literal.value).toPlainString();
        this.builder.append("new_decimal(\"")
                .append(value)
                .append("\", ")
                .append(type.precision)
                .append(", ")
                .append(type.scale)
                .append(").unwrap()");
        if (type.mayBeNull)
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCastExpression expression) {
        /* Default implementation of cast of a source expression to the 'this' type.
         * For example, to cast source which is an Option[i16] to a bool
         * the function called will be cast_to_b_i16N. */
        DBSPType destType = expression.getType();
        DBSPType sourceType = expression.source.getType();
        if (destType.sameType(sourceType)) {
            expression.source.accept(this);
            return VisitDecision.STOP;
        }

        // Handle cast Vec<i> to Vec<i>?
        if (sourceType.is(DBSPTypeVec.class)) {
            DBSPTypeVec sourceVec = sourceType.to(DBSPTypeVec.class);
            DBSPTypeVec destVec = destType.as(DBSPTypeVec.class);
            if (destVec == null)
                throw new UnsupportedException("Cast from " + sourceType + " to " + destType, expression.getNode());
            // TODO: This should probably be handled in Simplify
            if (destVec.getElementType().sameType(sourceVec.getElementType()) &&
                destVec.mayBeNull && !sourceVec.mayBeNull) {
                // TODO: This can happen when source is an empty vector literal with unknown type.
                // Can anything else happen?
                expression.source.some().accept(this);
            } else {
                expression.source.accept(this);
            }
            return VisitDecision.STOP;
        }

        String functionName = "cast_to_" + destType.baseTypeWithSuffix() +
                "_" + sourceType.baseTypeWithSuffix();
        this.builder.append(functionName).append("(");
        expression.source.accept(this);
        DBSPTypeDecimal dec = destType.as(DBSPTypeDecimal.class);
        if (dec != null) {
            // pass precision and scale as arguments to cast method too
            this.builder.append(", ")
                    .append(dec.precision)
                    .append(", ")
                    .append(dec.scale);
        }
        DBSPTypeString str = destType.as(DBSPTypeString.class);
        if (str != null) {
            // pass precision and scale as arguments to cast method too
            this.builder.append(", ")
                    .append(str.precision)
                    .append(", ")
                    .append(Boolean.toString(str.fixed));
        }
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConditionalAggregateExpression expression) {
        RustSqlRuntimeLibrary.FunctionDescription implementation = RustSqlRuntimeLibrary.INSTANCE.getImplementation(
                expression.opcode, expression.getType(), expression.left.getType(), expression.right.getType());
        this.builder.append(implementation.function);
        if (expression.condition != null)
            this.builder.append("_conditional");
        this.builder.append("(");
        expression.left.accept(this);
        this.builder.append(", ");
        expression.right.accept(this);
        if (expression.condition != null) {
            this.builder.append(", ");
            expression.condition.accept(this);
        }
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBinaryExpression expression) {
        if (expression.operation == DBSPOpcode.MUL_WEIGHT) {
            expression.left.accept(this);
            this.builder.append(".mul_by_ref(&");
            expression.right.accept(this);
            this.builder.append(")");
            return VisitDecision.STOP;
        } else if (expression.operation == DBSPOpcode.RUST_INDEX) {
            expression.left.accept(this);
            this.builder.append("[");
            expression.right.accept(this);
            this.builder.append("]");
            return VisitDecision.STOP;
        } else if(expression.operation == DBSPOpcode.SQL_INDEX) {
            DBSPTypeVec vec = expression.left.getType().to(DBSPTypeVec.class);
            this.builder.append("index")
                    .append("_")
                    .append(expression.left.getType().nullableSuffix())
                    .append("_")
                    .append(vec.getElementType().nullableSuffix())
                    .append("_")
                    .append(expression.right.getType().nullableSuffix())
                    .append("(");
            expression.left.accept(this);
            this.builder.append(", ");
            expression.right.accept(this);
            this.builder.append(" - 1)");
            return VisitDecision.STOP;
        } else if (expression.operation == DBSPOpcode.DIV_NULL) {
            this.builder.append("div_null")
                    .append(expression.left.getType().nullableSuffix())
                    .append(expression.right.getType().nullableSuffix())
                    .append("(");
            expression.left.accept(this);
            this.builder.append(", ");
            expression.right.accept(this);
            this.builder.append(")");
            return VisitDecision.STOP;
        }
        RustSqlRuntimeLibrary.FunctionDescription function = RustSqlRuntimeLibrary.INSTANCE.getImplementation(
                expression.operation,
                expression.getType(),
                expression.left.getType(),
                expression.right.getType());
        String func = function.function;
        this.builder.append(func).append("(");
        expression.left.accept(this);
        this.builder.append(", ");
        expression.right.accept(this);
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIsNullExpression expression) {
        if (!expression.expression.getType().mayBeNull) {
            this.builder.append("false");
        } else {
            expression.expression.accept(this);
            this.builder.append(".is_none()");
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCloneExpression expression) {
        expression.expression.accept(this);
        this.builder.append(".clone()");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnaryExpression expression) {
        if (expression.operation == DBSPOpcode.WRAP_BOOL ||
            expression.operation == DBSPOpcode.INDICATOR) {
            this.builder.append(expression.operation.toString());
            if (expression.operation == DBSPOpcode.INDICATOR) {
                this.builder.append("::<_, ");
                expression.getType().accept(this);
                this.builder.append(">");
            }
            this.builder.append("(");
            expression.source.accept(this);
            this.builder.append(")");
            return VisitDecision.STOP;
        } else if (expression.operation.toString().startsWith("is_")) {
            this.builder.append(expression.operation.toString())
                    .append("_")
                    .append(expression.source.getType().baseTypeWithSuffix())
                    .append("_(");
            expression.source.accept(this);
            this.builder.append(")");
            return VisitDecision.STOP;
        }
        if (expression.source.getType().mayBeNull) {
            this.builder.append("(")
                    .append("match ");
            expression.source.accept(this);
            this.builder.append(" {").increase()
                    .append("Some(x) => Some(")
                    .append(expression.operation.toString())
                    .append("(x)),\n")
                    .append("_ => None,\n")
                    .decrease()
                    .append("}")
                    .append(")");
        } else {
            this.builder.append("(")
                    .append(expression.operation.toString());
            expression.source.accept(this);
            this.builder.append(")");
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath expression) {
        this.builder.append(expression.variable);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConstItem item) {
        this.builder.append("const ")
                .append(item.name).append(": ");
        item.type.accept(this);
        if (item.expression != null) {
            this.builder.append(" = ");
            item.expression.accept(this);
        }
        this.builder.append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPExpressionStatement statement) {
        statement.expression.accept(this);
        this.builder.append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPLetStatement statement) {
        this.builder.append("let ")
                .append(statement.mutable ? "mut " : "")
                .append(statement.variable);
        if (!statement.type.is(DBSPTypeAny.class)) {
            this.builder.append(": ");
            statement.type.accept(this);
        }
        if (statement.initializer != null) {
            this.builder.append(" = ");
            statement.initializer.accept(this);
        }
        this.builder.append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFunction function) {
        this.builder.intercalateS("\n", function.annotations)
                .append("pub fn ")
                .append(function.name)
                .append("(");
        boolean first = true;
        for (DBSPParameter param: function.parameters) {
            if (!first)
                this.builder.append(", ");
            first = false;
            param.accept(this);
        }
        this.builder.append(") ");
        if (!function.returnType.is(DBSPTypeVoid.class)) {
            builder.append("-> ");
            function.returnType.accept(this);
            builder.append(" ");
        }
        if (function.body.is(DBSPBlockExpression.class)) {
            function.body.accept(this);
        } else {
            this.builder.append("{").increase();
            function.body.accept(this);
            this.builder.decrease()
                    .newline()
                    .append("}");
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPApplyExpression expression) {
        expression.function.accept(this);
        this.builder.append("(");
        boolean first = true;
        for (DBSPExpression arg: expression.arguments) {
            if (!first)
                this.builder.append(", ");
            first = false;
            arg.accept(this);
        }
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPApplyMethodExpression expression) {
        expression.self.accept(this);
        this.builder.append(".");
        expression.function.accept(this);
        this.builder.append("(");
        boolean first = true;
        for (DBSPExpression arg: expression.arguments) {
            if (!first)
                this.builder.append(", ");
            first = false;
            arg.accept(this);
        }
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPQuestionExpression expression) {
        this.builder.append("(");
        expression.source.accept(this);
        this.builder.append("?)");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAsExpression expression) {
        this.builder.append("(");
        expression.source.accept(this);
        this.builder.append(" as ");
        expression.getType().accept(this);
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAssignmentExpression expression) {
        expression.left.accept(this);
        this.builder.append(" = ");
        expression.right.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBlockExpression expression) {
        this.builder.append("{").increase();
        for (DBSPStatement stat: expression.contents) {
            stat.accept(this);
            this.builder.append("\n");
        }
        if (expression.lastExpression != null) {
            expression.lastExpression.accept(this);
            this.builder.append("\n");
        }
        this.builder.decrease().append("}");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBorrowExpression expression) {
        this.builder.append("&");
        if (expression.mut)
            this.builder.append("mut ");
        expression.expression.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPParameter parameter) {
        this.builder.append(parameter.name);
        if (!this.compact) {
            this.builder.append(": ");
            parameter.type.accept(this);
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        this.builder.append("move |");
        for (DBSPParameter param: expression.parameters) {
            param.accept(this);
            this.builder.append(", ");
        }
        this.builder.append("| ");
        if (!this.compact) {
            DBSPType resultType = expression.getResultType();
            if (!resultType.is(DBSPTypeVoid.class)) {
                this.builder.append("-> ").newline();
                resultType.accept(this);
                this.builder.append(" ");
            }
        }
        if (expression.body.is(DBSPBlockExpression.class)) {
            expression.body.accept(this);
        } else {
            this.builder.append("{")
                    .increase();
            expression.body.accept(this);
            this.builder.decrease()
                    .append("\n}");
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDerefExpression expression) {
        this.builder.append("(*");
        expression.expression.accept(this);
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnwrapExpression expression) {
        this.builder.append("(");
        expression.expression.accept(this);
        this.builder.append(".unwrap()");
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    public VisitDecision preorder(DBSPEnumValue expression) {
        this.builder.append(expression.enumName)
                .append("::")
                .append(expression.constructor);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldExpression expression) {
        expression.expression.accept(this);
        DBSPType baseType = expression.expression.getType();
        if (baseType.mayBeNull) {
            // TODO: this should be done differently
            if (!baseType.hasCopy() &&
                    !expression.expression.is(DBSPCloneExpression.class))
                this.builder.append(".clone()");
            this.builder.append(".unwrap()");
        }
        this.builder.append(".")
                .append(expression.fieldNo);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIfExpression expression) {
        builder.append("(if ");
        expression.condition.accept(this);
        this.builder.append(" ");
        if (!expression.positive.is(DBSPBlockExpression.class))
            this.builder.append("{")
                    .increase();
        expression.positive.accept(this);
        if (!expression.positive.is(DBSPBlockExpression.class))
            this.builder.decrease()
                    .append("\n}");
        this.builder.append(" else ");
        if (!expression.negative.is(DBSPBlockExpression.class))
            this.builder.append("{")
                    .increase();
        expression.negative.accept(this);
        if (!expression.negative.is(DBSPBlockExpression.class))
            this.builder.decrease()
                    .append("\n}");
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPPathExpression expression) {
        expression.path.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPForExpression node) {
        this.builder.append("for ")
                .append(node.variable)
                .append(" in ");
        node.iterated.accept(this);
        this.builder.append(" ");
        node.block.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPQualifyTypeExpression expression) {
        expression.expression.accept(this);
        this.builder.append("::<");
        for (DBSPType type: expression.types) {
            type.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(">");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPRawTupleExpression expression) {
        this.builder.append("(");
        boolean newlines = this.compact && expression.fields.length > 2;
        if (newlines)
            this.builder.increase();
        for (DBSPExpression field: expression.fields) {
            field.accept(this);
            this.builder.append(", ");
            if (newlines)
                this.builder.newline();
        }
        if (newlines)
            this.builder.decrease();
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTupleExpression expression) {
        if (expression.isNull)
            return this.doNullExpression(expression);
        boolean newlines = this.compact && expression.fields.length > 2;
        this.builder.append(DBSPTypeCode.TUPLE.rustName)
                .append(expression.size())
                .append("::new(");
        if (newlines)
            this.builder.increase();
        boolean first = true;
        for (DBSPExpression field : expression.fields) {
            if (!first) {
                this.builder.append(", ");
                if (newlines)
                    this.builder.newline();
            }
            first = false;
            field.accept(this);
        }
        if (newlines)
            this.builder.decrease();
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConstructorExpression expression) {
        expression.function.accept(this);
        this.builder.append("(");
        if (expression.arguments.length > 0) {
            boolean first = true;
            for (DBSPExpression arg: expression.arguments) {
                if (!first)
                    this.builder.append(", ");
                first = false;
                arg.accept(this);
            }
        }
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPPath path) {
        boolean first = true;
        for (DBSPPathSegment segment: path.components) {
            if (!first)
                this.builder.append("::");
            first = false;
            segment.accept(this);
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSimplePathSegment segment) {
        builder.append(segment.identifier);
        if (segment.genericArgs.length > 0) {
            builder.append("::<");
            boolean first = true;
            for (DBSPType arg : segment.genericArgs) {
                if (!first)
                    this.builder.append(", ");
                first = false;
                arg.accept(this);
            }
            this.builder.append(">");
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIdentifierPattern pattern) {
        this.builder.append(pattern.mutable ? "mut " : "")
                .append(pattern.identifier);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeAny type) {
        this.builder.append("_");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeFunction type) {
        this.builder.append("_");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeBaseType type) {
        type.wrapOption(this.builder, type.getRustString());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeRawTuple type) {
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append("(");
        for (DBSPType fType: type.tupFields) {
            fType.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(")");
        if (type.mayBeNull)
            this.builder.append(">");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeRef type) {
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append("&")
                .append(type.mutable ? "mut " : "");
        type.type.accept(this);
        if (type.mayBeNull)
            this.builder.append(">");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStream type) {
        this.builder.append("Stream<")
                .append("_, "); // Circuit type
        type.elementType.accept(this);
        this.builder.append(">");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStruct.Field field) {
        this.builder.append(field.getSanitizedName())
                .append(": ");
        field.type.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStructItem item) {
        this.builder.append("#[derive(Clone, Debug, Eq, PartialEq, Default, serde::Serialize, serde::Deserialize)]")
                .newline();
        builder.append("struct ")
                    .append(Objects.requireNonNull(item.type.sanitizedName))
                .append(" {")
                .increase();
        for (DBSPTypeStruct.Field field: item.type.fields.values()) {
            builder.append("#[serde(rename = \"")
                    .append(Utilities.escape(
                            this.options.canonicalName(field.name)))
                    .append("\")]\n");
            field.accept(this);
            this.builder.append(",")
                    .newline();
        }
        this.builder.decrease()
                .append("}")
                .newline();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStruct type) {
        // A *reference* to a struct type is just the type name.
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append(type.sanitizedName);
        if (type.mayBeNull)
            this.builder.append(">");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeTuple type) {
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append(type.getName());
        if (type.size() > 0) {
            this.builder.append("<");
            boolean first = true;
            for (DBSPType fType : type.tupFields) {
                if (!first)
                    this.builder.append(", ");
                first = false;
                fType.accept(this);
            }
            this.builder.append(">");
        }
        if (type.mayBeNull)
            this.builder.append(">");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeUser type) {
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append(type.name);
        if (type.typeArgs.length > 0) {
            this.builder.append("<");
            boolean first = true;
            for (DBSPType fType: type.typeArgs) {
                if (!first)
                    this.builder.append(", ");
                first = false;
                fType.accept(this);
            }
            this.builder.append(">");
        }
        if (type.mayBeNull)
            this.builder.append(">");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregate aggregate) {
        throw new InternalCompilerError("Should have been eliminated", aggregate.getNode());
    }

    @Override
    public VisitDecision preorder(DBSPAggregate.Implementation implementation) {
        throw new InternalCompilerError("Should have been eliminated", implementation.getNode());
    }

    public static String toRustString(IErrorReporter reporter, IDBSPInnerNode node,
                                      CompilerOptions options, boolean compact) {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        ToRustInnerVisitor visitor = new ToRustInnerVisitor(reporter, stream, options, compact);
        node.accept(visitor);
        return builder.toString();
    }
}
