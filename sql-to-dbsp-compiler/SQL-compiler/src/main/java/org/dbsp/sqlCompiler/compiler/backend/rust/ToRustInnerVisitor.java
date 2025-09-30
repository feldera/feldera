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
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.backend.rust.multi.CircuitWriter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EliminateStructs;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CreateRuntimeErrorWrappers;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalIncrementExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdField;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEnumValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPForExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPHandleErrorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLazyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPQualifyTypeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPQuestionExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPReturnExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSortExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPGeoPointConstructor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI128Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
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
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariantExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUuidLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVariantNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPPathSegment;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPComparatorItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBinary;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMonthsInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeOption;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeStream;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWeight;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** This visitor generates a Rust implementation of the program. */
public class ToRustInnerVisitor extends InnerVisitor {
    protected final IIndentStream builder;
    /** If set use a more compact display, which is not necessarily compilable. */
    protected final boolean compact;
    protected final CompilerOptions options;
    /** Set by binary expressions */
    int visitingChild;
    @Nullable
    final SourcePositionResource sourcePositionResource;
    final CreateRuntimeErrorWrappers createErrorWrappers;

    public ToRustInnerVisitor(DBSPCompiler compiler, IIndentStream builder,
                              @Nullable SourcePositionResource sourcePositionResource, boolean compact) {
        super(compiler);
        this.builder = builder;
        this.compact = compact;
        this.options = compiler.options;
        this.sourcePositionResource = sourcePositionResource;
        this.visitingChild = 0;
        this.createErrorWrappers = new CreateRuntimeErrorWrappers(compiler);
    }

    @SuppressWarnings("SameReturnValue")
    VisitDecision doNullExpression(DBSPExpression expression) {
        this.push(expression);
        this.builder.append("None::<");
        expression.getType().withMayBeNull(false).accept(this);
        this.builder.append(">");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    public VisitDecision doNull(DBSPLiteral literal) {
        if (!literal.isNull())
            throw new UnsupportedException(literal.getNode());
        return this.doNullExpression(literal);
    }

    @Override
    public VisitDecision preorder(DBSPNullLiteral literal) {
        this.builder.append("None::<()>");
        return VisitDecision.STOP;
    }

    /**
     * Helper function for {@link ToRustInnerVisitor#generateComparator} and
     * {@link ToRustInnerVisitor#generateCmpFunc}.
     * @param nullable True if the field is nullable (or the tuple itself).
     * @param fieldNo  Field index that is compared.
     * @param ascending Comparison direction.
     * @param nullsFirst How nulls are compared
     */
    void emitCompareField(boolean nullable, int fieldNo, boolean ascending, boolean nullsFirst) {
        String name = "compare" + (nullable ? "N" : "_");
        this.builder.append("let ord = ")
                .append(name)
                .append("(&left.")
                .append(fieldNo)
                .append(", ")
                .append("&right.")
                .append(fieldNo)
                .append(", ")
                .append(ascending)
                .append(", ")
                .append(nullsFirst)
                .append(");")
                .newline();
        this.builder.append("if ord != Ordering::Equal { return ord");
        this.builder.append(" };")
                .newline();
    }

    /** Helper function for {@link ToRustInnerVisitor#generateComparator} and
     * {@link ToRustInnerVisitor#generateCmpFunc}.
     * @param ascending Comparison direction. */
    void emitCompare(boolean ascending) {
        this.builder.append("let ord = left.cmp(&right);")
                .newline()
                .append("if ord != Ordering::Equal { return ord");
        if (!ascending)
            this.builder.append(".reverse()");
        this.builder.append(" };")
                .newline();
    }

    /**
     * Helper function for {@link ToRustInnerVisitor#generateCmpFunc}.
     * This could be part of an inner visitor too.
     * But we don't handle {@link DBSPComparatorExpression}s in the same way in
     * any context: we do it differently in TopK and Sort.
     * This is for TopK.
     * @param comparator  Comparator expression to generate Rust for.
     * @param fieldsCompared  Accumulate here a list of all fields compared.
     */
    void generateComparator(DBSPComparatorExpression comparator, Set<Integer> fieldsCompared) {
        // This could be done with a visitor... but there are only two cases
        if (comparator.is(DBSPNoComparatorExpression.class))
            return;
        if (comparator.is(DBSPFieldComparatorExpression.class)) {
            DBSPFieldComparatorExpression fieldComparator = comparator.to(DBSPFieldComparatorExpression.class);
            boolean nullable = comparator.comparedValueType()
                    .to(DBSPTypeTupleBase.class)
                    .getFieldType(fieldComparator.fieldNo)
                    .mayBeNull;
            this.generateComparator(fieldComparator.source, fieldsCompared);
            if (fieldsCompared.contains(fieldComparator.fieldNo))
                throw new InternalCompilerError("Field " + fieldComparator.fieldNo + " used twice in sorting");
            fieldsCompared.add(fieldComparator.fieldNo);
            this.emitCompareField(nullable, fieldComparator.fieldNo, fieldComparator.ascending, fieldComparator.nullsFirst);
        } else {
            DBSPDirectComparatorExpression direct = comparator.to(DBSPDirectComparatorExpression.class);
            this.generateComparator(direct.source, fieldsCompared);
            this.emitCompare(direct.ascending);
        }
    }

    /** Generate a comparator */
    void generateCmpFunc(DBSPComparatorExpression comparator) {
        // impl CmpFunc<(String, i32, i32)> for CmpXX {
        //     fn cmp(left: &(String, i32, i32), right: &(String, i32, i32)) -> std::cmp::Ordering {
        //         let ord = compare(&left.0, &right.0, true, true); // last value is nullsLast
        //         if ord != Ordering::Equal { return ord; }
        //         let ord = compare(&right.1, &left.1, true, true);
        //         if ord != Ordering::Equal { return ord; }
        //         let ord = compare(&left.2, &right.2, true, false);
        //         if ord != Ordering::Equal { return ord.reverse(); }
        //         return Ordering::Equal;
        //     }
        // }
        String structName = comparator.getComparatorStructName();
        this.builder.append("pub struct ")
                .append(structName)
                .append(";")
                .newline();

        DBSPType type = comparator.comparedValueType();
        this.builder.append("impl CmpFunc<");
        type.accept(this);
        this.builder.append("> for ")
                .append(structName)
                .append(" {")
                .increase()
                .append("fn cmp(left: &");
        type.accept(this);
        this.builder.append(", right: &");
        type.accept(this);
        this.builder.append(") -> std::cmp::Ordering {")
                .increase();
        // This is a subtle aspect. The comparator compares on some fields,
        // but we have to generate a comparator on ALL the fields, to avoid
        // declaring values as equal when they aren't really.
        Set<Integer> fieldsCompared = new HashSet<>();
        this.generateComparator(comparator, fieldsCompared);
        // Now compare on the fields that we didn't compare on.
        // The order doesn't really matter.
        if (type.is(DBSPTypeTuple.class)) {
            for (int i = 0; i < type.to(DBSPTypeTuple.class).size(); i++) {
                if (fieldsCompared.contains(i)) continue;
                this.emitCompareField(false, i, true, true);
            }
        }
        this.builder.append("return Ordering::Equal;")
                .newline();
        this.builder
                .decrease()
                .append("}")
                .newline()
                .decrease()
                .append("}")
                .newline();
    }

    @Override
    public VisitDecision preorder(DBSPComparatorItem item) {
        this.generateCmpFunc(item.expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPNoComparatorExpression expression) {
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSortExpression expression) {
        /*
        move |(k, array): (&(), &Vec<Tup<...>>, ), | -> Array<Tup<...>> {
            let comp = ...;    // comparator
            let mut ec: _ = move |a: &Tup<...>, b: &Tup<...>, | -> _ {
                Comparator::cmp(a, b)
            };
            let mut v = (**array).clone();
            v.sort_by(ec);
            v.into()
        }
         */
        this.push(expression);
        this.builder.append("move |(k, array): (&Tup0<>, &Array<");
        expression.elementType.accept(this);
        this.builder.append(">)| -> Array<");
        expression.elementType.accept(this);
        this.builder.append("> {").increase();
        this.builder.append("let comp = move |a: &");
        expression.elementType.accept(this);
        this.builder.append(", b: &");
        expression.elementType.accept(this);
        this.builder.append("| { ");
        expression.comparator.accept(this);
        this.builder.append("::cmp(a, b) };").newline();
        this.builder.append("let mut v = (**array).clone();").newline()
                // we don't use sort_unstable_by because it is
                // non-deterministic
                .append("v.sort_by(comp);").newline();
        if (expression.limit != null) {
            this.builder.append("let mut v = (**array).clone();").newline();
            this.builder.append("v.truncate(");
            expression.limit.accept(this);
            this.builder.append(");").newline();
        }
        this.builder.append("v.into()");
        this.builder.newline()
                .decrease()
                .append("}");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSomeExpression expression) {
        this.builder.append("Some(");
        this.push(expression);
        expression.expression.accept(this);
        this.pop(expression);
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    void codegen(DBSPUnsignedWrapExpression.TypeSequence sequence) {
        this.builder.append("<");
        // In the type parameter we do not put the Option<>
        sequence.dataType.withMayBeNull(false).accept(this);
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
        this.push(expression);
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
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPLazyExpression expression) {
        this.push(expression);
        this.builder.append(DBSPLazyExpression.RUST_IMPLEMENTATION)
                .append("::new(|| { ");
        expression.expression.accept(this);
        this.builder.append(" })");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnsignedUnwrapExpression expression) {
        this.push(expression);
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
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldComparatorExpression expression) {
        this.builder.append(expression.getComparatorStructName());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTimestampLiteral literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
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
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUuidLiteral literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("Uuid::from_bytes([");
        byte[] data = literal.getByteArray();
        Utilities.enforce(data != null);
        Utilities.enforce(literal.value != null);
        boolean first = true;
        for (byte b : data) {
            if (!first)
                this.builder.append(",");
            first = false;
            this.builder.append(Byte.toUnsignedInt(b));
        }
        this.builder.append("]/*")
                .append(Objects.requireNonNull(literal.value.toString()))
                .append("*/")
                .append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDateLiteral literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
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
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTimeLiteral literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
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
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntervalMonthsLiteral literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("LongInterval::new(")
                .append(Integer.toString(Objects.requireNonNull(literal.value)))
                .append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntervalMillisLiteral literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("ShortInterval::new(")
                .append(Long.toString(Objects.requireNonNull(literal.value)))
                .append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPGeoPointConstructor constructor) {
        this.push(constructor);
        if (constructor.isNull()) {
            this.builder.append("None::<");
            constructor.getType().withMayBeNull(false).accept(this);
            this.builder.append(">");
            this.pop(constructor);
            return VisitDecision.STOP;
        }
        if (constructor.getType().mayBeNull)
            this.builder.append("Some(");
        this.builder.append("GeoPoint::new(");
        Objects.requireNonNull(constructor.left).accept(this);
        this.builder.append(", ");
        Objects.requireNonNull(constructor.right).accept(this);
        this.builder.append(")");
        if (constructor.getType().mayBeNull)
            this.builder.append(")");
        this.pop(constructor);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBoolLiteral literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.builder.append(literal.wrapSome(Boolean.toString(Objects.requireNonNull(literal.value))));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPArrayExpression expression) {
        if (expression.data == null)
            return this.doNullExpression(expression);
        this.push(expression);
        if (expression.getType().mayBeNull)
            this.builder.append("Some(");
        this.builder.append("Arc::new(vec!(");
        if (expression.data.size() > 1)
            this.builder.increase();
        for (DBSPExpression exp: expression.data) {
            exp.accept(this);
            this.builder.append(", ");
        }
        if (expression.data.size() > 1)
            this.builder.decrease();
        this.builder.append("))");
        if (expression.getType().mayBeNull)
            this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPMapExpression expression) {
        if (expression.isNull())
            return this.doNullExpression(expression);
        this.push(expression);
        if (expression.getType().mayBeNull)
            this.builder.append("Some(");
        this.builder.append("Arc::new(BTreeMap::from([");
        Utilities.enforce(expression.values != null);
        if (expression.values.size() > 1)
            this.builder.increase();
        for (int i = 0; i < Objects.requireNonNull(expression.keys).size(); i++) {
            this.builder.append("(");
            expression.keys.get(i).accept(this);
            this.builder.append(", ");
            expression.values.get(i).accept(this);
            this.builder.append("), ");
        }
        if (expression.values.size() > 1)
            this.builder.decrease();
        this.builder.append("]))");
        if (expression.getType().mayBeNull)
            this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBinaryLiteral literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
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
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPZSetExpression expression) {
        this.push(expression);
        this.builder.append("zset!(");
        boolean large = expression.data.size() > 1;
        if (large)
            this.builder.increase();
        // Do this for a deterministic result
        List<Map.Entry<DBSPExpression, Long>> entries = Linq.list(expression.data.entrySet());
        entries.sort(Comparator.comparing(a -> a.getKey().toString()));
        for (Map.Entry<DBSPExpression, Long> e : entries) {
            e.getKey().accept(this);
            this.builder.append(" => ")
                    .append(DBSPTypeWeight.makeWeight(e.getValue()));
            if (large)
                this.builder.append(",\n");
        }
        if (large)
            this.builder.decrease();
        this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIndexedZSetExpression expression) {
        this.builder.append("indexed_zset!(")
                .append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPRealLiteral literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
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

        String out = literal.wrapSome("F32::new(" + f32 + ")");
        this.builder.append(out);
        this.builder.append("/*")
                .append(val)
                .append("*/");
        this.pop(literal);
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
                    .append(comment.comment);
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDoubleLiteral literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
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
        String out = literal.wrapSome("F64::new(" + f64 + ")");
        this.builder.append(out);
        this.builder.append("/*")
                .append(val)
                .append("*/");
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUSizeLiteral literal) {
        String val = Objects.requireNonNull(literal.value).toString();
        this.builder.append(literal.wrapSome(val + "usize"));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariantExpression expression) {
        if (expression.value == null)
            return this.doNullExpression(expression);
        this.push(expression);
        if (expression.getType().mayBeNull)
            this.builder.append("Some(");
        if (expression.isSqlNull) {
            this.builder.append("Variant::SqlNull");
        } else {
            this.builder.append("Variant::from(");
            expression.value.accept(this);
            this.builder.append(")");
        }
        if (expression.getType().mayBeNull)
            this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariantNullLiteral literal) {
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.push(literal);
        this.builder.append("Variant::VariantNull");
        if (literal.mayBeNull())
            this.builder.append(")");
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPISizeLiteral literal) {
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + "isize"));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPMinMax aggregator) {
        if (aggregator.postProcessing != null)
            this.builder.append("Postprocess::new(");
        this.builder.append(aggregator.toString());
        if (aggregator.postProcessing != null) {
            this.builder.append(", ");
            aggregator.postProcessing.accept(this);
            this.builder.append(")");
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI8Literal literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
        String val = Byte.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        this.pop(literal);
        return VisitDecision.STOP;
    }


    @Override
    public VisitDecision preorder(DBSPI16Literal literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
        String val = Short.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI32Literal literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
        String val = Integer.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU8Literal literal) {
        if (literal.isNull())
            return this.doNull(literal);
        String val = Integer.toString(Objects.requireNonNull(literal.value));
        this.push(literal);
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU16Literal literal) {
        if (literal.isNull())
            return this.doNull(literal);
        String val = Integer.toString(Objects.requireNonNull(literal.value));
        this.push(literal);
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU32Literal literal) {
        if (literal.isNull())
            return this.doNull(literal);
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.push(literal);
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI64Literal literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        this.pop(literal);
        return VisitDecision.STOP;
    }

    void generateStructDeclaration(DBSPTypeStruct struct) {
        this.builder.append("#[derive(Clone, Debug, Eq, PartialEq, Default, PartialOrd, Ord)]")
                .newline();
        builder.append("pub struct ")
                .append(Objects.requireNonNull(struct.sanitizedName))
                .append(" {")
                .increase();
        for (DBSPTypeStruct.Field field: struct.fields.values()) {
            field.accept(this);
            this.builder.append(",")
                    .newline();
        }
        this.builder.decrease()
                .append("}")
                .newline();
    }

    void generateStructHelpers(DBSPTypeStruct s, @Nullable TableMetadata metadata) {
        s = s.withMayBeNull(false).to(DBSPTypeStruct.class);
        this.generateStructDeclaration(s);
        this.generateFromTrait(s);
        this.generateRenameMacro(s, metadata);
    }

    @Override
    public VisitDecision preorder(DBSPStructItem item) {
        this.generateStructHelpers(item.type, item.metadata);
        return VisitDecision.STOP;
    }

    void generateInto(String field, DBSPType sourceType, DBSPType targetType) {
        if (sourceType.is(DBSPTypeOption.class)) {
            DBSPType fieldType;
            if (targetType.is(DBSPTypeOption.class)) {
                fieldType = targetType.to(DBSPTypeOption.class).typeArgs[0];
            } else {
                Utilities.enforce(targetType.mayBeNull);
                fieldType = targetType.withMayBeNull(false);
            }
            this.builder.append(field);
            DBSPTypeOption option = sourceType.to(DBSPTypeOption.class);
            this.builder.append(".map(|x| ");
            this.generateInto("x", option.typeArgs[0], fieldType);
            this.builder.append(")");
        } else if (sourceType.mayBeNull && !sourceType.is(DBSPTypeNull.class)) {
            this.builder.append(field);
            this.builder.append(".map(|x|").increase();
            this.generateInto("x", sourceType.withMayBeNull(false), targetType.withMayBeNull(false));
            this.builder.decrease().append(")");
        } else if (sourceType.is(DBSPTypeArray.class)) {
            this.builder.append("Arc::new(Arc::unwrap_or_clone(");
            this.builder.append(field);
            DBSPTypeArray vec = sourceType.to(DBSPTypeArray.class);
            DBSPTypeArray targetArray = targetType.to(DBSPTypeArray.class);
            this.builder.append(").into_iter().map(|y|").increase();
            this.generateInto("y", vec.getElementType(), targetArray.getElementType());
            this.builder.decrease().append(")")
                    .newline()
                    .append(".collect::<");
            targetArray.innerType().accept(this);
            this.builder.append(">())");
        } else if (sourceType.is(DBSPTypeMap.class)) {
            this.builder.append("Arc::new(Arc::unwrap_or_clone(");
            this.builder.append(field);
            DBSPTypeMap map = sourceType.to(DBSPTypeMap.class);
            DBSPTypeMap tMap = targetType.to(DBSPTypeMap.class);
            this.builder.append(").into_iter().map(|(k,v)|").increase()
                    .append("(");
            this.generateInto("k", map.getKeyType(), tMap.getKeyType());
            this.builder.append(", ");
            this.generateInto("v", map.getValueType(), tMap.getValueType());
            this.builder.decrease().append("))")
                    .newline()
                    .append(".collect::<");
            tMap.innerType().accept(this);
            this.builder.append(">())");
        } else {
            this.builder.append(field);
            this.builder.append(".into()");
        }
    }

    protected void generateFromTrait(DBSPTypeStruct type) {
        EliminateStructs es = new EliminateStructs(this.compiler());
        DBSPTypeTuple tuple = es.apply(type).to(DBSPTypeTuple.class);
        this.builder.append("impl From<")
                .append(type.sanitizedName)
                .append("> for ");
        tuple.accept(this);
        this.builder.append(" {")
                .increase()
                .append("fn from(t: ")
                .append(type.sanitizedName)
                .append(") -> Self");
        this.builder.append(" {")
                .increase()
                .append(tuple.getName())
                .append("::new(");
        int index = 0;
        for (DBSPTypeStruct.Field field: type.fields.values()) {
            this.generateInto("t." + field.getSanitizedName(), field.type, tuple.tupFields[index]);
            this.builder.append(", ");
            index++;
        }
        this.builder.append(")").newline();
        this.builder.decrease()
                .append("}")
                .newline()
                .decrease()
                .append("}")
                .newline();

        this.builder.append("impl From<");
        tuple.accept(this);
        this.builder.append("> for ")
                .append(type.sanitizedName);
        this.builder.append(" {")
                .increase()
                .append("fn from(t: ");
        tuple.accept(this);
        this.builder.append(") -> Self");
        this.builder.append(" {")
                .increase()
                .append("Self {")
                .increase();
        index = 0;
        for (DBSPTypeStruct.Field field: type.fields.values()) {
            this.builder
                    .append(field.getSanitizedName())
                    .append(": ");
            this.generateInto("t." + index, field.type, field.type);
            this.builder.append(", ")
                    .newline();
            index++;
        }
        this.builder.decrease().append("}").newline();
        this.builder.decrease()
                .append("}")
                .newline()
                .decrease()
                .append("}")
                .newline();
    }

    /**
     * Generate calls to the Rust macros that generate serialization and deserialization code
     * for the struct.
     *
     * @param type      Type of record in the table.
     * @param metadata  Metadata for the input columns (null for an output view). */
    protected void generateRenameMacro(DBSPTypeStruct type,
                                       @Nullable TableMetadata metadata) {
        this.builder.append("deserialize_table_record!(");
        this.builder.append(type.sanitizedName)
                .append("[")
                .append(Utilities.doubleQuote(type.name.name()))
                .append(", ")
                .append(type.fields.size())
                .append("] {")
                .increase();
        boolean first = true;
        for (DBSPTypeStruct.Field field: type.fields.values()) {
            DBSPTypeUser user = field.type.as(DBSPTypeUser.class);
            boolean isOption = user != null && user.name.equals("Option");
            if (!first)
                this.builder.append(",").newline();
            first = false;
            ProgramIdentifier name = field.name;
            String simpleName = name.name();
            InputColumnMetadata meta = null;
            if (metadata == null) {
                // output
                simpleName = this.options.canonicalName(name);
            } else {
                meta = metadata.getColumnMetadata(field.name);
            }
            this.builder.append("(")
                    .append(field.getSanitizedName())
                    .append(", ")
                    .append(Utilities.doubleQuote(simpleName))
                    .append(", ")
                    .append(Boolean.toString(name.isQuoted()))
                    .append(", ");
            field.type.accept(this);
            this.builder.append(", ");
            if (isOption)
                this.builder.append("Some(");
            if (meta == null || meta.defaultValue == null) {
                this.builder.append(field.type.mayBeNull ? "Some(None)" : "None");
            } else {
                this.builder.append("Some(");
                IDBSPInnerNode defaultValue = this.createErrorWrappers.apply(meta.defaultValue);
                defaultValue.accept(this);
                this.builder.append(")");
            }
            if (isOption)
                this.builder.append(")");

            if (isOption && user.typeArgs[0].mayBeNull) {
                // Option<Option<...>>
                this.builder.append(", |x| if x.is_none() { Some(None) } else {x}");
            }
            this.builder.append(")");
        }
        this.builder.newline()
                .decrease()
                .append("});")
                .newline();

        this.builder.append("serialize_table_record!(");
        this.builder.append(type.sanitizedName)
                .append("[")
                .append(type.fields.size())
                .append("]{")
                .increase();
        first = true;
        for (DBSPTypeStruct.Field field: type.fields.values()) {
            if (!first)
                this.builder.append(",").newline();
            first = false;
            ProgramIdentifier name = field.name;
            String simpleName = name.name();
            if (metadata == null) {
                simpleName = this.options.canonicalName(name);
            }
            this.builder
                    .append(field.getSanitizedName())
                    .append("[")
                    .append(Utilities.doubleQuote(simpleName))
                    .append("]")
                    .append(": ");
            field.type.accept(this);
        }
        this.builder.newline()
                .decrease()
                .append("});")
                .newline();
    }

    @Override
    public VisitDecision preorder(DBSPI128Literal literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
        String val = Objects.requireNonNull(literal.value).toString();
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU64Literal literal) {
        if (literal.isNull())
            return this.doNull(literal);
        String val = Objects.requireNonNull(literal.value).toString();
        this.push(literal);
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU128Literal literal) {
        String val = Objects.requireNonNull(literal.value).toString();
        this.push(literal);
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStringLiteral literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
        Objects.requireNonNull(literal.value);
        byte[] bytes = literal.value.getBytes(literal.charset);
        String decoded = new String(bytes, literal.charset);
        decoded = Utilities.doubleQuote(decoded);
        this.builder.append(literal.wrapSome(
        DBSPTypeCode.STRING.rustName + "::from(arcstr::literal!(" + decoded + "))"));
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStrLiteral literal) {
        if (literal.isNull()) {
            this.builder.append("None");
            return VisitDecision.STOP;
        }
        this.push(literal);
        Objects.requireNonNull(literal.value);
        if (literal.raw) {
            String contents = "r#\"" + literal.value + "\"#";
            this.builder.append(literal.wrapSome(contents));
        } else {
            this.builder.append(literal.wrapSome(Utilities.doubleQuote(literal.value)));
        }
        this.pop(literal);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeDecimal type) {
        this.push(type);
        this.optionPrefix(type);
        this.builder.append("SqlDecimal<")
                .append(type.precision)
                .append(", ")
                .append(type.scale)
                .append(">");
        this.optionSuffix(type);
        this.pop(type);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDecimalLiteral literal) {
        if (literal.isNull())
            return this.doNull(literal);
        this.push(literal);
        if (literal.getType().mayBeNull)
            this.builder.append("Some(");
        BigDecimal value = Objects.requireNonNull(literal.value).stripTrailingZeros();
        DBSPTypeDecimal type = literal.getType().to(DBSPTypeDecimal.class);
        this.builder.append("SqlDecimal::<")
                .append(type.precision)
                .append(", ")
                .append(type.scale)
                .append(">::new(")
                .append(value.unscaledValue().toString())
                .append("i128, ")
                .append(value.scale())
                .append(").expect(\"Could not convert ")
                .append(value.toString())
                .append(" to DECIMAL(")
                .append(type.precision)
                .append(", ")
                .append(type.scale)
                .append(")\")");
        if (literal.getType().mayBeNull)
            this.builder.append(")");
        this.pop(literal);
        return VisitDecision.STOP;
    }

    void unimplementedCast(DBSPCastExpression expression) {
        if (this.compact)
            return;
        throw new UnimplementedException(
                "Cast from " + expression.source.getType() + " to " + expression.getType() +
                " not implemented", expression.getNode());
    }

    @Override
    public VisitDecision preorder(DBSPCastExpression expression) {
        /* Default implementation of cast of a source expression to the 'this' type.
         * For example, to cast source which is an Option<i16> to a bool
         * the function called will be cast_to_b_i16N. */
        this.push(expression);
        DBSPType destType = expression.getType();
        DBSPTypeCode code = destType.code;
        DBSPType sourceType = expression.source.getType();
        DBSPTypeArray sourceVecType = sourceType.as(DBSPTypeArray.class);
        DBSPTypeArray destVecType = destType.as(DBSPTypeArray.class);
        String functionName;

        if (destVecType != null) {
            if (sourceType.is(DBSPTypeVariant.class)) {
                // Cast variant to vec
                functionName = "cast_to_vec" + destType.nullableSuffix() + "_" + sourceType.baseTypeWithSuffix();
                this.builder.append(functionName).append("::<");
                destVecType.getElementType().accept(this);
                this.builder.append(">(").increase();
                expression.source.accept(this);
                this.builder.decrease().append(")");
                this.pop(expression);
                return VisitDecision.STOP;
            }
        }

        if (sourceVecType != null) {
            if (code == DBSPTypeCode.VARIANT) {
                // cast vec to variant
                functionName = "cast_to_" + destType.baseTypeWithSuffix() + "_vec" + sourceType.nullableSuffix();
                this.builder.append(functionName)
                        .append("::<");
                sourceVecType.getElementType().accept(this);
                this.builder.append(">")
                        .append("(")
                        .increase();
                expression.source.accept(this);
                this.builder.decrease().append(")");
                this.pop(expression);
                return VisitDecision.STOP;
            }

            // should have been eliminated
            this.unimplementedCast(expression);
            this.pop(expression);
            return VisitDecision.STOP;
        }

        DBSPTypeMap sourceMap = sourceType.as(DBSPTypeMap.class);
        DBSPTypeMap destMap = destType.as(DBSPTypeMap.class);
        if (destMap != null) {
            if (sourceType.is(DBSPTypeVariant.class)) {
                // Cast variant to map
                functionName = "cast_to_map" + destType.nullableSuffix() + "_" + sourceType.baseTypeWithSuffix();
                this.builder.append(functionName).append("(").increase();
                expression.source.accept(this);
                this.builder.decrease().append(")");
                this.pop(expression);
                return VisitDecision.STOP;
            } else {
                this.unimplementedCast(expression);
                this.pop(expression);
            }
        }
        if (sourceMap != null) {
            if (code == DBSPTypeCode.VARIANT) {
                functionName = "cast_to_" + destType.baseTypeWithSuffix() + "_map" + sourceType.nullableSuffix();
                this.builder.append(functionName).append("(").increase();
                expression.source.accept(this);
                this.builder.decrease().append(")");
                this.pop(expression);
                return VisitDecision.STOP;
            } else {
                this.unimplementedCast(expression);
                this.pop(expression);
            }
        }

        if (sourceType.is(DBSPTypeTuple.class)) {
            // should have been eliminated
            if (!this.compact)
                this.unimplementedCast(expression);
            // we are dumping DOT
            this.builder.append("(");
            expression.type.accept(this);
            this.builder.append(")");
            expression.source.accept(this);
            this.pop(expression);
            return VisitDecision.STOP;
        }
        if (code == DBSPTypeCode.TUPLE) {
            // should have been eliminated
            if (!this.compact)
                this.unimplementedCast(expression);
            // we are dumping DOT
            this.builder.append("(");
            expression.type.accept(this);
            this.builder.append(")");
            expression.source.accept(this);
            this.pop(expression);
            return VisitDecision.STOP;
        }
        functionName = "cast_to_" + destType.baseTypeWithSuffix() +
                "_" + sourceType.baseTypeWithSuffix();

        if ((sourceType.is(DBSPTypeMonthsInterval.class) && code == DBSPTypeCode.INTERVAL_LONG) ||
            (sourceType.is(DBSPTypeMillisInterval.class) && code == DBSPTypeCode.INTERVAL_SHORT)) {
            DBSPTypeBaseType t = destType.to(DBSPTypeBaseType.class);
            functionName = "cast_to_" + t.shortName() + destType.nullableSuffix() +
                    "_" + t.shortName() + sourceType.nullableSuffix();
        }
        int argCount = sourceType.genericArgumentCount() + destType.genericArgumentCount();
        this.builder.append(functionName);
        if (argCount > 0) {
            this.builder.append("::<");
            boolean first = destType.emitGenericArguments(this.builder, true);
            sourceType.emitGenericArguments(this.builder, first);
            this.builder.append(">");
        }
        this.builder.append("(");
        expression.source.accept(this);
        DBSPTypeString str = destType.as(DBSPTypeString.class);
        if (str != null) {
            // pass precision and fixedness as arguments to cast method too
            this.builder.append(",")
                    .append(str.precision)
                    .append(",")
                    .append(Boolean.toString(str.fixed));
        }
        DBSPTypeBinary binary = destType.as(DBSPTypeBinary.class);
        if (binary != null) {
            // pass precision as argument to cast method too
            this.builder.append(",")
                    .append(binary.precision);
        }
        this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPHandleErrorExpression expression) {
        this.push(expression);
        if (!this.compact) {
            this.builder.append(expression.getFunction());
            this.builder.append("(");
            if (expression.runtimeBehavior == DBSPHandleErrorExpression.RuntimeBehavior.PanicWithSource) {
                if (this.operatorContext == null) {
                    this.builder.append("\"")
                            .append(SourcePositionResource.NO_OPERATOR_HASH)
                            .append("\", ");
                } else {
                    this.builder.append("hash.unwrap(), ");
                }
                this.builder.append(expression.index)
                        .append(", &")
                        .append(CircuitWriter.SOURCE_MAP_VARIABLE_NAME)
                        .append(", ");
            }
        }
        expression.source.accept(this);
        if (!this.compact) {
            this.builder.append(")");
        }
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFold fold) {
        this.push(fold);
        this.builder.append("Fold::<_, _, ");
        fold.semigroup.accept(this);
        this.builder.append(", _, _>::with_output(").increase();
        fold.zero.accept(this);
        this.builder.append(",").newline();
        fold.increment.accept(this);
        this.builder.append(",").newline();
        fold.postProcess.accept(this);
        this.builder.decrease().newline().append(")");
        this.pop(fold);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConditionalIncrementExpression expression) {
        this.push(expression);
        String function = RustSqlRuntimeLibrary.INSTANCE.getFunctionName(
                expression.getNode(),
                expression.opcode, expression.getType(), expression.left.getType(), expression.right.getType());
        this.builder.append(function);
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
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBinaryExpression expression) {
        this.push(expression);
        switch (expression.opcode) {
            case MUL_WEIGHT: {
                this.visitingChild = 0;
                expression.left.accept(this);
                this.builder.append(".mul_by_ref(&");
                this.visitingChild = 1;
                expression.right.accept(this);
                this.builder.append(")");
                break;
            }
            case SQL_INDEX:
                throw new InternalCompilerError("Should have been eliminated");
            case RUST_INDEX:
            case SAFE_RUST_INDEX: {
                DBSPType collectionType = expression.left.getType();
                DBSPType indexType = expression.right.getType();
                DBSPTypeArray arrayType = collectionType.to(DBSPTypeArray.class);
                this.builder.append("index")
                        .append(expression.left.getType().nullableUnderlineSuffix())
                        .append(arrayType.getElementType().nullableUnderlineSuffix())
                        .append(indexType.nullableUnderlineSuffix())
                        .append("(");
                boolean leftDone = false;
                // Cases:
                // - t: Tuple<Array<>>: &t.0
                // - t: Tuple<Option<Array<>>>: &t.0
                // - t: Option<Tuple<Array<>>>: &match t { None => None, Some(x) => Some(x.0.clone()) }
                // - t: Option<Tuple<Option<Array>>>>: &match t { None => None, Some(x) => x.0.clone() }
                if (expression.left.is(DBSPFieldExpression.class) && collectionType.mayBeNull) {
                    // One of last 3 cases
                    DBSPFieldExpression field = expression.left.to(DBSPFieldExpression.class);
                    DBSPTypeTupleBase tupleType = field.expression
                            .getType()
                            .to(DBSPTypeTupleBase.class);
                    boolean tupleMaybeNull = tupleType.mayBeNull;
                    boolean arrayMayBeNull = tupleType
                            .getFieldType(field.fieldNo).mayBeNull;
                    if (tupleMaybeNull) {
                        // one of last 2 cases
                        leftDone = true;
                        this.builder.append("&match &");
                        field.expression.accept(this);
                        this.builder.append(" {").increase()
                                .append("None => None,").newline();
                        if (arrayMayBeNull) {
                            this.builder
                                    .append("Some(x) => x.")
                                    .append(field.fieldNo)
                                    .append(".clone()");
                        } else {
                            this.builder
                                    .append("Some(x) => Some(x.")
                                    .append(field.fieldNo)
                                    .append(".clone())");
                        }
                        this.builder.newline().append("}");
                    }
                }
                if (!leftDone) {
                    this.visitingChild = 0;
                    this.builder.append("&(");
                    expression.left.accept(this);
                    this.builder.append(")");
                }
                this.visitingChild = 1;
                this.builder.append(", ");
                expression.right.accept(this);
                this.builder.append(")");
                break;
            }
            case MAP_INDEX: {
                DBSPType collectionType = expression.left.getType();
                DBSPTypeMap map = collectionType.to(DBSPTypeMap.class);
                this.builder.append("map_index")
                        .append(collectionType.nullableUnderlineSuffix())
                        .append(map.getValueType().nullableUnderlineSuffix())
                        .append(expression.right.getType().nullableUnderlineSuffix())
                        .append("(");
                // See ARRAY_INDEX above.
                boolean leftDone = false;
                if (expression.left.is(DBSPFieldExpression.class) && collectionType.mayBeNull) {
                    DBSPFieldExpression field = expression.left.to(DBSPFieldExpression.class);
                    DBSPTypeTupleBase tupleType = field.expression
                            .getType()
                            .to(DBSPTypeTupleBase.class);
                    boolean tupleMaybeNull = tupleType.mayBeNull;
                    boolean arrayMayBeNull = tupleType
                            .getFieldType(field.fieldNo).mayBeNull;
                    if (tupleMaybeNull) {
                        leftDone = true;
                        this.builder.append("&match &");
                        field.expression.accept(this);
                        this.builder.append(" {").increase()
                                .append("None => None,").newline();
                        if (arrayMayBeNull) {
                            this.builder
                                    .append("Some(x) => x.")
                                    .append(field.fieldNo)
                                    .append(".clone()");
                        } else {
                            this.builder
                                    .append("Some(x) => Some(x.")
                                    .append(field.fieldNo)
                                    .append(".clone())");
                        }
                        this.builder.newline().append("}");
                    }
                }
                if (!leftDone) {
                    this.visitingChild = 0;
                    this.builder.append("&(");
                    expression.left.accept(this);
                    this.builder.append(")");
                }
                this.builder.append(", ");
                this.visitingChild = 1;
                expression.right.accept(this);
                this.builder.append(")");
                break;
            }
            case VARIANT_INDEX: {
                DBSPType indexType = expression.right.getType();
                this.builder.append("indexV")
                        .append(expression.left.getType().nullableUnderlineSuffix())
                        .append(indexType.nullableUnderlineSuffix())
                        .append("(");
                this.builder.append("&");
                this.visitingChild = 0;
                expression.left.accept(this);
                this.builder.append(", ");
                this.visitingChild = 1;
                expression.right.accept(this);
                this.builder.append(")");
                break;
            }
            case ARRAY_CONVERT, MAP_CONVERT, DIV_NULL: {
                this.builder.append(expression.opcode.toString())
                        .append(expression.left.getType().nullableUnderlineSuffix())
                        .append(expression.right.getType().nullableUnderlineSuffix())
                        .append("(");
                this.visitingChild = 0;
                expression.left.accept(this);
                this.builder.append(", ");
                this.visitingChild = 1;
                expression.right.accept(this);
                this.builder.append(")");
                break;
            }
            case OR:
            case AND: {
                String function = RustSqlRuntimeLibrary.INSTANCE.getFunctionName(
                        expression.getNode(),
                        expression.opcode,
                        expression.getType(),
                        expression.left.getType(),
                        expression.right.getType());
                this.builder.append(function).append("(");
                this.visitingChild = 0;
                expression.left.accept(this);
                // lazy in the right argument, make it a closure
                this.builder.append(", || (");
                this.visitingChild = 1;
                expression.right.accept(this);
                this.builder.append("))");
                break;
            }
            default: {
                if (expression.opcode == DBSPOpcode.ADD && expression.left.getType().is(DBSPTypeTuple.class)) {
                    this.visitingChild = 0;
                    expression.left.accept(this);
                    this.builder.append(".add_by_ref(&");
                    this.visitingChild = 1;
                    expression.right.accept(this);
                    this.builder.append(")");
                } else {
                    String function = RustSqlRuntimeLibrary.INSTANCE.getFunctionName(
                            expression.getNode(),
                            expression.opcode,
                            expression.getType(),
                            expression.left.getType(),
                            expression.right.getType());
                    this.builder.append(function);
                    int genericArgCount =
                            expression.left.getType().genericArgumentCount() +
                                    expression.right.getType().genericArgumentCount() +
                                    expression.getType().genericArgumentCount();
                    boolean needsGenericArguments =
                            expression.opcode == DBSPOpcode.ADD ||
                                    expression.opcode == DBSPOpcode.SUB ||
                                    expression.opcode == DBSPOpcode.MUL ||
                                    expression.opcode == DBSPOpcode.DIV ||
                                    expression.opcode == DBSPOpcode.MOD;
                    if (genericArgCount > 0 && needsGenericArguments) {
                        this.builder.append("::<");
                        boolean first = expression.left.getType().emitGenericArguments(builder, true);
                        first = expression.right.getType().emitGenericArguments(builder, first);
                        expression.getType().emitGenericArguments(builder, first);
                        this.builder.append(">");
                    }
                    this.builder.append("(");
                    this.visitingChild = 0;
                    expression.left.accept(this);
                    this.builder.append(", ");
                    this.visitingChild = 1;
                    expression.right.accept(this);
                    this.builder.append(")");
                }
                break;
            }
        }
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPLetExpression expression) {
        this.push(expression);
        this.builder.append("{");
        this.builder
                .append("let ")
                .append(expression.variable.variable)
                .append(" = ");
        expression.initializer.accept(this);
        this.builder.append(";").newline();
        expression.consumer.accept(this);
        this.builder.append("}");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIsNullExpression expression) {
        this.push(expression);
        if (!expression.expression.getType().mayBeNull) {
            this.builder.append("false");
        } else {
            expression.expression.accept(this);
            this.builder.append(".is_none()");
        }
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCloneExpression expression) {
        this.push(expression);
        expression.expression.accept(this);
        if (expression.expression.is(DBSPFieldExpression.class) &&
                expression.expression.getType().mayBeNull)
            this.builder.append(".cloned()");
        else
            this.builder.append(".clone()");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStaticExpression expression) {
        this.push(expression);
        this.builder.append(expression.getName());
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnaryExpression expression) {
        this.push(expression);
        if (expression.opcode == DBSPOpcode.WRAP_BOOL ||
            expression.opcode == DBSPOpcode.INDICATOR) {
            this.builder.append(expression.opcode.toString());
            if (expression.opcode == DBSPOpcode.INDICATOR) {
                this.builder.append("::<_, ");
                expression.getType().accept(this);
                this.builder.append(">");
            }
            this.builder.append("(");
            expression.source.accept(this);
            this.builder.append(")");
            this.pop(expression);
            return VisitDecision.STOP;
        } else if (expression.opcode.toString().startsWith("is_")) {
            this.builder.append(expression.opcode.toString())
                    .append("_")
                    .append(expression.source.getType().baseTypeWithSuffix())
                    .append("_(");
            expression.source.accept(this);
            this.builder.append(")");
            this.pop(expression);
            return VisitDecision.STOP;
        } else if (expression.opcode == DBSPOpcode.TYPEDBOX) {
            this.builder.append("TypedBox::new(");
            expression.source.accept(this);
            this.builder.append(")");
            this.pop(expression);
            return VisitDecision.STOP;
        } else if (expression.opcode == DBSPOpcode.DECIMAL_TO_INTEGER ||
                expression.opcode == DBSPOpcode.INTEGER_TO_DECIMAL) {
            this.builder.append(expression.opcode.toString())
                    .append(expression.source.getType().nullableUnderlineSuffix())
                    .append("::<");
            boolean first = expression.source.getType().emitGenericArguments(this.builder, true);
            expression.getType().emitGenericArguments(this.builder, first);
            this.builder.append(">(");
            expression.source.accept(this);
            this.builder.append(")");
            this.pop(expression);
            return VisitDecision.STOP;
        }
        if (expression.source.getType().mayBeNull) {
            this.builder.append("(")
                    .append("match ");
            expression.source.accept(this);
            this.builder.append(" {").increase()
                    .append("Some(x) => Some(")
                    .append(expression.opcode.toString())
                    .append("(x)),\n")
                    .append("_ => None,\n")
                    .decrease()
                    .append("}")
                    .append(")");
        } else {
            this.builder.append("(")
                    .append(expression.opcode.toString());
            expression.source.accept(this);
            this.builder.append(")");
        }
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath expression) {
        this.push(expression);
        this.builder.append(expression.variable);
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPExpressionStatement statement) {
        this.push(statement);
        statement.expression.accept(this);
        this.builder.append(";");
        this.pop(statement);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPLetStatement statement) {
        this.push(statement);
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
        this.pop(statement);
        return VisitDecision.STOP;
    }

    static class NeedsSourceMap extends InnerVisitor {
        public boolean found = false;

        public NeedsSourceMap(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public VisitDecision preorder(DBSPHandleErrorExpression expression) {
            if (expression.runtimeBehavior == DBSPHandleErrorExpression.RuntimeBehavior.PanicWithSource)
                this.found = true;
            return VisitDecision.STOP;
        }
    }

    @Override
    public VisitDecision preorder(DBSPFunction function) {
        if (function.body == null)
            return VisitDecision.STOP;
        this.push(function);
        this.builder.intercalateS("\n", function.annotations)
                .append("pub fn ")
                .append(function.name)
                .append("(");
        boolean first = true;
        for (DBSPParameter param : function.parameters) {
            if (!first)
                this.builder.append(", ");
            first = false;
            param.accept(this);
        }
        this.builder.append(") ");
        if (!function.returnType.is(DBSPTypeVoid.class)) {
            builder.append("-> ");
            function.returnType.accept(this);
        }
        builder.append(" ");
        if (function.body.is(DBSPBlockExpression.class)) {
            function.body.accept(this);
        } else {
            this.builder.append("{").increase();
            NeedsSourceMap finder = new NeedsSourceMap(this.compiler);
            finder.apply(function.body);
            if (finder.found) {
                SourcePositionResource.generateReference(this.builder, CircuitWriter.SOURCE_MAP_VARIABLE_NAME);
            }
            function.body.accept(this);
            this.builder.decrease()
                    .newline()
                    .append("}");
        }
        this.pop(function);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPApplyExpression expression) {
        this.push(expression);
        expression.function.accept(this);
        this.builder.append("(");
        if (expression.arguments.length > 1)
            this.builder.increase();
        boolean first = true;
        for (DBSPExpression arg: expression.arguments) {
            if (!first)
                this.builder.append(",").newline();
            first = false;
            arg.accept(this);
        }
        if (expression.arguments.length > 1)
            this.builder.decrease();
        this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPApplyMethodExpression expression) {
        this.push(expression);
        expression.self.accept(this);
        this.builder.append(".");
        expression.function.accept(this);
        this.builder.append("(");
        if (expression.arguments.length > 1)
            this.builder.increase();
        boolean first = true;
        for (DBSPExpression arg: expression.arguments) {
            if (!first)
                this.builder.append(",").newline();
            first = false;
            arg.accept(this);
        }
        if (expression.arguments.length > 1)
            this.builder.decrease();
        this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPQuestionExpression expression) {
        this.push(expression);
        this.builder.append("(");
        expression.source.accept(this);
        this.builder.append("?)");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    boolean compilingAssignmentLHS = false;

    @Override
    public VisitDecision preorder(DBSPAssignmentExpression expression) {
        this.push(expression);
        Utilities.enforce(!this.compilingAssignmentLHS);
        this.compilingAssignmentLHS = true;
        expression.left.accept(this);
        this.compilingAssignmentLHS = false;
        this.builder.append(" = ");
        expression.right.accept(this);
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBlockExpression expression) {
        this.push(expression);
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
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBorrowExpression expression) {
        this.push(expression);
        this.builder.append("&");
        if (expression.mut)
            this.builder.append("mut ");
        expression.expression.accept(this);
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPParameter parameter) {
        this.push(parameter);
        this.builder.append(parameter.name);
        if (!this.compact) {
            this.builder.append(": ");
            parameter.type.accept(this);
        }
        this.pop(parameter);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        this.push(expression);
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
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDerefExpression expression) {
        this.push(expression);
        this.builder.append("(*");
        expression.expression.accept(this);
        this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnwrapExpression expression) {
        this.push(expression);
        this.builder.append("(");
        expression.expression.accept(this);
        this.builder.append(".unwrap()");
        this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    public VisitDecision preorder(DBSPEnumValue expression) {
        this.push(expression);
        this.builder.append(expression.enumName)
                .append("::")
                .append(expression.constructor);
        this.pop(expression);
        return VisitDecision.STOP;
    }

    public VisitDecision preorder(DBSPCustomOrdExpression expression) {
        this.push(expression);
        this.builder.append("WithCustomOrd::<");
        expression.source.getType().accept(this);
        this.builder.append(", ");
        expression.comparator.accept(this);
        this.builder.append(">::new(");
        expression.source.accept(this);
        this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    public VisitDecision preorder(DBSPUnwrapCustomOrdExpression expression) {
        this.push(expression);
        this.builder.append("(");
        expression.expression.accept(this);
        this.builder.append(".get())");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCustomOrdField expression) {
        this.push(expression);
        DBSPType fieldType = expression.getFieldType();
        this.builder.append("match ");
        expression.expression.accept(this);
        this.builder.append(" {")
                .increase()
                .append("None => None,").newline()
                .append("Some(x) => ");
        if (expression.needsSome())
            this.builder.append("Some(");
        this.builder.append("(*x).get().")
                .append(expression.fieldNo);
        if (!fieldType.hasCopy())
            this.builder.append(".clone()");
        if (expression.needsSome())
            this.builder.append(")");
        this.builder.decrease().newline().append("}");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStaticItem item) {
        DBSPStaticExpression stat = item.expression;
        String name = stat.getName();
        this.builder.append("static ")
                .append(name)
                .append(": ")
                .append(DBSPStaticExpression.RUST_IMPLEMENTATION)
                .append("<");
        stat.getType().accept(this);
        this.builder.append("> = ")
                .append(DBSPStaticExpression.RUST_IMPLEMENTATION)
                .append("::new();").newline();
        this.builder.append(name)
                .append(".init(move || ");
        stat.initializer.accept(this);
        this.builder.append(");").newline();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldExpression expression) {
        IDBSPInnerNode parent = this.getParent();
        this.push(expression);
        boolean avoidRef = false;
        if (parent != null) {
            if (parent.is(DBSPBinaryExpression.class)) {
                DBSPBinaryExpression bin = parent.to(DBSPBinaryExpression.class);
                if (bin.opcode == DBSPOpcode.SQL_INDEX ||
                        bin.opcode == DBSPOpcode.RUST_INDEX ||
                        bin.opcode == DBSPOpcode.MAP_INDEX ||
                        bin.opcode == DBSPOpcode.SAFE_RUST_INDEX ||
                        bin.opcode == DBSPOpcode.VARIANT_INDEX) {
                    if (this.visitingChild == 0)
                        // We are explicitly taking a reference for these cases
                        avoidRef = true;
                }
            } else if (parent.is(DBSPBorrowExpression.class)) {
                // Pattern appearing in aggregation
                avoidRef = true;
            }
        }

        // Expression of the form x.0
        // We have four cases, depending on the nullability of x, and of the 0-th field
        // in the type of x.  Invariant:
        // we generate code such that
        // - if the field is further indexed, we return a reference to the field (as_ref())
        // - if the field is a structure, we return a reference (as_ref()).  We expect
        //   that the consumer will clone this if necessary.
        // - if the field is a scalar, we return the field's value (cloned())
        DBSPType sourceType = expression.expression.getType();
        boolean fieldTypeIsNullable = false;
        if (sourceType.is(DBSPTypeTupleBase.class)) {
            // This should always be true when we compile SQL programs, but
            // it may be false when we generate some testing code
            DBSPType fieldType = sourceType.to(DBSPTypeTupleBase.class).getFieldType(expression.fieldNo);
            fieldTypeIsNullable = fieldType.mayBeNull;
        }
        if (!sourceType.mayBeNull) {
            // x is not nullable
            expression.expression.accept(this);
            this.builder.append(".")
                    .append(expression.fieldNo);
            if (expression.getType().mayBeNull && !this.compilingAssignmentLHS && !avoidRef) {
                // The value is in an option.
                this.builder.append(".as_ref()");
                if (expression.getType().hasCopy()) {
                    // This cloned() is used to convert Option<&T> into a T value.
                    // It is done only for scalar Ts, because all other expressions
                    // are followed by a real clone() call, which will be also compiled into a cloned().
                    this.builder.append(".cloned()");
                }
            }
        } else {
            // Accessing a field within a nullable struct is allowed
            if (!expression.getType().mayBeNull) {
                // If the result is not null, we need to unwrap().
                // This should really not happen.
                throw new InternalCompilerError(
                        "Accessing nullable field produces a non-nullable result"
                                + System.lineSeparator() + Utilities.getCurrentStackTrace());
            } else {
                expression.expression.accept(this);
                if (!expression.expression.is(DBSPFieldExpression.class))
                    this.builder.append(".as_ref()");
                this.builder.increase();
                if (fieldTypeIsNullable) {
                    this.builder.append(".and_then(|x| x.");
                } else {
                    this.builder.append(".map(|x| ");
                    if (!expression.getType().hasCopy())
                        this.builder.append("&");
                    this.builder.append("x.");
                }
                this.builder.append(expression.fieldNo);
                if (fieldTypeIsNullable && !expression.getType().hasCopy() && !avoidRef) {
                    this.builder.append(".as_ref()");
                }
                this.builder.append(")").decrease();
            }
        }
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPReturnExpression expression) {
        this.push(expression);
        this.builder.append("return ");
        expression.argument.accept(this);
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIfExpression expression) {
        this.push(expression);
        this.builder.append("(if ");
        expression.condition.accept(this);
        this.builder.append(" ");
        if (!expression.positive.is(DBSPBlockExpression.class))
            this.builder.append("{")
                    .increase();
        expression.positive.accept(this);
        if (!expression.positive.is(DBSPBlockExpression.class))
            this.builder.decrease()
                    .append("\n}");
        if (expression.negative != null) {
            this.builder.append(" else ");
            if (!expression.negative.is(DBSPBlockExpression.class))
                this.builder.append("{")
                        .increase();
            expression.negative.accept(this);
            if (!expression.negative.is(DBSPBlockExpression.class))
                this.builder.decrease()
                        .append("\n}");
        }
        this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPPathExpression expression) {
        this.push(expression);
        expression.path.accept(this);
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPForExpression expression) {
        this.push(expression);
        this.builder.append("for ")
                .append(expression.variable)
                .append(" in ");
        expression.iterated.accept(this);
        this.builder.append(" ");
        expression.block.accept(this);
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPQualifyTypeExpression expression) {
        this.push(expression);
        expression.expression.accept(this);
        this.builder.append("::<");
        for (DBSPType type: expression.types) {
            type.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(">");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPRawTupleExpression expression) {
        if (expression.fields == null)
            return this.doNullExpression(expression);
        this.push(expression);
        if (expression.getType().mayBeNull)
            this.builder.append("Some");
        this.builder.append("(");
        boolean newlines = this.compact && expression.fields.length > 2;
        if (newlines)
            this.builder.increase();
        for (DBSPExpression field : expression.fields) {
            field.accept(this);
            this.builder.append(", ");
            if (newlines)
                this.builder.newline();
        }
        if (newlines)
            this.builder.decrease();
        this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTupleExpression expression) {
        if (expression.fields == null)
            return this.doNullExpression(expression);
        this.push(expression);
        if (expression.getType().mayBeNull)
            this.builder.append("Some(");
        boolean newlines = expression.fields.length > 2;
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
        if (expression.getType().mayBeNull)
            this.builder.append(")");
        this.pop(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConstructorExpression expression) {
        this.push(expression);
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
        this.pop(expression);
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
        this.push(type);
        if (this.compact) {
            this.builder.append(type.getRustString());
            if (type.mayBeNull)
                this.builder.append("?");
        } else {
            type.wrapOption(this.builder, type.getRustString());
        }
        this.pop(type);
        return VisitDecision.STOP;
    }

    void optionPrefix(DBSPType type) {
        if (type.mayBeNull && !this.compact)
            this.builder.append("Option<");
    }

    void optionSuffix(DBSPType type) {
        if (type.mayBeNull) {
            if (this.compact)
                this.builder.append("?");
            else
                this.builder.append(">");
        }
    }

    @Override
    public VisitDecision preorder(DBSPTypeRawTuple type) {
        this.push(type);
        this.optionPrefix(type);
        this.builder.append("(");
        for (DBSPType fType: type.tupFields) {
            fType.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(")");
        this.optionSuffix(type);
        this.pop(type);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeRef type) {
        this.push(type);
        this.optionPrefix(type);
        this.builder.append("&")
                .append(type.mutable ? "mut " : "");
        type.type.accept(this);
        this.optionSuffix(type);
        this.pop(type);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStream type) {
        this.push(type);
        this.builder.append("Stream<");
        if (type.outerCircuit)
            this.builder.append("RootCircuit");
        else
            this.builder.append("NestedCircuit");
        this.builder.append(", ");
        type.elementType.accept(this);
        this.builder.append(">");
        this.pop(type);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStruct.Field field) {
        this.push(field);
        this.builder.append(field.getSanitizedName())
                .append(": ");
        field.type.accept(this);
        this.pop(field);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStruct type) {
        // A *reference* to a struct type is just the type name.
        this.push(type);
        this.optionPrefix(type);
        this.builder.append(type.sanitizedName);
        this.optionSuffix(type);
        this.pop(type);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeTuple type) {
        this.push(type);
        this.optionPrefix(type);
        this.builder.append(type.getName());
        if (type.size() > 0) {
            this.builder.append("<");
            if (!this.compact || type.size() <= 10) {
                boolean first = true;
                for (DBSPType fType : type.tupFields) {
                    if (!first)
                        this.builder.append(", ");
                    first = false;
                    fType.accept(this);
                }
            }
            this.builder.append(">");
        }
        this.optionSuffix(type);
        this.pop(type);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeUser type) {
        this.push(type);
        this.optionPrefix(type);
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
        this.optionSuffix(type);
        this.pop(type);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregateList aggregate) {
        throw new InternalCompilerError("Should have been eliminated", aggregate.getNode());
    }

    @Override
    public VisitDecision preorder(IAggregate implementation) {
        throw new InternalCompilerError("Should have been eliminated", implementation.getNode());
    }

    public static String toRustString(DBSPCompiler compiler, IDBSPInnerNode node,
                                      @Nullable SourcePositionResource sourcePosition, boolean compact) {
        IndentStream stream = new IndentStreamBuilder();
        ToRustInnerVisitor visitor = new ToRustInnerVisitor(compiler, stream, sourcePosition, compact);
        node.accept(visitor);
        return stream.toString();
    }

    public static void toRustString(
            DBSPCompiler compiler, IIndentStream stream, IDBSPInnerNode node,
            @Nullable SourcePositionResource sourcePositionResource, boolean compact) {
        ToRustInnerVisitor visitor = new ToRustInnerVisitor(compiler, stream, sourcePositionResource, compact);
        node.accept(visitor);
    }
}
