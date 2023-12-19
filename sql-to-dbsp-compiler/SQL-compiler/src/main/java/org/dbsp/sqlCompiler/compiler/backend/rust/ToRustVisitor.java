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

import org.dbsp.sqlCompiler.circuit.*;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

/**
 * This visitor generate a Rust implementation of the program.
 */
public class ToRustVisitor extends CircuitVisitor {
    protected final IndentStream builder;
    public final InnerVisitor innerVisitor;

    public ToRustVisitor(IErrorReporter reporter, IndentStream builder) {
        super(reporter);
        this.builder = builder;
        this.innerVisitor = new ToRustInnerVisitor(reporter, builder, false);
    }

    //////////////// Operators

    private void genRcCell(DBSPOperator op) {
        this.builder.append("let ")
                .append(op.getName())
                .append(" = Rc::new(RefCell::<");
        op.getType().accept(this.innerVisitor);
        this.builder.append(">::new(Default::default()));")
                .newline();
        this.builder.append("let ")
                .append(op.getName())
                .append("_external = ")
                .append(op.getName())
                .append(".clone();")
                .newline();
        if (op instanceof DBSPSourceMultisetOperator) {
            this.builder.append("let ")
                    .append(op.getName())
                    .append(" = Generator::new(move || ")
                    .append(op.getName())
                    .append(".borrow().clone());")
                    .newline();
        }
    }

    void processNode(IDBSPNode node) {
        DBSPOperator op = node.as(DBSPOperator.class);
        if (op != null)
            this.generateOperator(op);
        IDBSPInnerNode inner = node.as(IDBSPInnerNode.class);
        if (inner != null) {
            inner.accept(this.innerVisitor);
            this.builder.newline();
        }
    }

    void generateOperator(DBSPOperator operator) {
        String str = operator.getNode().toInternalString();
        this.writeComments(str);
        operator.accept(this);
        this.builder.newline();
    }

    public void generateBody(DBSPPartialCircuit circuit) {
        this.builder.append("let root = dbsp::RootCircuit::build(|circuit| {")
                .increase();
        for (IDBSPNode node : circuit.getAllOperators())
            this.processNode(node);
        this.builder.append("Ok(())");
        this.builder.decrease()
                .append("})")
                .append(".unwrap();")
                .newline();
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        this.builder.append("fn ")
                .append(circuit.name);
        circuit.circuit.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPPartialCircuit circuit) {
        // function prototype:
        // fn name() -> impl FnMut(T0, T1) -> (O0, O1) {
        boolean first = true;
        this.builder.append("() -> impl FnMut(");
        for (DBSPOperator i : circuit.inputOperators) {
            if (!first)
                this.builder.append(",");
            first = false;
            i.getType().accept(this.innerVisitor);
        }
        this.builder.append(") -> ");
        DBSPTypeRawTuple tuple = new DBSPTypeRawTuple(CalciteObject.EMPTY, Linq.map(circuit.outputOperators, DBSPOperator::getType));
        tuple.accept(this.innerVisitor);
        this.builder.append(" {").increase();
        // For each input and output operator a corresponding Rc cell
        for (DBSPOperator i : circuit.inputOperators)
            this.genRcCell(i);

        for (DBSPOperator o : circuit.outputOperators)
            this.genRcCell(o);

        // Circuit body
        this.generateBody(circuit);

        // Create the closure and return it.
        this.builder.append("return move |")
                .joinS(", ", Linq.map(circuit.inputOperators, DBSPOperator::getName))
                .append("| {")
                .increase();

        for (DBSPOperator i : circuit.inputOperators)
            builder.append("*")
                    .append(i.getName())
                    .append("_external.borrow_mut() = ")
                    .append(i.getName())
                    .append(";")
                    .newline();
        this.builder.append("root.0.step().unwrap();")
                .newline()
                .append("return ")
                .append("(")
                .intercalateS(", ",
                        Linq.map(circuit.outputOperators, o -> o.getName() + "_external.borrow().clone()"))
                .append(")")
                .append(";")
                .newline()
                .decrease()
                .append("};")
                .newline()
                .decrease()
                .append("}")
                .newline();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSourceMultisetOperator operator) {
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(" = ")
                .append("circuit.add_source(")
                .append(operator.outputName)
                .append(");");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSourceMapOperator operator) {
       throw new UnimplementedException();
    }

    @Override
    public VisitDecision preorder(DBSPDistinctOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(operator.input().getName())
                .append(".")
                .append(operator.operation)
                .append("();");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPControlledFilterOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        boolean isZset = operator.getType().is(DBSPTypeZSet.class);
        this.builder.append(" = ")
                .append(operator.inputs.get(0).getName())
                .append(".apply2(&")
                .append(operator.inputs.get(1).getName())
                .append(", |d, v| ");
        if (isZset)
            this.builder.append("zset_");
        else
            this.builder.append("indexed_zset_");
        this.builder.append("filter_comparator(d, v, ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append("));");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPWaterlineOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(operator.input().getName())
                .append(".")
                .append(operator.operation)
                .append("(");
        operator.init.accept(this.innerVisitor);
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(");");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSinkOperator operator) {
        this.writeComments(operator)
                .append(operator.input().getName())
                .append(".")
                .append(operator.operation) // inspect
                .append("(move |m| { *")
                .append(operator.getName())
                .append(".borrow_mut() = ")
                .append("m.clone() });");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            builder.append(operator.inputs.get(0).getName())
                    .append(".");
        builder.append(operator.operation)
                .append("(");
        for (int i = 1; i < operator.inputs.size(); i++) {
            if (i > 1)
                builder.append(",");
            builder.append("&")
                    .append(operator.inputs.get(i).getName());
        }
        if (operator.function != null) {
            if (operator.inputs.size() > 1)
                builder.append(", ");
            operator.function.accept(this.innerVisitor);
        }
        builder.append(");");
        return VisitDecision.STOP;
    }

    /**
     * Helper function for generateComparator and generateCmpFunc.
     * @param fieldNo  Field index that is compared.
     * @param ascending Comparison direction.
     */
    void emitCompareField(int fieldNo, boolean ascending) {
        this.builder.append("let ord = left.")
                .append(fieldNo)
                .append(".cmp(&right.")
                .append(fieldNo)
                .append(");")
                .newline();
        this.builder.append("if ord != Ordering::Equal { return ord");
        if (!ascending)
            this.builder.append(".reverse()");
        this.builder.append("};")
                .newline();
    }

    /**
     * Helper function for generateCmpFunc.
     * This could be part of an inner visitor too.
     * But we don't handle DBSPComparatorExpressions in the same way in
     * any context: we do it differently in TopK and Sort.
     * This is for TopK.
     * @param comparator  Comparator expression to generate Rust for.
     * @param fieldsCompared  Accumulate here a list of all fields compared.
     */
    void generateComparator(DBSPComparatorExpression comparator, Set<Integer> fieldsCompared) {
        // This could be done with a visitor... but there are only two cases
        if (comparator.is(DBSPNoComparatorExpression.class))
            return;
        DBSPFieldComparatorExpression fieldComparator = comparator.to(DBSPFieldComparatorExpression.class);
        this.generateComparator(fieldComparator.source, fieldsCompared);
        if (fieldsCompared.contains(fieldComparator.fieldNo))
            throw new InternalCompilerError("Field " + fieldComparator.fieldNo + " used twice in sorting");
        fieldsCompared.add(fieldComparator.fieldNo);
        this.emitCompareField(fieldComparator.fieldNo, fieldComparator.ascending);
    }

    void generateCmpFunc(DBSPExpression function, String structName) {
        //    impl CmpFunc<(String, i32, i32)> for AscDesc {
        //        fn cmp(left: &(String, i32, i32), right: &(String, i32, i32)) -> std::cmp::Ordering {
        //            let ord = left.1.cmp(&right.1);
        //            if ord != Ordering::Equal { return ord; }
        //            let ord = right.2.cmp(&left.2);
        //            if ord != Ordering::Equal { return ord; }
        //            let ord = left.3.cmp(&right.3);
        //            if ord != Ordering::Equal { return ord; }
        //            return Ordering::Equal;
        //        }
        //    }
        DBSPComparatorExpression comparator = function.to(DBSPComparatorExpression.class);
        DBSPType type = comparator.tupleType();
        this.builder.append("impl CmpFunc<");
        type.accept(this.innerVisitor);
        this.builder.append("> for ")
                .append(structName)
                .append(" {")
                .increase()
                .append("fn cmp(left: &");
        type.accept(this.innerVisitor);
        this.builder.append(", right: &");
        type.accept(this.innerVisitor);
        this.builder.append(") -> std::cmp::Ordering {")
                .increase();
        // This is a subtle aspect. The comparator compares on some fields,
        // but we have to generate a comparator on ALL the fields, to avoid
        // declaring values as equal when they aren't really.
        Set<Integer> fieldsCompared = new HashSet<>();
        this.generateComparator(comparator, fieldsCompared);
        // Now compare on the fields that we didn't compare on.
        // The order doesn't really matter.
        for (int i = 0; i < type.to(DBSPTypeTuple.class).size(); i++) {
            if (fieldsCompared.contains(i)) continue;
            this.emitCompareField(i, true);
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

    DBSPClosureExpression generateEqualityComparison(DBSPExpression comparator) {
        CalciteObject node = comparator.getNode();
        DBSPExpression result = new DBSPBoolLiteral(true);
        DBSPComparatorExpression comp = comparator.to(DBSPComparatorExpression.class);
        DBSPType type = comp.tupleType().ref();
        DBSPVariablePath left = new DBSPVariablePath("left", type);
        DBSPVariablePath right = new DBSPVariablePath("right", type);
        while (comparator.is(DBSPFieldComparatorExpression.class)) {
            DBSPFieldComparatorExpression fc = comparator.to(DBSPFieldComparatorExpression.class);
            DBSPExpression eq = new DBSPBinaryExpression(node, new DBSPTypeBool(node, false),
                    DBSPOpcode.IS_NOT_DISTINCT, left.deref().field(fc.fieldNo), right.deref().field(fc.fieldNo));
            result = new DBSPBinaryExpression(node, eq.getType(), DBSPOpcode.AND, result, eq);
            comparator = fc.source;
        }
        return result.closure(left.asParameter(), right.asParameter());
    }

    @Override
    public VisitDecision preorder(DBSPIndexedTopKOperator operator) {
        // TODO: this should be a fresh identifier.
        String structName = "Cmp" + operator.outputName;
        this.builder.append("struct ")
                .append(structName)
                .append(";")
                .newline();

        // Generate a CmpFunc impl for the new struct.
        this.generateCmpFunc(operator.getFunction(), structName);

        String streamOperation = "topk_custom_order";
        if (operator.outputProducer != null) {
            switch (operator.numbering) {
                case ROW_NUMBER:
                    streamOperation = "topk_row_number_custom_order";
                    break;
                case RANK:
                    streamOperation = "topk_rank_custom_order";
                    break;
                case DENSE_RANK:
                    streamOperation = "topk_dense_rank_custom_order";
                    break;
            }
        }

        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(operator.input().getName())
                .append(".")
                .append(streamOperation)
                .append("::<")
                .append(structName);
                if (operator.outputProducer != null) {
                    this.builder.append(", _, _");
                    if (operator.numbering != DBSPIndexedTopKOperator.TopKNumbering.ROW_NUMBER)
                        this.builder.append(", _");
                }
                this.builder.append(">(");
        DBSPExpression cast = operator.limit.cast(
                new DBSPTypeUSize(CalciteObject.EMPTY, operator.limit.getType().mayBeNull));
        cast.accept(this.innerVisitor);
        if (operator.outputProducer != null) {
            if (operator.numbering != DBSPIndexedTopKOperator.TopKNumbering.ROW_NUMBER) {
                this.builder.append(", ");
                DBSPExpression equalityComparison = this.generateEqualityComparison(
                        operator.getFunction());
                equalityComparison.accept(this.innerVisitor);
            }
            this.builder.append(", ");
            operator.outputProducer.accept(this.innerVisitor);
        }
        builder.append(");");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPWindowAggregateOperator operator) {
        // We generate two DBSP operator calls: partitioned_rolling_aggregate
        // and map_index
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        String tmp = new NameGen("stream").nextName();
        this.writeComments(operator)
                .append("let ")
                .append(tmp)
                .append(" = ")
                .append(operator.input().getName())
                .append(".partitioned_rolling_aggregate(");
        operator.getFunction().accept(this.innerVisitor);
        builder.append(", ");
        operator.window.accept(this.innerVisitor);
        builder.append(");")
                .newline();

        this.builder.append("let ")
                .append(operator.getName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        builder.append(" = " )
                .append(tmp)
                .append(".map_index(|(key, (ts, agg))| { ((key.clone(), *ts), agg.unwrap_or_default())});");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregateOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        builder.append(operator.input().getName())
                    .append(".");
        builder.append(operator.operation)
                .append("(");
        operator.getFunction().accept(this.innerVisitor);
        builder.append(");");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSumOperator operator) {
        this.writeComments(operator)
                    .append("let ")
                    .append(operator.getName())
                    .append(": ");
        new DBSPTypeStream(operator.outputType).accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            this.builder.append(operator.inputs.get(0).getName())
                        .append(".");
        this.builder.append(operator.operation)
                    .append("([");
        for (int i = 1; i < operator.inputs.size(); i++) {
            if (i > 1)
                this.builder.append(", ");
            this.builder.append("&").append(operator.inputs.get(i).getName());
        }
        this.builder.append("]);");
        return VisitDecision.STOP;
    }

    IIndentStream writeComments(@Nullable String comment) {
        if (comment == null || comment.isEmpty())
            return this.builder;
        String[] parts = comment.split("\n");
        parts = Linq.map(parts, p -> "// " + p, String.class);
        return this.builder.intercalate("\n", parts);
    }

     IIndentStream writeComments(DBSPOperator operator) {
        return this.writeComments(operator.getClass().getSimpleName() + " " + operator.id +
                (operator.comment != null ? "\n" + operator.comment : ""));
    }

    @Override
    public VisitDecision preorder(DBSPJoinOperator operator) {
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(": ");
        new DBSPTypeStream(operator.outputType).accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            this.builder.append(operator.inputs.get(0).getName())
                    .append(".");
        this.builder.append(operator.operation)
                .append("(&");
        this.builder.append(operator.inputs.get(1).getName());
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(");");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConstantOperator operator) {
        assert operator.function != null;
        builder.append("let ")
                .append(operator.getName())
                .append(" = ")
                .append("circuit.add_source(Generator::new(|| ");
        operator.function.accept(this.innerVisitor);
        this.builder.append("));");
        return VisitDecision.STOP;
    }

    public static String toRustString(IErrorReporter reporter, IDBSPOuterNode node) {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        LowerCircuitVisitor lower = new LowerCircuitVisitor(reporter);
        node = lower.apply(node.to(DBSPCircuit.class));
        ToRustVisitor visitor = new ToRustVisitor(reporter, stream);
        node.accept(visitor);
        return builder.toString();
    }
}
