package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.GlobalAggregate;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.ViewMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/** Tests for the TagRegions visitor */
public class TestTagRegions {
    DBSPCircuit createCircuit(Predicate<DBSPOperator> addAnnotation) {
        ProgramMetadata meta = new ProgramMetadata();
        DBSPCircuit circuit = new DBSPCircuit(meta);
        ProgramIdentifier table = new ProgramIdentifier("T");
        ProgramIdentifier view = new ProgramIdentifier("V");
        ProgramIdentifier col = new ProgramIdentifier("x");
        var field = new DBSPTypeStruct.Field(CalciteObject.EMPTY, col, 0, DBSPTypeBool.INSTANCE);
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        fields.add(field);
        DBSPTypeStruct str = new DBSPTypeStruct(
                CalciteObject.EMPTY, table, fields, false);
        DBSPTypeTuple tuple = str.toTuple();
        DBSPTypeZSet z = new DBSPTypeZSet(tuple);
        InputColumnMetadata inputMeta = new InputColumnMetadata(CalciteObject.EMPTY, col, tuple.getFieldType(0), false,
                null, null ,null, null, false);

        var source = new DBSPSourceMultisetOperator(
                CalciteEmptyRel.INSTANCE, CalciteObject.EMPTY, z, str,
                new TableMetadata(table, Linq.list(inputMeta), new ArrayList<>(), false, false, null),
                table, null);
        if (addAnnotation.test(source))
            source.addAnnotation(new GlobalAggregate(0), DBSPSimpleOperator.class);
        circuit.addOperator(source);
        DBSPVariablePath var = tuple.ref().var();
        DBSPClosureExpression id = new DBSPTupleExpression(var.deref().field(0)).closure(var);
        var map = new DBSPMapOperator(CalciteEmptyRel.INSTANCE, id, source.outputPort());
        if (addAnnotation.test(map))
            map.addAnnotation(new GlobalAggregate(0), DBSPSimpleOperator.class);
        circuit.addOperator(map);
        ViewMetadata vMeta = new ViewMetadata(view, new ArrayList<>(), SqlCreateView.ViewKind.STANDARD,
                -1, false, false, null);
        var sink = new DBSPSinkOperator(CalciteEmptyRel.INSTANCE, view, "V", str, vMeta, map.outputPort());
        if (addAnnotation.test(sink))
            sink.addAnnotation(new GlobalAggregate(0), DBSPSimpleOperator.class);
        circuit.addOperator(sink);
        return circuit;
    }

    @Test
    public void testAnnotationGap() {
        CompilerOptions options = new CompilerOptions();
        DBSPCompiler compiler = new DBSPCompiler(options);
        // Tag input and output; map is not tagged
        DBSPCircuit circuit = this.createCircuit(operator -> !operator.is(DBSPMapOperator.class));
        Assert.assertNotNull(circuit);
        TagRegions tag = new TagRegions(compiler);
        DBSPCircuit result = tag.apply(circuit);
        // there should be no annotations in the result
        CircuitVisitor hasAnnotation = new CircuitVisitor(compiler) {
            int seen = 0;

            @Override
            public void postorder(DBSPOperator operator) {
                Assert.assertTrue(operator.annotations.isEmpty());
                this.seen++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(3, this.seen);
            }
        };
        hasAnnotation.apply(result);
    }

    @Test
    public void testAnnotationContiguous() {
        CompilerOptions options = new CompilerOptions();
        DBSPCompiler compiler = new DBSPCompiler(options);
        // Tag everything
        DBSPCircuit circuit = this.createCircuit(operator -> true);
        Assert.assertNotNull(circuit);
        TagRegions tag = new TagRegions(compiler);
        DBSPCircuit result = tag.apply(circuit);
        // there should an annotations in the result
        CircuitVisitor hasAnnotation = new CircuitVisitor(compiler) {
            int seen = 0;

            @Override
            public void postorder(DBSPOperator operator) {
                Assert.assertFalse(operator.annotations.isEmpty());
                this.seen++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(3, this.seen);
            }
        };
        hasAnnotation.apply(result);
    }

    @Test
    public void testAnnotationTwoAnnotations() {
        CompilerOptions options = new CompilerOptions();
        DBSPCompiler compiler = new DBSPCompiler(options);
        // Tag everything
        DBSPCircuit circuit = this.createCircuit(operator -> true);
        // Add one more conflicting annotation on the input
        var input = Objects.requireNonNull(circuit.getInput(new ProgramIdentifier("T"))).asOperator();
        input.addAnnotation(new GlobalAggregate(1), DBSPSimpleOperator.class);
        Assert.assertEquals(2, input.annotations.size());

        Assert.assertNotNull(circuit);
        TagRegions tag = new TagRegions(compiler);
        DBSPCircuit result = tag.apply(circuit);

        // there should only one annotation in the result; the conflicting one is removed
        CircuitVisitor hasAnnotation = new CircuitVisitor(compiler) {
            int seen = 0;

            @Override
            public void postorder(DBSPOperator operator) {
                Assert.assertFalse(operator.annotations.isEmpty());
                this.seen++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(3, this.seen);
            }
        };
        hasAnnotation.apply(result);
    }
}
