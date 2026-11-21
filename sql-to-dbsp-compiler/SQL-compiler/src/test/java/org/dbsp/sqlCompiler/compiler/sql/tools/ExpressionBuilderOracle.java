package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import java.util.ArrayList;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeShortInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import java.util.List;
import java.util.Arrays;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.PositionOnlyRel;
import org.dbsp.sqlCompiler.ir.expression.DBSPGeoPointConstructor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVariantNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

/** Export builder inputs together with their results so cross-boundary sharing is observable. */
public final class ExpressionBuilderOracle {
    private final ObjectNode cases = Utilities.deterministicObjectMapper().createObjectNode();
    private final DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());

    private CalciteObject origin = CalciteObject.EMPTY;

    private void expand(String name, DBSPExpression input) throws Exception {
        var pair = new DBSPTupleExpression(input, ExpressionCompiler.expandTuple(this.origin, input));
        record(name, pair);
    }

    private void record(String name, DBSPTupleExpression example) throws Exception {
        var input = new DBSPTupleExpression(Arrays.copyOf(example.fields, example.fields.length - 1));
        for (boolean replay : List.of(false, true)) {
            ObjectNode record = this.cases.putObject(name + (replay ? "/checkpoint" : "/observation"));
            record.set("before", this.serialize(input, replay));
            record.set("after", this.serialize(example, replay));
        }
    }

    private com.fasterxml.jackson.databind.JsonNode serialize(DBSPExpression expression, boolean replay) throws Exception {
        var stream = new JsonStream(new IndentStream(new StringBuilder()), replay);
        expression.accept(new ToJsonInnerVisitor(this.compiler, stream, 1, replay));
        return Utilities.deterministicObjectMapper().readTree(stream.toString());
    }

    private void binary(String name, DBSPType target, DBSPOpcode op, DBSPExpression... args) throws Exception {
        var result = ExpressionCompiler.makeBinaryExpressions(this.origin, target, op, List.of(args));
        this.record(name + "/" + op.name(), new DBSPTupleExpression(new DBSPTupleExpression(args),
                target.var(), result));
    }

    private void rejection(String name, DBSPType target, DBSPOpcode op, DBSPExpression... args) throws Exception {
        try {
            ExpressionCompiler.makeBinaryExpressions(this.origin, target, op, List.of(args));
        } catch (org.dbsp.sqlCompiler.compiler.errors.BaseCompilerException expected) {
            var inputs = new DBSPTupleExpression(new DBSPTupleExpression(args), target.var());
            ObjectNode record = this.cases.putObject(name + "/" + op.name() + "/checkpoint");
            record.set("before", this.serialize(inputs, true));
            record.put("error", expected.getClass().getSimpleName());
            return;
        }
        throw new IllegalStateException("Expected Java to reject " + name);
    }

    private void binaryCases() throws Exception {
        var empty = CalciteObject.EMPTY;
        var integer = new DBSPTypeInteger(empty, 32, true, false);
        var small = new DBSPTypeInteger(empty, 16, true, false);
        var nullable = integer.withMayBeNull(true);
        var bool = new DBSPTypeBool(empty, false);
        var dec = new DBSPTypeDecimal(empty, 8, 2, false);
        var dbl = new DBSPTypeDouble(empty, false);
        var string = DBSPTypeString.varchar(false);
        this.binary("balanced_odd", integer, DBSPOpcode.ADD, integer.var(), integer.var(), integer.var(), integer.var(), integer.var());
        this.binary("mixed_widths", integer, DBSPOpcode.MUL, small.var(), integer.var(), small.var(), integer.var());
        this.binary("nullable", nullable, DBSPOpcode.ADD, nullable.var(), integer.var());
        this.binary("strict_null", nullable, DBSPOpcode.ADD, integer.var(), nullable.none());
        this.binary("nullable_comparison", bool.withMayBeNull(true), DBSPOpcode.GTE, nullable.var(), small.var());
        this.binary("distinct", bool, DBSPOpcode.IS_DISTINCT, integer.var(), nullable.var());
        this.binary("decimal_int", dec, DBSPOpcode.DIV, dec.var(), integer.var());
        this.binary("decimal_float", dbl, DBSPOpcode.DIV, dec.var(), dbl.var());
        this.binary("mixed_decimal", dec, DBSPOpcode.ADD, dec.var(), new DBSPTypeDecimal(empty, 6, 1, false).var());
        this.binary("string_comparison", bool, DBSPOpcode.LT, string.var(), integer.var());
        this.binary("string_concat", string, DBSPOpcode.CONCAT, string.var(), string.var());
        this.binary("variant_null_value", bool, DBSPOpcode.EQ, DBSPVariantNullLiteral.INSTANCE, DBSPVariantNullLiteral.INSTANCE);
        this.rejection("saturating_helper", integer, DBSPOpcode.ADD_SATURATING, integer.var(), integer.var());
        var interval = new DBSPTypeShortInterval(empty, DBSPTypeShortInterval.Units.DAYS, false);
        var ts = DBSPTypeTimestamp.create(false);
        this.binary("interval_mult", interval, DBSPOpcode.MUL, integer.var(), interval.var());
        this.binary("date_interval", ts, DBSPOpcode.ADD, interval.var(), ts.var());
        this.binary("aggregate_min", integer, DBSPOpcode.AGG_MIN, integer.var(), integer.var(), integer.var());
    }

    private void aggregateCases() throws Exception {
        var empty = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(empty, 32, true, false);
        var key = new DBSPTypeTuple(integer, DBSPTypeString.varchar(false));
        var range = new SourcePositionRange(new SqlParserPos(2, 3, 2, 10));
        var site = new PositionOnlyRel(range, List.of(range), false);
        for (int count : List.of(1, 2, 3, 5)) {
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            List<DBSPSimpleOperator> inputs = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                var value = new DBSPTypeTuple(integer, DBSPTypeString.varchar(i % 2 == 0));
                var collection = new DBSPTypeIndexedZSet(empty, key, value);
                var input = new DBSPConstantOperator(site, new DBSPIndexedZSetExpression(empty, collection));
                circuit.addOperator(input);
                inputs.add(input);
            }
            long next = DBSPVariablePath.nextGeneratedName();
            var publicInput = this.circuit(circuit, false);
            var checkpointInput = this.circuit(circuit, true);
            var result = CalciteToDBSPCompiler.combineAggregateList(site, key, inputs, circuit::addOperator);
            for (boolean replay : List.of(false, true)) {
                ObjectNode record = this.cases.putObject(count + (replay ? "/checkpoint" : "/observation"));
                record.put("next_generated_name", next);
                record.set("site_position", range.asJson());
                record.set("before", replay ? checkpointInput : publicInput);
                record.put("result", result.id);
                record.put("next_generated_name_after", DBSPVariablePath.nextGeneratedName());
                record.set("after", this.circuit(circuit, replay));
            }
        }
    }

    private com.fasterxml.jackson.databind.JsonNode circuit(DBSPCircuit circuit, boolean replay) throws Exception {
        ToJsonOuterVisitor writer = replay ? ToJsonOuterVisitor.createForReplay(this.compiler)
                : ToJsonOuterVisitor.create(this.compiler, 1);
        circuit.accept(writer);
        return Utilities.deterministicObjectMapper().readTree(writer.getJsonString());
    }

    public static void main(String[] args) throws Exception {
        var oracle = new ExpressionBuilderOracle();
        if (args.length == 1) {
            switch (args[0]) {
                case "binary" -> oracle.binaryCases();
                case "aggregate" -> oracle.aggregateCases();
                default -> throw new IllegalArgumentException("Unknown builder " + args[0]);
            }
            System.out.println(oracle.cases);
            return;
        }
        var integer = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false);
        var row = new DBSPTypeTuple(integer, DBSPTypeString.varchar(false));
        oracle.expand("nested_raw_tuple", new DBSPTypeRawTuple(row, integer).var());
        oracle.expand("nullable_row", row.withMayBeNull(true).var());
        oracle.expand("nullable_constructor", new DBSPTupleExpression(new DBSPI32Literal(3)).some());
        oracle.expand("nullable_field", new DBSPTypeTuple(integer.withMayBeNull(true)).var());
        oracle.expand("null_tuple", new DBSPTypeTuple(CalciteObject.EMPTY, true, null,
                DBSPTypeString.varchar(true)).none());
        var named = new DBSPTypeStruct(CalciteObject.EMPTY, new ProgramIdentifier("row_type"),
                List.of(new DBSPTypeStruct.Field(CalciteObject.EMPTY, new ProgramIdentifier("f"), 0, integer)), false);
        var structField = new DBSPTypeTuple(CalciteObject.EMPTY, true, null, named).none();
        try {
            ExpressionCompiler.expandTuple(oracle.origin, structField);
            throw new IllegalStateException("Expected the tuple constructor to reject a struct field");
        } catch (org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError expected) {
            var record = oracle.cases.putObject("null_struct_field/checkpoint");
            record.set("before", oracle.serialize(new DBSPTupleExpression(structField), true));
            record.put("error", expected.getClass().getSimpleName());
        }
        oracle.expand("owned_geopoint", new DBSPGeoPointConstructor());
        var range = new SourcePositionRange(new SqlParserPos(3, 4, 3, 9));
        oracle.origin = new PositionOnlyRel(range, List.of(range), false);
        oracle.expand("positioned_tuple", row.var(oracle.origin));
        System.out.println(oracle.cases);
    }
}
