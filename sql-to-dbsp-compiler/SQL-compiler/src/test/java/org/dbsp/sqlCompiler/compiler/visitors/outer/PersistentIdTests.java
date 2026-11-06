package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.ViewMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.MerkleHashes;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PersistentIdTests extends BaseSQLTests {
    private static final DBSPType INT = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, false);

    private static DBSPSourceMultisetOperator table(DBSPCircuit circuit, String name) {
        ProgramIdentifier tableName = new ProgramIdentifier(name);
        ProgramIdentifier column = new ProgramIdentifier("k");
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        fields.add(new DBSPTypeStruct.Field(CalciteObject.EMPTY, column, 0, INT));
        List<InputColumnMetadata> columnMetadata = new ArrayList<>();
        columnMetadata.add(new InputColumnMetadata(
                CalciteObject.EMPTY, column, INT, false, null, null, null, false));
        DBSPTypeStruct struct = new DBSPTypeStruct(CalciteObject.EMPTY, tableName, fields, false);
        DBSPSourceMultisetOperator result = new DBSPSourceMultisetOperator(
                CalciteEmptyRel.INSTANCE, CalciteObject.EMPTY, new DBSPTypeZSet(struct.toTuple()), struct,
                new TableMetadata(tableName, columnMetadata, new ArrayList<>(), null, false, false, null),
                tableName, StreamKind.DELTA, null);
        circuit.addOperator(result);
        return result;
    }

    private static void view(DBSPCircuit circuit, String name, DBSPSimpleOperator input) {
        ProgramIdentifier viewName = new ProgramIdentifier(name);
        DBSPTypeTuple row = input.getOutputZSetElementType().to(DBSPTypeTuple.class);
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        for (int i = 0; i < row.size(); i++)
            fields.add(new DBSPTypeStruct.Field(
                    CalciteObject.EMPTY, new ProgramIdentifier("c" + i), i, row.getFieldType(i)));
        DBSPTypeStruct struct = new DBSPTypeStruct(CalciteObject.EMPTY, viewName, fields, false);
        ViewMetadata metadata = new ViewMetadata(
                viewName, new ArrayList<>(), SqlCreateView.ViewKind.STANDARD, -1, false, false, null);
        circuit.addOperator(new DBSPSinkOperator(
                CalciteEmptyRel.INSTANCE, viewName, name, struct, metadata, input.outputPort()));
    }

    /** Two maps compute Tup1::new(Some(1i32)), one with some() and one with a nullable literal. */
    @Test
    public void someOfLiteral() {
        DBSPCompiler compiler = this.testCompiler();
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), false);
        DBSPSourceMultisetOperator t = table(circuit, "T");
        List<DBSPExpression> values = List.of(new DBSPI32Literal(1).some(), new DBSPI32Literal(1, true));
        for (int i = 0; i < values.size(); i++) {
            DBSPVariablePath row = t.getOutputZSetElementType().ref().var();
            DBSPMapOperator map = new DBSPMapOperator(CalciteEmptyRel.INSTANCE,
                    new DBSPTupleExpression(values.get(i)).closure(row), t.outputPort());
            circuit.addOperator(map);
            view(circuit, "V" + i, map);
        }
        circuit = new CSE(compiler).apply(circuit);
        circuit = new CanonicalForm(compiler).getCircuitRewriter(false).apply(circuit);
        // The test passes if MerkleHashes does not crash with duplicate persistent ids
        new MerkleHashes(compiler).apply(circuit);
    }

    /** Pairs of views that use one interval value written in two units. */
    @Test
    public void intervalUnits() {
        // The test passes if the compiler does not crash with duplicate persistent ids
        this.getCC("""
                CREATE TABLE t (ts TIMESTAMP NOT NULL, ts2 TIMESTAMP NOT NULL, x INT, c BOOLEAN);
                CREATE VIEW v1 AS SELECT ts FROM t WHERE ts >= NOW() - INTERVAL 60 MINUTES;
                CREATE VIEW v2 AS SELECT x FROM t WHERE ts >= NOW() - INTERVAL 1 HOUR;
                CREATE VIEW v3 AS SELECT ts FROM t WHERE (ts - ts2) MINUTE > INTERVAL 60 MINUTE;
                CREATE VIEW v4 AS SELECT ts FROM t WHERE (ts - ts2) MINUTE > INTERVAL 1 HOUR;
                CREATE VIEW v5 AS SELECT ts FROM t WHERE ts > ts2 - (-INTERVAL 60 MINUTES);
                CREATE VIEW v6 AS SELECT ts FROM t WHERE ts > ts2 - (-INTERVAL 1 HOUR);
                CREATE VIEW v7 AS SELECT ts FROM t
                WHERE ts > ts2 + CASE WHEN c THEN INTERVAL 60 MINUTES ELSE INTERVAL 120 MINUTES END;
                CREATE VIEW v8 AS SELECT ts FROM t
                WHERE ts > ts2 + CASE WHEN c THEN INTERVAL 1 HOUR ELSE INTERVAL 2 HOURS END;
                CREATE VIEW v9 AS SELECT ts FROM t
                WHERE ts > ts2 + COALESCE(CASE WHEN c THEN INTERVAL 60 MINUTES END, INTERVAL 60 MINUTES);
                CREATE VIEW v10 AS SELECT ts FROM t
                WHERE ts > ts2 + COALESCE(CASE WHEN c THEN INTERVAL 1 HOUR END, INTERVAL 1 HOUR);
                CREATE VIEW v11 AS SELECT ts FROM t WHERE ts > ts2 + INTERVAL 12 MONTHS;
                CREATE VIEW v12 AS SELECT ts FROM t WHERE ts > ts2 + INTERVAL 1 YEAR;""");
    }
}
