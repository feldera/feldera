package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LineageTests extends BaseSQLTests {
    @Test
    public void testInnerLineage() {
        Lineage.InnerLineage lineage = new Lineage.InnerLineage(this.testCompiler());

        DBSPType i32 = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, false);
        var var = new DBSPVariablePath(new DBSPTypeTuple(i32, i32, i32).ref());
        DBSPExpression plus = new DBSPBinaryExpression(
                CalciteObject.EMPTY, i32, DBSPOpcode.ADD, var.deref().field(0), var.deref().field(1));
        DBSPClosureExpression tup = new DBSPTupleExpression(plus, var.deref().field(2)).closure(var);

        List<Lineage.ValueSource> cols = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Lineage.ValueSource col = new Lineage.Atom<>(
                    new Lineage.ColumnSet(new Lineage.TableColumn("t", "col" + i)));
            cols.add(col);
        }
        DBSPParameter param = tup.parameters[0];
        Map<DBSPParameter, Lineage.ValueSource> initial = new HashMap<>();
        initial.put(param, new Lineage.Ref(new Lineage.Tuple(cols)));

        Lineage.ValueSource result = lineage.analyze(tup, initial);
        Assert.assertNotNull(result);
        Assert.assertEquals("Tuple[fields=[Unknown[], [t.col2]]]", result.toString());

        ///////////////

        var = new DBSPVariablePath(new DBSPTypeRawTuple(new DBSPTypeTuple(i32, i32).ref(), new DBSPTypeTuple(i32, i32, i32).ref()));
        DBSPExpression func = new DBSPApplyExpression("func", i32, var.field(1).deref()).applyClone();
        DBSPExpression comp = new DBSPBinaryExpression(CalciteObject.EMPTY, DBSPTypeBool.INSTANCE,
                DBSPOpcode.EQ, var.field(1).deref().field(0), var.field(0).deref().field(0));
        DBSPIfExpression cond = new DBSPIfExpression(CalciteObject.EMPTY, comp,
                var.field(0).deref().field(1), var.field(1).deref().field(2));
        tup = new DBSPTupleExpression(
                cond,
                func,
                var.field(1).deref().field(1).applyClone(),
                new DBSPI32Literal(1)).closure(var);

        List<Lineage.ValueSource> cols0 = new ArrayList<>();
        List<Lineage.ValueSource> cols1 = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Lineage.ValueSource col = new Lineage.Atom<>(
                    new Lineage.ColumnSet(new Lineage.TableColumn("t", "col" + i)));
            cols0.add(col);
        }
        for (int i = 2; i < 5; i++) {
            Lineage.ValueSource col = new Lineage.Atom<>(
                    new Lineage.ColumnSet(new Lineage.TableColumn("t", "col" + i)));
            cols1.add(col);
        }

        param = tup.parameters[0];
        initial.put(param, new Lineage.Tuple(Linq.list(
                new Lineage.Ref(new Lineage.Tuple(cols0)),
                new Lineage.Ref(new Lineage.Tuple(cols1)))));
        result = lineage.analyze(tup, initial);
        Assert.assertNotNull(result);
        Assert.assertEquals("Tuple[fields=[Unknown[], Unknown[], [t.col3], Constant[value=1]]]", result.toString());
    }

    @Test
    public void testCorrelateFlag() throws IOException, SQLException {
        String sql = """
                CREATE TABLE T(X INT, Y INT, Z INT);
                CREATE TABLE S(X INT, Y INT);
                CREATE LOCAL VIEW W AS SELECT T.X AS X, S.Y AS Y FROM T JOIN S ON T.X = S.X;
                CREATE VIEW V AS SELECT X+1, Y, 2 FROM W WHERE X - 2 < Y;""";
        File file = createInputScript(sql);
        PrintStream save = System.out;
        ByteArrayOutputStream capture = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capture));
        CompilerMain.execute("--noRust", "--correlatedColumns", file.getPath());
        System.setOut(save);
        Assert.assertEquals("[Correlated:] [s.x, t.x]\n", capture.toString());
    }
}
