package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.sql.SqlInsert;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlRemove;
import org.dbsp.sqlCompiler.compiler.frontend.statements.TableModifyStatement;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** The lowering must use captured statement metadata, even if its source AST
 * subsequently changes. This is a dependency test for detached S1 replay. */
public class S1StatementMetadataTests {
    private record Result(DBSPCompiler compiler, List<TableModifyStatement> modifications) { }

    private static Result compile(String sql, boolean changeSourceOrder) {
        var options = new CompilerOptions();
        options.ioOptions.testing = true;
        options.ioOptions.inputCircuit = true;
        var compiler = new DBSPCompiler(options);
        var modifications = new ArrayList<TableModifyStatement>();
        compiler.relationalObservationHook = statement -> {
            if (!(statement instanceof TableModifyStatement modify)) return;
            modifications.add(modify);
            if (!changeSourceOrder || modify.targetColumns == null) return;
            var source = modify.parsedStatement.statement();
            var columns = source instanceof SqlInsert insert
                    ? insert.getTargetColumnList() : ((SqlRemove) source).getTargetColumnList();
            // Reverse the parsed selectors after capture. Relational values and
            // the immutable target metadata retain the original semantics.
            Assert.assertNotNull(columns);
            var first = columns.get(0);
            columns.set(0, columns.get(2));
            columns.set(2, first);
        };
        compiler.submitStatementsForCompilation(sql);
        Assert.assertNotNull(compiler.getFinalCircuit(false));
        compiler.throwIfErrorsOccurred();
        return new Result(compiler, modifications);
    }

    private static void assertRow(DBSPCompiler compiler, int a, int mixed, int c) {
        var data = compiler.getTableContents().getTableContents(new ProgramIdentifier("t"));
        Assert.assertEquals(1, data.data.size());
        var entry = data.data.entrySet().iterator().next();
        Assert.assertEquals(Long.valueOf(1), entry.getValue());
        var tuple = entry.getKey().to(DBSPTupleExpression.class);
        int[] expected = { a, mixed, c };
        for (int i = 0; i < expected.length; i++)
            Assert.assertEquals(Integer.valueOf(expected[i]), tuple.get(i).to(DBSPI32Literal.class).value);
    }

    @Test public void insertOrderQuotingAndPositionsAreDetachedFromSourceAst() {
        String insertion = "INSERT INTO t(c, \"MiXeD\", a) VALUES (30, 20, 10);";
        var result = compile("CREATE TABLE t(a INT NOT NULL, \"MiXeD\" INT NOT NULL, c INT NOT NULL);\n" + insertion, true);
        assertRow(result.compiler(), 10, 20, 30);
        Assert.assertEquals(1, result.modifications().size());
        var columns = result.modifications().get(0).targetColumns;
        Assert.assertNotNull(columns);
        Assert.assertEquals(List.of("c", "MiXeD", "a"), columns.stream().map(c -> c.name().name()).toList());
        Assert.assertTrue(columns.get(1).name().isQuoted());
        Assert.assertEquals(2, columns.get(1).position().start.line);
        Assert.assertEquals(insertion.indexOf("\"MiXeD\"") + 1, columns.get(1).position().start.column);
        Assert.assertTrue(columns.get(1).displayName().contains("MiXeD"));
        Assert.assertThrows(UnsupportedOperationException.class, columns::clear);
    }

    @Test public void removeOrderAndImplicitInsertOrderRemainDistinct() {
        var result = compile("""
                CREATE TABLE t(a INT NOT NULL, "MiXeD" INT NOT NULL, c INT NOT NULL);
                INSERT INTO t VALUES (10, 20, 30), (40, 50, 60);
                REMOVE FROM t(c, a, "MiXeD") VALUES (60, 40, 50);
                """, true);
        assertRow(result.compiler(), 10, 20, 30);
        Assert.assertEquals(2, result.modifications().size());
        Assert.assertNull(result.modifications().get(0).targetColumns);
        var remove = result.modifications().get(1);
        Assert.assertFalse(remove.insert);
        Assert.assertNotNull(remove.targetColumns);
        Assert.assertEquals(List.of("c", "a", "MiXeD"), remove.targetColumns.stream().map(c -> c.name().name()).toList());
        Assert.assertEquals(3, remove.targetColumns.get(0).position().start.line);
    }
    @Test public void observationOrderIsTheActualLoweringOrder() {
        var options = new CompilerOptions();
        options.ioOptions.inputCircuit = true;
        var compiler = new DBSPCompiler(options);
        var kinds = new ArrayList<String>();
        var sourceLines = new ArrayList<Integer>();
        compiler.relationalObservationHook = statement -> {
            if (!statement.isVisible()) return;
            kinds.add(statement.getClass().getSimpleName());
            sourceLines.add(statement.getPosition().start.line);
        };
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t(a INT);
                CREATE TYPE point_t AS (x INT, y INT);
                CREATE VIEW v AS SELECT * FROM t;
                """);
        Assert.assertNotNull(compiler.getFinalCircuit(false));
        compiler.throwIfErrorsOccurred();
        Assert.assertEquals(List.of("CreateTypeStatement", "CreateTableStatement", "CreateViewStatement"), kinds);
        Assert.assertEquals(List.of(2, 1, 3), sourceLines);
    }

    @Test public void namedTypeAttributesRemainBoundAfterSourceAstChanges() {
        var options = new CompilerOptions(); options.ioOptions.inputCircuit = true;
        var compiler = new DBSPCompiler(options);
        var captured = new ArrayList<org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTypeStatement>();
        compiler.relationalObservationHook = statement -> {
            if (!(statement instanceof org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTypeStatement type)
                    || !type.typeName.name().equals("outer_t")) return;
            captured.add(type);
            var syntax = (org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateType) type.parsedStatement.statement();
            Assert.assertNotNull(syntax.attributeDefs);
            var first = syntax.attributeDefs.get(0);
            syntax.attributeDefs.set(0, syntax.attributeDefs.get(1)); syntax.attributeDefs.set(1, first);
        };
        compiler.submitStatementsForCompilation("""
                CREATE TYPE inner_t AS (x INT);
                CREATE TYPE outer_t AS ("MiXeD" inner_t, flag BOOLEAN);
                CREATE TABLE t(id INT);
                """);
        Assert.assertNotNull(compiler.getFinalCircuit(false)); compiler.throwIfErrorsOccurred();
        Assert.assertEquals(1, captured.size());
        var type = compiler.getStructByName(new ProgramIdentifier("outer_t"));
        Assert.assertNotNull(type);
        var nested = type.getField(new ProgramIdentifier("MiXeD", true));
        Assert.assertNotNull(nested);
        Assert.assertTrue(nested.type.is(org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct.class));
        Assert.assertEquals(new ProgramIdentifier("inner_t"), nested.type.to(org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct.class).name);
        Assert.assertTrue(type.getField(new ProgramIdentifier("flag")).type.is(org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool.class));
        Assert.assertThrows(UnsupportedOperationException.class, captured.get(0).attributes::clear);
    }

    @Test public void indexColumnPositionsAreImmutableStatementMetadata() {
        var options = new CompilerOptions(); options.ioOptions.inputCircuit = true;
        var compiler = new DBSPCompiler(options);
        var captured = new ArrayList<org.dbsp.sqlCompiler.compiler.frontend.statements.CreateIndexStatement>();
        compiler.relationalObservationHook = statement -> {
            if (!(statement instanceof org.dbsp.sqlCompiler.compiler.frontend.statements.CreateIndexStatement index)) return;
            captured.add(index);
            var syntax = (org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateIndex) index.parsedStatement.statement();
            var first = syntax.columns.get(0);
            syntax.columns.set(0, syntax.columns.get(1)); syntax.columns.set(1, first);
            Assert.assertEquals(first.getParserPosition(), index.columnParserPosition(0));
            Assert.assertNotEquals(syntax.columns.get(0).getParserPosition(), index.columnParserPosition(0));
        };
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t(a INT, b INT);
                CREATE VIEW v AS SELECT * FROM t;
                CREATE INDEX ix ON v(b, a);
                """);
        Assert.assertNotNull(compiler.getFinalCircuit(false)); compiler.throwIfErrorsOccurred();
        Assert.assertEquals(1, captured.size());
        var index = captured.get(0);
        Assert.assertEquals(List.of(new ProgramIdentifier("b"), new ProgramIdentifier("a")), index.columns);
        Assert.assertThrows(UnsupportedOperationException.class, index.columns::clear);
        Assert.assertThrows(UnsupportedOperationException.class, index.columnPositions::clear);
    }

    @Test public void reorderedValuesKeepDestinationTypesAndNullability() {
        var result = compile("""
                CREATE TABLE t(a INT NOT NULL, b INT);
                INSERT INTO t(b, a) VALUES (20, 10), (NULL, 30);
                REMOVE FROM t(b, a) VALUES (20, 10);
                """, false);
        var rows = result.compiler().getTableContents().getTableContents(new ProgramIdentifier("t"));
        Assert.assertEquals(1, rows.data.size());
        var entry = rows.data.entrySet().iterator().next();
        var row = entry.getKey().to(DBSPTupleExpression.class);
        Assert.assertEquals(Integer.valueOf(30), row.get(0).to(DBSPI32Literal.class).value);
        Assert.assertNull(row.get(1).to(DBSPI32Literal.class).value);
        Assert.assertFalse(row.get(0).getType().mayBeNull); Assert.assertTrue(row.get(1).getType().mayBeNull);
        Assert.assertEquals(Long.valueOf(1), entry.getValue());
    }

    @Test public void reorderedProjectedAndUnionRowsUseTheSamePermutation() {
        var result = compile("""
                CREATE TABLE t(a INT NOT NULL, b INT ARRAY);
                INSERT INTO t(b, a) VALUES (ARRAY[20], 10), (NULL, 30);
                REMOVE FROM t(b, a) VALUES (ARRAY[20], 10);
                """, false);
        var rows = result.compiler().getTableContents().getTableContents(new ProgramIdentifier("t"));
        var weights = new java.util.HashMap<Integer, Long>();
        for (var entry: rows.data.entrySet()) {
            var row = entry.getKey().to(DBSPTupleExpression.class);
            int key = row.get(0).to(DBSPI32Literal.class).value;
            weights.merge(key, entry.getValue(), Long::sum);
            var array = row.get(1).to(org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression.class);
            if (key == 30) Assert.assertNull(array.data);
            else {
                Assert.assertEquals(10, key); Assert.assertNotNull(array.data); Assert.assertEquals(1, array.data.size());
                var element = array.data.get(0);
                if (element instanceof org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression cast) element = cast.source;
                Assert.assertEquals(Integer.valueOf(20), element.to(DBSPI32Literal.class).value);
            }
        }
        Assert.assertEquals(java.util.Map.of(10, 0L, 30, 1L), weights);
    }

}
