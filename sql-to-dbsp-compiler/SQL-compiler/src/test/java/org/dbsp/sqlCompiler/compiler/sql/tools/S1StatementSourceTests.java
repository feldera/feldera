package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.sql.FelderaS1StatementSource;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateIndexStatement;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class S1StatementSourceTests {
    @Test public void sourceTextAndDiagnosticRenderingRemainDistinctWithoutParsing() throws IOException {
        var position = new SqlParserPos(3, 5, 8, 13).withQuoting(true);
        var original = new ParsedStatement(new FelderaS1StatementSource(position, SqlKind.CREATE_INDEX,
                "opaque statement text: λ\nnot parseable SQL", "diagnostic spelling: \"MiXeD\""), false);
        var restored = S1StatementSource.decode(S1StatementSource.encode(original));
        Assert.assertEquals(original.toString(), restored.toString());
        Assert.assertEquals(CalciteObject.create(original).toString(), CalciteObject.create(restored).toString());
        Assert.assertEquals(position, restored.statement().getParserPosition());
        Assert.assertTrue(restored.statement().getParserPosition().isQuoted());
        Assert.assertFalse(restored.visible());
        Assert.assertThrows(UnsupportedOperationException.class, () -> restored.statement().validate(null, null));
        Assert.assertThrows(UnsupportedOperationException.class, () -> restored.statement().accept(null));
        var index = new CreateIndexStatement(restored, new ProgramIdentifier("ix"), new ProgramIdentifier("v"),
                List.of(new ProgramIdentifier("MiXeD", true)), List.of(position), position, position);
        Assert.assertEquals(new ProgramIdentifier("MiXeD", true), index.columns.get(0));
        Assert.assertEquals(position, index.columnParserPosition(0));
        Assert.assertEquals(original.toString(), index.getStatement());
    }

    @Test public void malformedSourceRecordsAreRejected() {
        var source = new ParsedStatement(new FelderaS1StatementSource(SqlParserPos.ZERO, SqlKind.CREATE_VIEW, "s", "d"), true);
        var wire = S1StatementSource.encode(source);
        wire.put("kind", "java.lang.Runtime");
        Assert.assertThrows(IOException.class, () -> S1StatementSource.decode(wire));
        wire.put("kind", "CREATE_VIEW").put("visible", "true");
        Assert.assertThrows(IOException.class, () -> S1StatementSource.decode(wire));
        wire.put("visible", true).put("unmodeled", true);
        Assert.assertThrows(IOException.class, () -> S1StatementSource.decode(wire));
        wire.remove("unmodeled"); wire.remove("diagnostic_text");
        Assert.assertThrows(IOException.class, () -> S1StatementSource.decode(wire));
    }
    @Test public void internalPositionsRemainHiddenFromDiagnostics() throws IOException {
        for (boolean internal: new boolean[]{true, false}) {
            var position = new org.dbsp.sqlCompiler.compiler.frontend.ExtendedSqlParserPos(new SqlParserPos(4, 2, 4, 8), internal);
            var original = new ParsedStatement(new FelderaS1StatementSource(position, SqlKind.CREATE_VIEW, "s", "d"), true);
            var restored = S1StatementSource.decode(S1StatementSource.encode(original));
            Assert.assertEquals(CalciteObject.create(original).getPositionRange(), CalciteObject.create(restored).getPositionRange());
            Assert.assertEquals(internal, ((org.dbsp.sqlCompiler.compiler.frontend.ExtendedSqlParserPos) restored.statement().getParserPosition()).internal);
        }
    }

}
