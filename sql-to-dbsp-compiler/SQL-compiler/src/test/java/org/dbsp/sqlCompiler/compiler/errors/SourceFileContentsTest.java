package org.dbsp.sqlCompiler.compiler.errors;

import org.junit.Assert;
import org.junit.Test;

public class SourceFileContentsTest {
    @Test
    public void testShortLineFragment() {
        SourceFileContents sources = new SourceFileContents();
        sources.append("SELECT a, b, c FROM t;");
        // 'b' is at line 1, column 11 to 11
        SourcePositionRange range = new SourcePositionRange(
                new SourcePosition(1, 11),
                new SourcePosition(1, 11)
        );
        String fragment = sources.getFragment(range, true);
        Assert.assertTrue(fragment.contains("SELECT a, b, c FROM t;"));
        Assert.assertTrue(fragment.contains("^"));
        Assert.assertFalse(fragment.contains("..."));
    }

    @Test
    public void testLongLineSnippetIsBounded() {
        SourceFileContents sources = new SourceFileContents();
        // Construct a single line of over 20,000 characters
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE t(");
        for (int i = 0; i < 2000; i++) {
            if (i > 0) sb.append(", ");
            sb.append("col").append(i).append(" INT");
        }
        sb.append(");");
        String longLine = sb.toString();
        Assert.assertTrue(longLine.length() > 20000);
        sources.append(longLine);

        // Find the position of col1000 in the middle of the line
        int idx = longLine.indexOf("col1000");
        Assert.assertTrue(idx > 5000);
        int startCol = idx + 1; // 1-based
        int endCol = idx + "col1000".length();

        SourcePositionRange range = new SourcePositionRange(
                new SourcePosition(1, startCol),
                new SourcePosition(1, endCol)
        );

        String fragment = sources.getFragment(range, true);
        // Snippet length must be bounded (e.g. < 500 characters), not the full 20,000+ chars
        Assert.assertTrue("Snippet too long: " + fragment.length(), fragment.length() < 500);
        Assert.assertTrue(fragment.contains("..."));
        Assert.assertTrue(fragment.contains("col1000"));
        Assert.assertTrue(fragment.contains("^^^^^^^"));
    }

    @Test
    public void testLongAdjacentLinesAreBounded() {
        SourceFileContents sources = new SourceFileContents();
        // 3 lines, each 10,000 characters
        String line1 = "SELECT " + "a".repeat(10000) + ";";
        String line2 = "SELECT " + "b".repeat(10000) + ";";
        String line3 = "SELECT " + "c".repeat(10000) + ";";
        sources.append(line1 + "\n" + line2 + "\n" + line3);

        // Error in middle line at column 5000
        SourcePositionRange range = new SourcePositionRange(
                new SourcePosition(2, 5000),
                new SourcePosition(2, 5005)
        );
        String fragment = sources.getFragment(range, true);
        // Each line in snippet should be bounded
        Assert.assertTrue("Snippet too long: " + fragment.length(), fragment.length() < 1000);
        Assert.assertTrue(fragment.contains("..."));
    }

    @Test
    public void testUndecoratedFragmentReturnsExactSubstring() {
        SourceFileContents sources = new SourceFileContents();
        String code = "SELECT " + "x".repeat(1000) + " AS result;";
        sources.append(code);
        int idx = code.indexOf("AS");
        SourcePositionRange range = new SourcePositionRange(
                new SourcePosition(1, idx + 1),
                new SourcePosition(1, idx + 2)
        );
        String fragment = sources.getFragment(range, false);
        Assert.assertEquals("AS" + SourceFileContents.newline(), fragment);
    }
}
