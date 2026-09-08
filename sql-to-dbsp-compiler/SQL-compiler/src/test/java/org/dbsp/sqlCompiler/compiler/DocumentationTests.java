package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.backend.rust.StubsWriter;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RejectUnsupportedPlans;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.WarnFloatingPointEquality;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FindUnboundedState;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.RewriteNow;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/** Tests for {@link Documentation} */
public class DocumentationTests {
    /** The links that compiler messages cite */
    static final List<Documentation.Link> CITED = List.of(
            WarnFloatingPointEquality.DOCUMENTATION,
            RejectUnsupportedPlans.ROW_DOCUMENTATION,
            SqlToRelCompiler.RECURSION_DOCUMENTATION,
            RewriteNow.NOW_DOCUMENTATION,
            StubsWriter.UDF_DOCUMENTATION,
            FindUnboundedState.DOCUMENTATION);

    @Test
    public void citedLinksResolve() throws IOException {
        for (Documentation.Link link : CITED)
            Documentation.checkResolves(link);
    }

    @Test
    public void citation() {
        Documentation.Link link = new Documentation.Link("sql/comparisons", "comparing-row-values");
        Assert.assertEquals("See https://docs.feldera.com/sql/comparisons#comparing-row-values", link.citation());
        Assert.assertEquals(link, Documentation.Link.parse(link.url()));
        Assert.assertNull(Documentation.Link.parse("https://example.com/page#anchor"));
        Documentation.Link page = new Documentation.Link("sql/recursion");
        Assert.assertEquals("See https://docs.feldera.com/sql/recursion", page.citation());
        Assert.assertEquals(page, Documentation.Link.parse(page.url()));
    }

    /** A heading with an explicit id defines that id and not its slug */
    @Test
    public void anchors() {
        Set<String> anchors = Documentation.anchors("""
                # Comparison Operations
                Text with <a id="eq"></a> and <a id="ne"></a> tags.
                ### Comparing `ROW` values {#comparing-row-values}
                ## Date_parsing and formatting
                not a heading # with a hash""");
        Assert.assertEquals(Set.of("comparison-operations", "eq", "ne",
                "comparing-row-values", "date_parsing-and-formatting"), anchors);
    }

    @Test
    public void missingAnchorIsReported() {
        Documentation.Link link = new Documentation.Link("sql/comparisons", "no-such-heading");
        RuntimeException e = Assert.assertThrows(RuntimeException.class, () -> Documentation.checkResolves(link));
        Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("Anchor `no-such-heading` does not appear in file"));
        Documentation.Link missing = new Documentation.Link("sql/no-such-page", "x");
        e = Assert.assertThrows(RuntimeException.class, () -> Documentation.checkResolves(missing));
        Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("Documentation page not found"));
    }
}
