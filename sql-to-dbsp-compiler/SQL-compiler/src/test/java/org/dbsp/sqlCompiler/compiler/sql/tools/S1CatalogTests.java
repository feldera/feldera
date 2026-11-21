package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.core.TableScan;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CalciteTableDescription;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class S1CatalogTests {
    private static ObjectNode fixture() throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var input = mapper.createObjectNode().put("id", "catalog");
        input.set("options", mapper.valueToTree(new CompilerOptions()));
        input.putArray("submissions").addObject().put("batch", 0).put("many", true).put("sql", """
                CREATE TABLE "Mixed"("Key" INT NOT NULL PRIMARY KEY, value INT DEFAULT 7)
                WITH ('expected_size' = '123');
                CREATE VIEW v AS SELECT * FROM "Mixed";
                """);
        return S1CatalogProbe.capture(input, new S1BuiltinOperatorRegistry());
    }

    @Test public void scanResolvesRealCatalogNameKeysAndStatistics() throws IOException {
        var wire = fixture();
        var decoded = S1RelNodes.decodeCatalog(wire, new S1BuiltinOperatorRegistry());
        var scans = decoded.nodes().stream().filter(node -> node instanceof TableScan).map(node -> (TableScan) node)
                .filter(scan -> scan.getTable().getQualifiedName().contains("Mixed")).toList();
        Assert.assertEquals(1, scans.size());
        var table = scans.get(0).getTable();
        var schema = table.unwrap(CalciteTableDescription.class);
        Assert.assertNotNull(schema); Assert.assertEquals("Mixed", schema.getName().name()); Assert.assertTrue(schema.getName().isQuoted());
        Assert.assertEquals(Double.valueOf(123), schema.getStatistic().getRowCount());
        Assert.assertEquals(List.of(org.apache.calcite.util.ImmutableBitSet.of(0)), schema.getStatistic().getKeys());
        Assert.assertEquals("Key", table.getRowType().getFieldNames().get(0));
    }

    @Test public void scanCannotSubstituteMissingOrIncompatibleCatalogMetadata() throws IOException {
        for (String mutation: List.of("missing", "duplicate", "row", "name")) {
            var wire = fixture();
            switch (mutation) {
                case "missing" -> wire.withObject("/catalog").putArray("scans");
                case "duplicate" -> wire.withArray("/catalog/scans").add(wire.at("/catalog/scans/0").deepCopy());
                case "row" -> ((ObjectNode) wire.at("/catalog/schemas/0/columns/0/name")).put("name", "changed");
                case "name" -> ((ObjectNode) wire.at("/catalog/tables/0")).putArray("name").add("changed");
            }
            Assert.assertThrows(IOException.class, () -> S1RelNodes.decodeCatalog(wire, new S1BuiltinOperatorRegistry()));
        }
    }

    @Test public void catalogColumnIndicesMustMatchTheirPositions() throws IOException {
        for (long index: new long[] {1, 999, 0xffff_ffffL}) {
            var wire = fixture();
            ((ObjectNode) wire.at("/catalog/schemas/0/columns/0")).put("index", index);
            Assert.assertThrows(IOException.class, () -> S1RelNodes.decodeCatalog(wire, new S1BuiltinOperatorRegistry()));
        }
    }
}
