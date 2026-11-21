package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class S1RelationMetadataTests {
    @Test public void multipleTraitsAndHintPositionsRoundTripExactly() throws IOException {
        var first = RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING,
                RelFieldCollation.NullDirection.FIRST));
        var second = RelCollations.of(1);
        var traits = RelTraitSet.createEmpty().plus(Convention.NONE).plus(first)
                .replace(RelCollationTraitDef.INSTANCE, List.of(first, second)).plus(RelDistributions.ANY)
                .replace(RelDistributionTraitDef.INSTANCE, List.of(RelDistributions.hash(List.of(0, 0, 1)), RelDistributions.range(List.of(1, 0))));
        var hint = RelHint.builder("hint").position(new SqlParserPos(2, 3, 2, 9).withQuoting(true))
                .inheritPath(0, 2).hintOption("\uD800\uDC00", "second").hintOption("\uE000", "first").build();
        var wire = S1RelationMetadata.encode(traits, List.of(hint));
        var decoded = S1RelationMetadata.decode(wire, 2);
        Assert.assertEquals(traits, decoded.traits());
        Assert.assertTrue(hint.identical(decoded.hints().get(0)));
        Assert.assertEquals("\uE000", wire.at("/hints/0/options/0/key").asText());
        Assert.assertThrows(IOException.class, () -> S1RelationMetadata.decode(wire, 1));
        ((com.fasterxml.jackson.databind.node.ObjectNode) wire.at("/traits/1/values/1/0")).put("field", 0);
        Assert.assertThrows(IOException.class, () -> S1RelationMetadata.decode(wire, 2));
    }

    @Test public void unknownTraitsAndExtraHintFieldsFail() throws IOException {
        var wire = S1RelationMetadata.encode(RelTraitSet.createEmpty().plus(Convention.NONE),
                List.of(RelHint.builder("hint").hintOption("x").build()));
        var unknown = wire.deepCopy();
        ((com.fasterxml.jackson.databind.node.ObjectNode) unknown.at("/traits/0")).put("kind", "unknown");
        Assert.assertThrows(IOException.class, () -> S1RelationMetadata.decode(unknown, 1));
        ((com.fasterxml.jackson.databind.node.ObjectNode) wire.at("/hints/0")).put("extra", true);
        Assert.assertThrows(IOException.class, () -> S1RelationMetadata.decode(wire, 1));
    }
}
