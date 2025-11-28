package org.dbsp.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

/** Test various utilities functions */
public class TestUtilities {
    @Test
    public void testExpandBraces() {
        String path = "a{b,cd,e}hi{j,k,l}z";
        List<String> expansion = Utilities.expandBraces(path);
        Assert.assertEquals(9, expansion.size());
        Assert.assertEquals("[abhijz, acdhijz, aehijz, abhikz, acdhikz, aehikz, abhilz, acdhilz, aehilz]",
                expansion.toString());
    }

    @Test
    public void testShuffle() {
        Shuffle id = new IdShuffle(4);
        final var data = List.of("a", "b", "c", "d");
        final var d0 = id.shuffle(data);
        Assert.assertEquals(d0, data);

        final var id0 = id.after(id);
        final var d1 = id0.shuffle(data);
        Assert.assertEquals(d1, data);

        final Shuffle rev = new ExplicitShuffle(4, List.of(3, 2, 1, 0));
        final var r0 = rev.shuffle(data);
        Assert.assertEquals(List.of("d", "c", "b", "a"), r0);

        final Shuffle rev2 = rev.after(rev);
        final var d2 = rev2.shuffle(data);
        Assert.assertEquals(d0, d2);

        final Shuffle odd = new ExplicitShuffle(4, List.of(1, 3));
        final var od = odd.shuffle(data);
        Assert.assertEquals(List.of("b", "d"), od);

        final Shuffle repl = new ExplicitShuffle(4, List.of(0, 0, 0));
        final var three = repl.shuffle(data);
        Assert.assertEquals(List.of("a", "a", "a"), three);

        final var compressed = odd.compress(Set.of(0, 2));
        Assert.assertEquals("[0, 1]", compressed.toString());

        final var compressed0 = odd.compress(Set.of(0));
        Assert.assertEquals("[0, 2]", compressed0.toString());

        final var twoOne = new ExplicitShuffle(3, List.of(2, 1));
        final var compressed1 = twoOne.compress(Set.of());
        Assert.assertEquals(twoOne.toString(), compressed1.toString());

        final var threeZero = new ExplicitShuffle(4, List.of(3, 0));
        final var compressed2 = threeZero.compress(Set.of(2));
        Assert.assertEquals("[2, 0]", compressed2.toString());
    }
}
