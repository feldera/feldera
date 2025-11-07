package org.dbsp.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

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
}
