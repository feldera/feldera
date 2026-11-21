package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class S1OperatorRegistryTests {
    @Test public void everyStaticEntryResolvesToTheExactOperatorObject() throws IOException {
        var registry = new S1OperatorRegistry();
        Assert.assertFalse(registry.entries().isEmpty());
        Assert.assertEquals("SqlStdOperatorTable.PLUS", registry.key(SqlStdOperatorTable.PLUS));
        for (var entry: registry.entries().entrySet()) {
            Assert.assertSame(entry.getValue(), registry.resolve(S1OperatorRegistry.NAME, S1OperatorRegistry.VERSION, entry.getKey()));
            String canonical = registry.key(entry.getValue());
            Assert.assertSame(entry.getValue(), registry.resolve(S1OperatorRegistry.NAME, S1OperatorRegistry.VERSION, canonical));
        }
        Assert.assertThrows(UnsupportedOperationException.class, () -> registry.entries().clear());
    }

    @Test public void displayNamesCannotForgeIdentityAndWireKeysCannotLoadClasses() {
        var registry = new S1OperatorRegistry();
        Assert.assertThrows(IOException.class, () -> registry.key(null));
        Assert.assertThrows(IOException.class, () -> registry.resolve(S1OperatorRegistry.NAME, S1OperatorRegistry.VERSION, null));
        Assert.assertThrows(IOException.class, () -> registry.key(new SqlSpecialOperator("+", SqlKind.PLUS)));
        Assert.assertThrows(IOException.class, () -> registry.resolve(S1OperatorRegistry.NAME, "future", "SqlStdOperatorTable.PLUS"));
        Assert.assertThrows(IOException.class, () -> registry.resolve("unknown", S1OperatorRegistry.VERSION, "SqlStdOperatorTable.PLUS"));
        Assert.assertThrows(IOException.class, () -> registry.resolve(S1OperatorRegistry.NAME, S1OperatorRegistry.VERSION, "java.lang.Runtime.getRuntime"));
    }
}
