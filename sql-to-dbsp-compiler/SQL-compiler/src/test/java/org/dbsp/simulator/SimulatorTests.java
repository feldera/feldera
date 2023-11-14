package org.dbsp.simulator;

import org.dbsp.simulator.types.IntegerWeight;
import org.dbsp.simulator.types.StringSqlType;
import org.dbsp.simulator.values.IntegerSqlValue;
import org.dbsp.simulator.values.SqlTuple;
import org.dbsp.simulator.values.StringSqlValue;
import org.junit.Assert;
import org.junit.Test;

public class SimulatorTests {
    @Test
    public void zsetTests() {
        ZSet<SqlTuple, Integer> zero = new ZSet<SqlTuple, Integer>(IntegerWeight.INSTANCE);
        Assert.assertEquals(0, zero.entryCount());
        ZSet<SqlTuple, Integer> some = new ZSet<SqlTuple, Integer>(IntegerWeight.INSTANCE);
        SqlTuple tuple = new SqlTuple()
                .add(new IntegerSqlValue(10))
                .add(new StringSqlValue("string", new StringSqlType()));

        some.append(tuple, 2);
        Assert.assertEquals("{\n[10, 'string'] => 2\n}", some.toString());
        ZSet<SqlTuple, Integer> dbl = some.add(some);
        Assert.assertEquals("{\n[10, 'string'] => 4\n}", dbl.toString());
        ZSet<SqlTuple, Integer> neg = dbl.negate();
        Assert.assertEquals("{\n[10, 'string'] => -4\n}", neg.toString());
        ZSet<SqlTuple, Integer> z = dbl.add(neg);
        Assert.assertTrue(z.isEmpty());
        ZSet<SqlTuple, Integer> one = dbl.distinct();
        Assert.assertEquals("{\n[10, 'string'] => 1\n}", one.toString());
        ZSet<SqlTuple, Integer> four = dbl.positive(false);
        Assert.assertEquals("{\n[10, 'string'] => 4\n}", four.toString());
        ZSet<SqlTuple, Integer> none = neg.positive(false);
        Assert.assertTrue(none.isEmpty());
    }
}
