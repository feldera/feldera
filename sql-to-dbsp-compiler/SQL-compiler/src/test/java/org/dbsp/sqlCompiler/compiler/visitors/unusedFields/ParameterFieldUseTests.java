package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.junit.Assert;
import org.junit.Test;

/** Parameter rewriting must allocate types independently of object identity hashes. */
public class ParameterFieldUseTests {
    @Test
    public void compressedTypesFollowParameterOrder() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.options.ioOptions.writeS3 = "capture.json";
        var integer = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false);
        var bigint = new DBSPTypeInteger(CalciteObject.EMPTY, 64, true, false);
        var type = new DBSPTypeTuple(integer, bigint).ref();
        for (int attempt = 0; attempt < 16; attempt++) {
            var use = new ParameterFieldUse();
            var parameters = new DBSPParameter[4];
            for (int index = 0; index < parameters.length; index++) {
                for (int noise = 0; noise < attempt; noise++)
                    System.identityHashCode(new Object());
                var parameter = new DBSPParameter("p" + index, type);
                parameters[index] = parameter;
            }
            // Analysis unions may discover parameters in a different order from
            // their declarations. Neither that order nor identity hashes may
            // choose the allocation order of replacement types.
            for (int index = 0; index < parameters.length; index++) {
                var parameter = parameters[attempt % 2 == 0 ? index : parameters.length - index - 1];
                var fields = new FieldUseMap(type, false);
                fields.deref().field(1).setUsed();
                use.add(parameter, fields);
            }
            var rewrite = use.getFieldRewriter(compiler, 1);
            long previous = -1;
            for (var parameter: parameters) {
                var replacement = rewrite.newParam.get(parameter);
                var tuple = replacement.getType().deref().to(DBSPTypeTuple.class);
                Assert.assertEquals(1, tuple.size());
                Assert.assertTrue(tuple.getFieldType(0).sameType(bigint));
                Assert.assertTrue("Type allocation changed parameter order on attempt " + attempt,
                        replacement.getType().getId() > previous);
                previous = replacement.getType().getId();
            }
        }
    }
}
