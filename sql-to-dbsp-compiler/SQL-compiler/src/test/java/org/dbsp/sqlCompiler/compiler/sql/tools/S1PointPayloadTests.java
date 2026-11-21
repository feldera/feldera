package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

import java.io.IOException;

public class S1PointPayloadTests {
    @Test
    public void malformedPointDimensionsAndBitFieldsFailBeforeAllocation() throws IOException {
        var factory = S1LiteralPayloadTests.factory();
        var builder = new RexBuilder(factory);
        var literal = builder.makeLiteral(new GeometryFactory().createPoint(new Coordinate(1, 2)),
                factory.createSqlType(SqlTypeName.GEOMETRY));
        var original = S1PointPayload.encode(literal);
        for (int dimension: new int[]{0, 1, 5, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
            var changed = original.deepCopy();
            changed.put("dimension", dimension);
            Assert.assertThrows(IOException.class, () -> S1PointPayload.decode(changed));
        }
        var wrongCount = original.deepCopy();
        wrongCount.withArray("ordinates").remove(0);
        Assert.assertThrows(IOException.class, () -> S1PointPayload.decode(wrongCount));
        var badBits = original.deepCopy();
        badBits.withArray("ordinates").set(0, org.dbsp.util.Utilities.deterministicObjectMapper().getNodeFactory().numberNode(-1));
        Assert.assertThrows(IOException.class, () -> S1PointPayload.decode(badBits));
        var extra = original.deepCopy();
        extra.put("extra", true);
        Assert.assertThrows(IOException.class, () -> S1PointPayload.decode(extra));
    }

    @Test
    public void unsupportedGeometryMetadataAndShapesFailAtExport() {
        var factory = S1LiteralPayloadTests.factory();
        var builder = new RexBuilder(factory);
        var type = factory.createSqlType(SqlTypeName.GEOMETRY);
        var point = new GeometryFactory().createPoint(new Coordinate(1, 2));
        point.setUserData("not a SQL literal field");
        Assert.assertThrows(IOException.class, () -> S1PointPayload.encode(builder.makeLiteral(point, type)));
        var fixed = new GeometryFactory(new PrecisionModel(10)).createPoint(new Coordinate(1, 2));
        Assert.assertThrows(IOException.class, () -> S1PointPayload.encode(builder.makeLiteral(fixed, type)));
        var line = new GeometryFactory().createLineString(new Coordinate[]{new Coordinate(1, 2), new Coordinate(3, 4)});
        Assert.assertThrows(IOException.class, () -> S1PointPayload.encode(builder.makeLiteral(line, type)));
    }
}
