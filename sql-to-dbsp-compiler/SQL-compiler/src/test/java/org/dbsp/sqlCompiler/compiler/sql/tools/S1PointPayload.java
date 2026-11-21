package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rex.RexLiteral;
import org.dbsp.util.Utilities;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

/** Exact coordinate-sequence payload for the Point subset used by lowering.
 * Arbitrary geometry factories, user data and non-Point shapes are not imported. */
final class S1PointPayload {
    private S1PointPayload() { }

    static ObjectNode encode(RexLiteral literal) throws IOException {
        if (!(literal.getValueAs(org.locationtech.jts.geom.Geometry.class) instanceof Point point))
            throw new IOException("Only Point geometry literals are supported");
        if (point.getUserData() != null || point.getPrecisionModel().getType() != PrecisionModel.FLOATING)
            throw new IOException("Unsupported geometry user data or precision model");
        var sequence = point.getCoordinateSequence();
        int dimension = sequence.getDimension();
        int measures = sequence.getMeasures();
        dimensions(dimension, measures, sequence.size() == 0 ? 0 : dimension);
        ObjectNode result = Utilities.deterministicObjectMapper().createObjectNode();
        result.put("kind", "geometry_point");
        result.put("srid", point.getSRID());
        result.put("dimension", dimension);
        result.put("measures", measures);
        var ordinates = result.putArray("ordinates");
        if (sequence.size() != 0) {
            for (int ordinate = 0; ordinate < dimension; ordinate++)
                ordinates.add(new BigInteger(Long.toUnsignedString(Double.doubleToRawLongBits(sequence.getOrdinate(0, ordinate)))));
        }
        return result;
    }

    private static void dimensions(int dimension, int measures, int ordinates) throws IOException {
        if (measures < 0 || measures > 1 || dimension - measures < 2 || dimension - measures > 3
                || (ordinates != 0 && ordinates != dimension))
            throw new IOException("Invalid point coordinate dimensions");
    }

    private static int integer(JsonNode payload, String field) throws IOException {
        JsonNode value = payload.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt())
            throw new IOException("Expected signed integer point field: " + field);
        return value.intValue();
    }

    static Point decode(JsonNode payload) throws IOException {
        Set<String> fields = new HashSet<>();
        payload.fieldNames().forEachRemaining(fields::add);
        if (!payload.isObject() || !fields.equals(Set.of("kind", "srid", "dimension", "measures", "ordinates"))
                || !payload.path("kind").isTextual() || !payload.path("kind").textValue().equals("geometry_point"))
            throw new IOException("Unexpected point payload fields or kind");
        int srid = integer(payload, "srid");
        int dimension = integer(payload, "dimension");
        int measures = integer(payload, "measures");
        JsonNode ordinates = payload.get("ordinates");
        if (!ordinates.isArray())
            throw new IOException("Point requires an ordinate bit array");
        dimensions(dimension, measures, ordinates.size());
        var sequence = PackedCoordinateSequenceFactory.DOUBLE_FACTORY.create(ordinates.isEmpty() ? 0 : 1, dimension, measures);
        for (int ordinate = 0; ordinate < ordinates.size(); ordinate++) {
            JsonNode bits = ordinates.get(ordinate);
            if (!bits.isIntegralNumber() || bits.bigIntegerValue().signum() < 0 || bits.bigIntegerValue().bitLength() > 64)
                throw new IOException("Point ordinate requires unsigned 64-bit IEEE bits");
            sequence.setOrdinate(0, ordinate, Double.longBitsToDouble(bits.bigIntegerValue().longValue()));
        }
        return new GeometryFactory(new PrecisionModel(), srid, PackedCoordinateSequenceFactory.DOUBLE_FACTORY).createPoint(sequence);
    }
}
