/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.util.Utilities;
import org.locationtech.jts.geom.Point;

/** Reimplementation of RelJsonWriter from Calcite.
 * Feldera changes expose RelIdMap and retain point literals in diagnostic plans. */
public class RelJsonWriter implements RelWriter {
    protected final JsonBuilder jsonBuilder;
    private final Map<RelNode, Integer> relIdMap;
    protected final RelJson relJson;
    protected final List<@Nullable Object> relList;
    private final List<Pair<String, @Nullable Object>> values;
    public final int verbosity;

    /** Calcite's generic value serializer lacks the Point values produced by
     * folding ST_POINT. This is diagnostic-plan JSON, not the S1 replay format.
     * Preserve all ordinates and SRID rather than relying on a 2D WKT display. */
    private static final class PointRelJson extends RelJson {
        private final JsonBuilder builder;

        @SuppressWarnings("deprecation") // Calcite exposes no other subclass constructor.
        PointRelJson(JsonBuilder builder) {
            super(builder);
            this.builder = builder;
        }

        @Override public @Nullable Object toJson(@Nullable Object value) {
            if (!(value instanceof Point point))
                return super.toJson(value);
            var sequence = point.getCoordinateSequence();
            Map<String, Object> result = this.builder.map();
            result.put("geometry_type", "Point");
            result.put("srid", point.getSRID());
            result.put("dimension", sequence.getDimension());
            result.put("measures", sequence.getMeasures());
            List<Object> coordinates = this.builder.list();
            for (int index = 0; index < sequence.size(); index++) {
                List<Object> ordinates = this.builder.list();
                for (int ordinate = 0; ordinate < sequence.getDimension(); ordinate++) {
                    double coordinate = sequence.getOrdinate(index, ordinate);
                    // JSON has no non-finite numbers; use Calcite's convention.
                    ordinates.add(Double.isFinite(coordinate) ? coordinate : Double.toString(coordinate));
                }
                coordinates.add(ordinates);
            }
            result.put("coordinates", coordinates);
            return result;
        }
    }

    public static RelJsonWriter forReplay(Map<RelNode, Integer> relIdMap, int verbosity) {
        JsonBuilder builder = new JsonBuilder();
        return new RelJsonWriter(builder, ignored -> new PointRelJson(builder), relIdMap, verbosity);
    }

    public RelJsonWriter(Map<RelNode, Integer> relIdMap, int verbosity) {
        this(new JsonBuilder(), relIdMap, verbosity);
    }

    public RelJsonWriter(JsonBuilder jsonBuilder, Map<RelNode, Integer> relIdMap, int verbosity) {
        this(jsonBuilder, UnaryOperator.identity(), relIdMap, verbosity);
    }

    public RelJsonWriter(JsonBuilder jsonBuilder,
                         UnaryOperator<RelJson> relJsonTransform,
                         Map<RelNode, Integer> relIdMap,
                         int verbosity) {
        this.values = new ArrayList<>();
        this.jsonBuilder = Objects.requireNonNull(jsonBuilder, "jsonBuilder");
        this.relList = this.jsonBuilder.list();
        this.relJson = relJsonTransform.apply(RelJson.create().withJsonBuilder(jsonBuilder));
        this.relIdMap = relIdMap;
        this.verbosity = verbosity;
    }

    final Set<String> highVerbosity = Set.of("exprs", "aggs", "condition", "tuples");

    /** Attributes that the dataflow graph does not describe, and that
     * {@link RelJson} cannot serialize. */
    static final Set<String> skipped = Set.of("hints");

    protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
        Map<String, Object> map = this.jsonBuilder.map();
        map.put("id", null);
        map.put("relOp", this.relJson.classToTypeName(rel.getClass()));

        for(Pair<String, Object> value : values) {
            if (skipped.contains(value.left))
                continue;
            if (this.verbosity < 1 && highVerbosity.contains(value.left))
                continue;
            if (!(value.right instanceof RelNode)) {
                this.put(map, value.left, value.right);
            }
        }

        List<Object> list = this.explainInputs(rel.getInputs());
        map.put("inputs", list);

        Integer id = this.relIdMap.size();
        this.relIdMap.put(rel, id);
        map.put("id", id);
        this.relList.add(map);
    }

    private void put(Map<String, @Nullable Object> map, String name, @Nullable Object value) {
        map.put(name, this.relJson.toJson(value));
    }

    private List<Object> explainInputs(List<RelNode> inputs) {
        List<Object> list = this.jsonBuilder.list();

        for(RelNode input : inputs) {
            Integer id = this.relIdMap.get(input);
            if (id == null) {
                input.explain(this);
            }
            list.add(Utilities.getExists(this.relIdMap, input));
        }
        return list;
    }

    @Override
    public final void explain(RelNode rel, List<Pair<String, @Nullable Object>> valueList) {
        this.explain_(rel, valueList);
    }

    @Override
    public SqlExplainLevel getDetailLevel() {
        return SqlExplainLevel.ALL_ATTRIBUTES;
    }

    @Override
    public RelWriter item(String term, @Nullable Object value) {
        this.values.add(Pair.of(term, value));
        return this;
    }

    @Override
    public RelWriter done(RelNode node) {
        List<Pair<String, Object>> valuesCopy = ImmutableList.copyOf(this.values);
        this.values.clear();
        this.explain_(node, valuesCopy);
        return this;
    }

    @Override
    public boolean nest() {
        return true;
    }

    public String asString() {
        Map<String, Object> map = this.jsonBuilder.map();
        map.put("rels", this.relList);
        return this.jsonBuilder.toJsonString(map);
    }
}
