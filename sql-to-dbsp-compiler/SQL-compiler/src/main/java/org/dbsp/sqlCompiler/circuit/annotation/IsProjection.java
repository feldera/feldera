package org.dbsp.sqlCompiler.circuit.annotation;

import org.dbsp.sqlCompiler.compiler.visitors.outer.UnusedFields;

/** Annotation used on a Map which projects fields from the input.
 * These are created by the {@link UnusedFields} analysis.
 * This annotation prevents such maps from being themselves optimized,
 * since this would lead to an infinite loop. */
public class IsProjection extends Annotation {}
