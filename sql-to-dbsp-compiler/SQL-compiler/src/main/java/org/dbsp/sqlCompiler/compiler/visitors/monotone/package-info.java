/**
 * Discover monotone expressions starting from LATENESS annotations
 * and optimize the circuit by inserting garbage-collection operators.
 */

@ParametersAreNonnullByDefault
@FieldsAreNonnullByDefault
@MethodsAreNonnullByDefault
package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.util.FieldsAreNonnullByDefault;
import org.dbsp.util.MethodsAreNonnullByDefault;

import javax.annotation.ParametersAreNonnullByDefault;
