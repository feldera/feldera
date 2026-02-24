package org.dbsp.util;

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

public record ErrorWithPosition(String error, SourcePositionRange range) {}
