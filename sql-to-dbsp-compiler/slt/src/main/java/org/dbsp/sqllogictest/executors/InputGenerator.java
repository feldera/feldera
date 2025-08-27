package org.dbsp.sqllogictest.executors;

import org.dbsp.sqlCompiler.compiler.frontend.TableData;

import java.sql.SQLException;

public interface InputGenerator {
    TableData[] getInputs() throws SQLException;
}
