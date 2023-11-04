package org.dbsp.sqllogictest.executors;

import org.dbsp.util.TableValue;

import java.sql.SQLException;

public interface InputGenerator {
    TableValue[] getInputs() throws SQLException;
}
