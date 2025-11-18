package org.dbsp.util;

import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.ServerAcl;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/** Manages a DB server in a separate process */
public class HSQDBManager {
    final Server server;
    @Nullable
    Connection dbConn = null;

    public HSQDBManager(String path) throws ServerAcl.AclFormatException, IOException {
        HsqlProperties props = new HsqlProperties();
        props.setProperty("server.database.0", "file:" + path + "/mydb;");
        props.setProperty("server.dbname.0", "xdb");
        this.server = new org.hsqldb.Server();
        // We don't want to see the output from the hsqldb server
        this.server.setLogWriter(new PrintWriter(OutputStream.nullOutputStream()));
        this.server.setProperties(props);
    }

    public void start() {
        this.server.start();
    }

    public void stop() throws SQLException, ClassNotFoundException {
        try (Statement s = this.getConnection().createStatement()) {
            s.execute("SHUTDOWN");
        }
        this.server.shutdown();
    }

    public String getConnectionString() {
        return "jdbc:hsqldb:hsql://localhost/xdb";
    }

    public Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.hsqldb.jdbcDriver");
        this.dbConn = DriverManager.getConnection(this.getConnectionString(), "SA", "");
        return this.dbConn;
    }
}
