package com.order.db;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;

public class DB implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    transient Connection conn;
    private static Logger log = Logger.getLogger(DB.class);

    public DB() {
    }

    private Connection getConn() throws SQLException {
        if (conn == null) {
            log.info("Connection is null!");
            conn = (new JDBCUtil()).getConnection();
        }
        return conn;
    }
}
