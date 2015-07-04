package com.order.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.io.Serializable;

import oracle.jdbc.pool.OracleDataSource;
import org.apache.log4j.Logger;

public class JDBCUtil implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String CACHE_NAME = "MYCACHE";
    private static OracleDataSource ods = null;
    static Logger log = Logger.getLogger(JDBCUtil.class);

    public static final JDBCUtil connUtil = new JDBCUtil();

    private JDBCUtil() {
        initConn();
    }

    private void initConn() {
        log.info("OracleDataSource Initialization");
        try {
            ods = new OracleDataSource();
            ods.setURL(DBConstant.DBURL);
            ods.setUser(DBConstant.DBUSER);
            ods.setPassword(DBConstant.DBPASSWORD);
            ods.setConnectionCacheName(CACHE_NAME);
            Properties cacheProps = new Properties();
            cacheProps.setProperty("MinLimit", "1");
            cacheProps.setProperty("MaxLimit", "30000");
            cacheProps.setProperty("InitialLimit", "100");
            cacheProps.setProperty("ConnectionWaitTimeout", "3");
            cacheProps.setProperty("ValidateConnection", "true");
            ods.setConnectionProperties(cacheProps);
        } catch (SQLException e) {
            log.error("JDBCUtil has an Exception");
        }
    }

    public Connection getConnection() throws SQLException {
        return getConnection("env.unspecified");
    }

    public Connection getConnection(String env) throws SQLException {
        //for security
        if (connUtil == null || ods == null) {
            initConn();
        }
        return ods.getConnection();
    }
}
