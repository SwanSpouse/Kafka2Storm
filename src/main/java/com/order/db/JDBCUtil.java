package com.order.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.io.Serializable;

import oracle.jdbc.pool.OracleDataSource;
import org.apache.log4j.Logger;

public class JDBCUtil implements Serializable {
    private static final long serialVersionUID = 1L;
    private final static String CACHE_NAME = "MYCACHE";
    private static OracleDataSource ods = null;
    static Logger log = Logger.getLogger(JDBCUtil.class);

    //单例
    private JDBCUtil(){}

    public static Connection getConnection() throws SQLException {
        if (ods == null) {
            ods = new OracleDataSource();
            log.info("OracleDataSource Initialization");
            try {
                ods = new OracleDataSource();
                ods.setURL(DBConstant.DBURL);
                ods.setUser(DBConstant.DBUSER);
                ods.setPassword(DBConstant.DBPASSWORD);
                ods.setConnectionCacheName(CACHE_NAME);
                Properties cacheProps = new Properties();
                cacheProps.setProperty("MinLimit", "1");
                cacheProps.setProperty("MaxLimit", "1000");
                cacheProps.setProperty("InitialLimit", "5");
                cacheProps.setProperty("ConnectionWaitTimeout", "3");
                cacheProps.setProperty("ValidateConnection", "true");
                ods.setConnectionProperties(cacheProps);
            } catch (SQLException e) {
                log.error("JDBCUtil has an Exception");
            }
            log.error("OracleDataSource is null.This is an Exception.");
        }
        return ods.getConnection();
    }
}
