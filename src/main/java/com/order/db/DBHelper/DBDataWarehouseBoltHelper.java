package com.order.db.DBHelper;

import com.order.db.JDBCUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * Created by LiMingji on 2015/6/4.
 */
public class DBDataWarehouseBoltHelper {
    private static Logger log = Logger.getLogger(DBDataWarehouseBoltHelper.class);
    private transient Connection conn = null;

    //Key: data|channelCode|context|contextType|ruleID
    private HashMap<String, Integer> totalFee = null;
    private HashMap<String, Integer> abnormalFee = null;

    private Connection getConn() throws SQLException {
        if (conn == null) {
            log.info("Connection is null!");
            conn = (new JDBCUtil()).getConnection();
        }
        return conn;
    }

    public DBDataWarehouseBoltHelper() {
        totalFee = new HashMap<String, Integer>();
        abnormalFee = new HashMap<String, Integer>();
        try {
            conn = (new JDBCUtil()).getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void updateData(Long time, String channelCode, String context, String contextType,
                           String provinceId, String rules, int realInfoFee) {

    }
}

















