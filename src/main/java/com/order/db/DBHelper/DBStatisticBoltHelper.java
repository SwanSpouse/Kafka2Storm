package com.order.db.DBHelper;

import com.order.db.JDBCUtil;
import com.order.util.LogUtil;
import com.order.util.StormConf;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.*;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by LiMingji on 2015/6/4.
 */
public class DBStatisticBoltHelper implements Serializable {
    private static final long serialVersionUID = 1L;
    private static Logger log = Logger.getLogger(DBStatisticBoltHelper.class);
    private static transient Connection conn = null;

    public static ConcurrentHashMap<String, String> parameterId2SecChannelId = null;
    public static ConcurrentHashMap<String, String> parameterId2ChannelIds = null;

    private static Connection getConn() throws SQLException {
        if (conn == null) {
            log.info("Connection is null!");
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
        }
        return conn;
    }

    public DBStatisticBoltHelper() {
        try {
            conn = getConn();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        parameterId2SecChannelId = new ConcurrentHashMap<String, String>();
        parameterId2ChannelIds = new ConcurrentHashMap<String, String>();
    }

    /**
     * 获取营销参数 二级渠道维表
     */
    public static void getData() {
        LogUtil.printLog("加载二维渠道维表" + new Date());
        if (parameterId2ChannelIds == null) {
            parameterId2ChannelIds = new ConcurrentHashMap<String, String>();
        } else {
            parameterId2ChannelIds.clear();
        }
        if (parameterId2SecChannelId == null) {
            parameterId2SecChannelId = new ConcurrentHashMap<String, String>();
        } else {
            parameterId2SecChannelId.clear();
        }
        String sql = "SELECT FIRST_CHANNEL_ID,SECOND_CHANNEL_ID,THIRD_CHANNEL_ID,PARAMETER_ID" +
                " FROM " + StormConf.channelCodesTable;
        try {
            Connection conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                String firstChannelId = resultSet.getString("FIRST_CHANNEL_ID");
                String secondChannelId = resultSet.getString("SECOND_CHANNEL_ID");
                String thirdChannelId = resultSet.getString("THIRD_CHANNEL_ID");
                String parameterId = resultSet.getString("PARAMETER_ID");
                parameterId2SecChannelId.put(parameterId, secondChannelId);
                parameterId2ChannelIds.put(parameterId, firstChannelId + "|" + secondChannelId + "|" + thirdChannelId);
            }
            resultSet.close();
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            log.error(sql + ":insert data to DB is failed.");
            e.printStackTrace();
        }
    }
}