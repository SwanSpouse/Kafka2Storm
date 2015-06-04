package com.order.db.DBHelper;

import com.order.db.JDBCUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

/**
 * Created by LiMingji on 2015/6/4.
 */
public class DBStatisticBoltHelper {
    private static Logger log = Logger.getLogger(DBStatisticBoltHelper.class);
    private Connection conn = null;

    private final Object LOCK = new Object();

    private static HashMap<String, String> parameterId2SecChannelId = null;
    private static HashMap<String, String> parameterId2ChannelIds = null;

    private Connection getConn() throws SQLException {
        if (conn == null) {
            log.info("Connection is null!");
            conn = (new JDBCUtil()).getConnection();
        }
        return conn;
    }

    public DBStatisticBoltHelper() {
        try {
            conn = getConn();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        synchronized (LOCK) {
            parameterId2SecChannelId = new HashMap<String, String>();
            parameterId2ChannelIds = new HashMap<String, String>();
        }
    }

    /**
     * 获取营销参数 二级渠道维表
     */
    public void getData() {
        synchronized (LOCK) {
            this.parameterId2SecChannelId.clear();
            this.parameterId2ChannelIds.clear();
        }
//        String sql = "select FIRST_CHANNEL_ID,SECOND_CHANNEL_ID,THIRD_CHANNEL_ID,PARAMETER_ID" +
//                " from dim.dim_drp_sale_param";
        String sql = "select FIRST_CHANNEL_ID,SECOND_CHANNEL_ID,THIRD_CHANNEL_ID,PARAMETER_ID" +
                " from ods_iread.dim_drp_sale_param";
        try {
            if (conn == null) {
                conn = getConn();
            }
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                String firstChannelId = resultSet.getString("FIRST_CHANNEL_ID");
                String secondChannelId = resultSet.getString("SECOND_CHANNEL_ID");
                String thirdChannelId = resultSet.getString("THIRD_CHANNEL_ID");
                String parameterId = resultSet.getString("PARAMETER_ID");
                synchronized (LOCK) {
                    this.parameterId2SecChannelId.put(parameterId, secondChannelId);
                    this.parameterId2ChannelIds.put(parameterId, firstChannelId + "|" + secondChannelId + "|" + thirdChannelId);
                }
                if (!(parameterId2ChannelIds.size() == 0 || parameterId2SecChannelId.size() == 0)) {
                    log.info("营销参数二级渠道维表已经加载");
                }
            }
        } catch (SQLException e) {
            log.error(sql + ":insert data to DB is failed.");
            e.printStackTrace();
        }
    }

    public static HashMap<String, String> getParameterId2SecChannelId() {
        return parameterId2SecChannelId;
    }

    public static HashMap<String, String> getParameterId2ChannelIds() {
        return parameterId2ChannelIds;
    }
}