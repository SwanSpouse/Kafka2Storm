package com.order.db.DBHelper;

import com.order.db.JDBCUtil;
import com.order.util.LogUtil;
import com.order.util.StormConf;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by LiMingji on 2015/6/4.
 */
public class DBStatisticBoltHelper implements Serializable {
    private static final long serialVersionUID = 1L;
    private static Logger log = Logger.getLogger(DBStatisticBoltHelper.class);

    public static ConcurrentHashMap<String, String> parameterId2SecChannelId = new ConcurrentHashMap<String, String>();
    public static ConcurrentHashMap<String, String> parameterId2ChannelIds = new ConcurrentHashMap<String, String>();

    /**
     * 获取营销参数 二级渠道维表
     */
    public static void getData() throws SQLException{
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
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                //营销参数均应该大写。
                String firstChannelId = resultSet.getString("FIRST_CHANNEL_ID");
                firstChannelId = firstChannelId == null ? null : firstChannelId.toUpperCase();

                String secondChannelId = resultSet.getString("SECOND_CHANNEL_ID");
                secondChannelId = secondChannelId == null ? null : secondChannelId.toUpperCase();

                String thirdChannelId = resultSet.getString("THIRD_CHANNEL_ID");
                thirdChannelId = thirdChannelId == null ? null : thirdChannelId.toUpperCase();

                String parameterId = resultSet.getString("PARAMETER_ID");
                parameterId = parameterId == null ? null : parameterId.toUpperCase();

                parameterId2SecChannelId.put(parameterId, secondChannelId);
                parameterId2ChannelIds.put(parameterId, firstChannelId + "|" + secondChannelId + "|" + thirdChannelId);
            }
        } catch (SQLException e) {
            log.error(sql + ":insert data to DB is failed.");
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
                conn = null;
            }
        }
    }
}