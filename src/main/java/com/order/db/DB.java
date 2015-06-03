package com.order.db;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

    /**
     * 获取营销参数 二级渠道维表
     */
    public void getData() {
        String sql = "select FIRST_CHANNEL_ID,SECOND_CHANNEL_ID,THIRD_CHANNEL_ID,PARAMETER_ID" +
                " from dim.dim_drp_sale_param";
        try {
            conn = getConn();
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                String firstChannelId = resultSet.getString("FIRST_CHANNEL_ID");
                String secondChannelId = resultSet.getString("SECOND_CHANNEL_ID");
                String thirdChannelId = resultSet.getString("THIRD_CHANNEL_ID");
                String parameterId = resultSet.getString("PARAMETER_ID");
            }
        } catch (SQLException e) {
            log.error(sql + ":insert data to DB is failed.");
            e.printStackTrace();
        }
    }
}
