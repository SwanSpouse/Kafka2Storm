package com.order.db.DBHelper;

import com.order.db.JDBCUtil;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.*;

/**
 * 用于实现RealTimeOutputBolt中的数据库操作。
 * <p/>
 * * 输出表结构:
 * CREATE TABLE "AAS"."RESULT_TABLE"
 * (
 * "record_time"   varchar2(8 byte),
 * "msisdn"        varchar2(32 byte),
 * "sessionid"     varchar2(40 byte),
 * "channelcode"   varchar2(40 byte),
 * "realfee"       NUMBER,
 * "rule_1"        varchar2(2 byte),
 * "rule_2"        varchar2(2 byte),
 * "rule_3"        varchar2(2 byte),
 * "rule_4"        varchar2(2 byte),
 * "rule_5"        varchar2(2 byte),
 * "rule_6"        varchar2(2 byte),
 * "rule_7"        varchar2(2 byte),
 * "rule_8"        varchar2(2 byte),
 * "rule_9"        varchar2(2 byte),
 * "rule_10"       varchar2(2 byte),
 * "rule_11"       varchar2(2 byte),
 * "rule_12"       varchar2(2 byte)
 * )
 * <p/>
 * Created by LiMingji on 2015/6/4.
 */
public class DBRealTimeOutputBoltHelper implements Serializable{
    private static Logger log = Logger.getLogger(DBStatisticBoltHelper.class);
    private Connection conn = null;

    private Connection getConn() throws SQLException {
        if (conn == null) {
            log.info("Connection is null!");
            conn = (new JDBCUtil()).getConnection();
        }
        return conn;
    }

    public DBRealTimeOutputBoltHelper() {
        try {
            conn = (new JDBCUtil()).getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void upDateData(String msisdn, String sessionId, String channelCode,
                            String reacordTime, int realInfoFee, String rule) {
        String sql = " SELECT COUNT(*) FROM ods_iread.RESULT_TABLE WHERE \"msisdn\"=18001214581 AND \"sessionid\"=1 AND \"channelcode\"=1 ";
        try {
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            resultSet.last();
            int count= resultSet.getRow();
            System.out.println(count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}






















