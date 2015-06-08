package com.order.db.DBHelper;

import com.order.constant.Rules;
import com.order.db.JDBCUtil;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.*;

/**
 * 用于实现RealTimeOutputBolt中的数据库操作。
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
 *
 * Created by LiMingji on 2015/6/4.
 */
public class DBRealTimeOutputBoltHelper implements Serializable {
    private static Logger log = Logger.getLogger(DBStatisticBoltHelper.class);
    private transient Connection conn = null;

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

    public void updateData(String msisdn, String sessionId, String channelCode,
                           String reacordTime, int realInfoFee, String rule) {
        int ruleNum = getRuleNumFromString(rule);
        if (checkExists(msisdn, sessionId, channelCode)) {
            update(msisdn, sessionId, channelCode, ruleNum);
        } else {
            insert(msisdn, sessionId, channelCode, reacordTime, realInfoFee);
        }
    }

    private boolean checkExists(String msisdn, String sessionId, String channelCode) {
        String queryTimesSql = "SELECT COUNT(*) recordTimes FROM ods_iread.RESULT_TABLE " +
                "WHERE \"msisdn\"=? AND \"sessionid\"=? AND \"channelcode\"=?";
        try {
            PreparedStatement stmt = conn.prepareStatement(queryTimesSql);
            stmt.setString(1, msisdn);
            stmt.setString(2, sessionId);
            stmt.setString(3, channelCode);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            int count = rs.getInt("recordTimes");
            return count != 0;
        } catch (SQLException e) {
            log.error("查询sql错误" + queryTimesSql);
        }
        return false;
    }

    private void insert(String msisdn, String sessionId, String channelCode,
                        String reacordTime, int realInfoFee) {
        String sql =
                "INSERT INTO ods_iread.RESULT_TABLE " +
                        "VALUES (?,?,?,?,?," +
                        "?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            PreparedStatement prepStmt = conn.prepareStatement(sql);
            prepStmt.setString(1, reacordTime);
            prepStmt.setString(2, msisdn);
            prepStmt.setString(3, sessionId);
            prepStmt.setString(4, channelCode);
            prepStmt.setInt(5, realInfoFee);
            for (int i = 6; i <= 17; i++) {
                prepStmt.setString(i, 0 + "");
            }
            prepStmt.execute();
        } catch (SQLException e) {
            System.out.println(sql);
            log.error("插入sql错误: " + sql);
        }
    }

    private void update(String msisdn, String sessionId, String channelCode, int rules) {
        String sql = "UPDATE ods_iread.RESULT_TABLE SET \"rule_" + rules + "\"=1 " +
                "WHERE \"msisdn\"=? AND \"sessionid\"=? AND \"channelcode\"=?";
        try {
            PreparedStatement prepStmt = conn.prepareStatement(sql);
            prepStmt.setString(1, msisdn);
            prepStmt.setString(2, sessionId);
            prepStmt.setString(3, channelCode);
            prepStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(sql);
            log.error("更新sql错误: " + sql);
        }
    }

    private int getRuleNumFromString(String rule) {
        if (rule.equals(Rules.ONE.name())) {
            return 1;
        } else if (rule.equals(Rules.TWO.name())) {
            return 2;
        } else if (rule.equals(Rules.THREE.name())) {
            return 3;
        } else if (rule.equals(Rules.FOUR.name())) {
            return 4;
        } else if (rule.equals(Rules.FIVE.name())) {
            return 5;
        } else if (rule.equals(Rules.SIX.name())) {
            return 6;
        } else if (rule.equals(Rules.SEVEN.name())) {
            return 7;
        } else if (rule.equals(Rules.EIGHT.name())) {
            return 8;
        } else if (rule.equals(Rules.NINE.name())) {
            return 9;
        } else if (rule.equals(Rules.TEN.name())) {
            return 10;
        } else if (rule.equals(Rules.ELEVEN.name())) {
            return 11;
        } else if (rule.equals(Rules.TWELVE.name())) {
            return 12;
        }
        return 0;
    }
}