package com.order.db.DBHelper;

import com.order.constant.Rules;
import com.order.db.JDBCUtil;
import com.order.util.LogUtil;
import com.order.util.StormConf;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.*;

/**
 * 用于实现DataWarehouseBolt中的数据库操作。
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
public class DBDataWarehouseBoltHelper implements Serializable {
    private static final long serialVersionUID = 1L;

    private static Logger log = Logger.getLogger(DBStatisticBoltHelper.class);
    private transient Connection conn = null;

    private Connection getConn() throws SQLException {
        if (conn == null) {
            log.info("Connection is null!");
            conn = (new JDBCUtil()).getConnection();
        }
        return conn;
    }

    public DBDataWarehouseBoltHelper() {
        try {
            conn = this.getConn();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void updateData(String msisdn, String sessionId, String channelCode,
                           Date reacordTime, double realInfoFee, String rule,
                           String bookId, String productId) {
        int ruleNum = getRuleNumFromString(rule);
        if (checkExists(msisdn, sessionId, channelCode, bookId, productId)) {
            LogUtil.printLog("更新数据成功: " + msisdn + " " + sessionId + " " + channelCode + " rules: " + rule);
            update(msisdn, sessionId, channelCode, ruleNum, bookId, productId);
        } else {
            LogUtil.printLog("插入数据成功: " + msisdn + " " + sessionId + " " + channelCode);
            insert(msisdn, sessionId, channelCode, reacordTime, realInfoFee, bookId, productId);
        }
    }

    private boolean checkExists(String msisdn, String sessionId, String channelCode,
                                String bookId, String productId) {
        String queryTimesSql = "SELECT COUNT(*) recordTimes FROM " + StormConf.dataWarehouseTable +
                " WHERE \"MSISDN\"=? AND \"SESSIONID\"=? AND \"CHANNELCODE\"=? " +
                " AND \"BOOKID\"=? AND \"PRODUCTID\"=? ";
        try {
            if (conn == null) {
                conn = getConn();
            }
            PreparedStatement stmt = conn.prepareStatement(queryTimesSql);
            stmt.setString(1, msisdn);
            stmt.setString(2, sessionId);
            stmt.setString(3, channelCode);
            stmt.setString(4, bookId);
            stmt.setString(5, productId);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            int count = rs.getInt("recordTimes");
            LogUtil.printLog("检查数据是否存在: " + msisdn + " " + sessionId + " " + channelCode);

            rs.close();
            stmt.close();
            return count != 0 ? true : false;
        } catch (SQLException e) {
            log.error("查询sql错误" + queryTimesSql);
            e.printStackTrace();
        }
        return false;
    }

    private void insert(String msisdn, String sessionId, String channelCode,
                        Date reacordTime, double realInfoFee,
                        String bookId, String productId) {
        String sql =
                "INSERT INTO " + StormConf.dataWarehouseTable+
                        " VALUES (?,?,?,?, ?,?, ?," +
                        /*后面全是rules*/"?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            if (conn == null) {
                conn = (new JDBCUtil()).getConnection();
            }
            PreparedStatement prepStmt = conn.prepareStatement(sql);
            prepStmt.setDate(1, reacordTime);
            prepStmt.setString(2, msisdn);
            prepStmt.setString(3, sessionId);
            prepStmt.setString(4, channelCode);
            prepStmt.setString(5, bookId);
            prepStmt.setString(6, productId);
            prepStmt.setDouble(7, realInfoFee);
            for (int i = 8; i <= 19; i++) {
                prepStmt.setString(i, 0 + "");
            }
            prepStmt.execute();
            prepStmt.execute("commit");
            prepStmt.close();
        } catch (SQLException e) {
            log.error("插入sql错误: " + sql);
            e.printStackTrace();
        }
    }

    private void update(String msisdn, String sessionId, String channelCode, int rules,
                        String bookId, String productId) {
        if (rules == 0) {
            return;
        }
        String sql = "UPDATE " + StormConf.dataWarehouseTable + " SET \"RULE_" + rules + "\"=1 " +
                "WHERE \"MSISDN\"=? AND \"SESSIONID\"=? AND \"CHANNELCODE\"=? " +
                " AND \"BOOKID\"=? AND \"PRODUCTID\"=? ";
        try {
            if (conn == null) {
                conn = (new JDBCUtil()).getConnection();
            }
            PreparedStatement prepStmt = conn.prepareStatement(sql);
            prepStmt.setString(1, msisdn);
            prepStmt.setString(2, sessionId);
            prepStmt.setString(3, channelCode);
            prepStmt.setString(4, bookId);
            prepStmt.setString(5, productId);

            prepStmt.execute();
            prepStmt.execute("commit");
            prepStmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("更新sql错误: " + sql);
        }
    }

    public static int getRuleNumFromString(String rule) {
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