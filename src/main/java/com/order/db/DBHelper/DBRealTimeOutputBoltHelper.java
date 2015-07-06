package com.order.db.DBHelper;

import com.order.constant.Rules;
import com.order.db.JDBCUtil;
import com.order.util.StormConf;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by LiMingji on 2015/6/4.
 */
public class DBRealTimeOutputBoltHelper implements Serializable {
    private static final long serialVersionUID = 1L;

    private static Logger log = Logger.getLogger(DBRealTimeOutputBoltHelper.class);
    private transient Connection conn = null;

    //Key: date|provinceId|channelCode|content|contextType|
    public static ConcurrentHashMap<String, Double> abnormalFee = null;
    //Key: date|provinceId|channelCode|content|contextType|ruleID
    public static ConcurrentHashMap<String, Double> totalFee = null;
    // 每次重启时，先从数据库中查询所有总费用到内存中。此bolt中需要哪些在将其复制到totalfee中
    public static ConcurrentHashMap<String, Double> totalFeeInDB = null;

    private Connection getConn() throws SQLException {
        if (conn == null) {
            log.info("Connection is null!");
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
        }
        return conn;
    }

    private transient Thread storageData2DBTimer = null;

    public DBRealTimeOutputBoltHelper() {
        if (storageData2DBTimer == null) {
            storageData2DBTimer = new DBTimer(conn);
            storageData2DBTimer.setDaemon(true);
            storageData2DBTimer.start();
        }
        totalFee = new ConcurrentHashMap<String, Double>();
        abnormalFee = new ConcurrentHashMap<String, Double>();
        totalFeeInDB = new ConcurrentHashMap<String, Double>();
        try {
            getAllTotalFeeFromDB();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            conn = this.getConn();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * orderType = 21 为自定义类型。
     * ps说明：
     * ordertype = 4 内容id里填写产品id，内容类型填包月
     * ordertype = 5 内容id里填写产品id，内容类型填促销包
     * ordertype 非4、5 内容id里填写图书id，内容类型填图书
     * 可以建立一个内容类型维表，1 包月 2 促销包 3 图书
     */
    public void updateData(String msisdn, Long time, String channelCode, String contentId, String contentType,
                           String provinceId, String productId, String rules,
                           double realInfoFee, int orderType, String bookId) {
        if (storageData2DBTimer == null) {
            storageData2DBTimer = new DBTimer(conn);
            storageData2DBTimer.setDaemon(true);
            storageData2DBTimer.start();
        }
        String currentTime = TimeParaser.formatTimeInDay(time);
        if (orderType == 1 || orderType == 2 || orderType == 21 || orderType == 3) {  //图书
            contentType = 3 + "";
            contentId = bookId;
        } else if (orderType == 4) {  //包月
            contentType = 1 + "";
            contentId = productId;
        } else if (orderType == 5) {  //促销包
            contentType = 2 + "";
            contentId = productId;
        }
        // 总费用key值
        String totalFeeKey = currentTime + "|" + provinceId + "|" + channelCode + "|"
                + contentId + "|" + contentType;
        int ruleId = getRuleNumFromString(rules);

        // 获取总费用旧值
        double oldTotalFee = 0;
        if (totalFee.containsKey(totalFeeKey)) {
            oldTotalFee = totalFee.get(totalFeeKey);
        } else {
            if (totalFeeInDB.contains(totalFeeKey)) {
                oldTotalFee = totalFeeInDB.get(totalFeeKey);
            }
            this.totalFee.put(totalFeeKey, oldTotalFee);
        }
        //else {
        //	try {
        //		oldTotalFee = getTotalFeeFromDB(currentTime, provinceId, channelCode, contentId, contentType);
        //	} catch (SQLException e) {
        //		e.printStackTrace();
        //	}
        //	this.totalFee.put(totalFeeKey, oldTotalFee);
        //}

        // 统计正常费用
        double curTotalFee = 0;
        if (ruleId == 0) {
            curTotalFee = oldTotalFee + realInfoFee;
            this.totalFee.put(totalFeeKey, curTotalFee);
            //log.info(this.toString());
            return;
        }

        // 统计异常费用
        String abnormalFeeKey = totalFeeKey + "|" + ruleId;
        if (abnormalFee.containsKey(abnormalFeeKey)) {
            double currentAbnormalFee = abnormalFee.get(abnormalFeeKey) + realInfoFee;
            this.abnormalFee.put(abnormalFeeKey, currentAbnormalFee);
        } else {
            this.abnormalFee.put(abnormalFeeKey, realInfoFee);
        }
        //log.info(this.toString());
        return;
    }

    private double getTotalFeeFromDB(String currentTime, String provinceId, String channelCode,
                                     String contentId, String contentType) throws SQLException {
        String selectSql = "SELECT ODR_FEE FROM " + StormConf.realTimeOutputTable
                + " WHERE RECORD_DAY=? AND PROVINCE_ID=? AND SALE_PARM=? "
                + " AND CONTENT_ID=? AND CONTENT_TYPE=? AND RULE_ID=0 ";
        ResultSet rs = null;
        PreparedStatement prepStmt = null;
        double totalFee = 0;
        Connection conn = null;
        try {
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            prepStmt = conn.prepareStatement(selectSql);
            prepStmt.setString(1, currentTime);
            prepStmt.setString(2, provinceId);
            prepStmt.setString(3, channelCode);
            prepStmt.setString(4, contentId);
            prepStmt.setString(5, contentType);
            rs = prepStmt.executeQuery();
            while (rs.next()) {
                totalFee = rs.getFloat("ODR_FEE");
            }
            rs.close();
            prepStmt.close();
            //log.info("Get fee " + String.valueOf(totalFee));
            return totalFee;
        } catch (SQLException e) {
            log.error("查询sql错误" + selectSql);
            e.printStackTrace();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (prepStmt != null) {
                prepStmt.close();
            }
            if (conn != null) {
                conn.close();
                conn = null;
            }
        }
        return totalFee;
    }

    private void getAllTotalFeeFromDB() throws SQLException {
        String selectSql = "SELECT PROVINCE_ID,SALE_PARM,CONTENT_ID,CONTENT_TYPE," +
        		"max(ODR_ABN_FEE) ODR_ABN_FEE,max(ODR_FEE) ODR_FEE " +
        		"FROM " + StormConf.realTimeOutputTable + " WHERE RECORD_DAY=? " +
        		"group by PROVINCE_ID, SALE_PARM, CONTENT_ID, CONTENT_TYPE";
        ResultSet rs = null;
        PreparedStatement prepStmt = null;
        try {
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            prepStmt = conn.prepareStatement(selectSql);
            String date = TimeParaser.formatTimeInDay(System.currentTimeMillis());
            prepStmt.setString(1, date);

            rs = prepStmt.executeQuery();
            while (rs.next()) {
                String provinceId = rs.getString("PROVINCE_ID");
                String channelCode = rs.getString("SALE_PARM");
                String content = rs.getString("CONTENT_ID");
                String contextType = rs.getString("CONTENT_TYPE");
                double abFee = rs.getFloat("ODR_ABN_FEE");
                double totalFee = rs.getFloat("ODR_FEE");
                // 防止数据库中异常费用比总费用大
                if (abFee > totalFee) {
                	totalFee = abFee;
                }
                String totalFeeKey = date + "|" + provinceId + "|" + channelCode + "|"
                        + content + "|" + contextType;
                this.totalFeeInDB.put(totalFeeKey, totalFee);
            }
            rs.close();
            prepStmt.close();
            log.info("Init totalFee map size is " + String.valueOf(this.totalFeeInDB.size()) + "!");
        } catch (SQLException e) {
            log.error("查询sql错误" + selectSql);
            e.printStackTrace();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (prepStmt != null) {
                prepStmt.close();
            }
            if (conn != null) {
                conn.close();
                conn = null;
            }
        }
        return;
    }


    /* 获取异常规则对应的数字编号 */
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

    public String toString() {
        String result = "\n size of totalFee is " + String.valueOf(totalFee.size()) + "\n";
        // 遍历
        Iterator<Map.Entry<String, Double>> it = totalFee.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Double> entry = it.next();
            result += entry.getKey() + " " + String.valueOf(entry.getValue()) + "\n";
        }

        result += "\n size of ABFee is " + String.valueOf(abnormalFee.size()) + "\n";
        Iterator<Map.Entry<String, Double>> it2 = abnormalFee.entrySet().iterator();
        while (it2.hasNext()) {
            Map.Entry<String, Double> entry2 = it2.next();
            result += entry2.getKey() + " " + String.valueOf(entry2.getValue()) + "\n";
        }

        return result;
    }
}
