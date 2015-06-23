package com.order.db.DBHelper;

import com.order.bolt.StatisticsBolt;
import com.order.db.JDBCUtil;
import com.order.util.LogUtil;
import com.order.util.StormConf;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.*;
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

    private Connection getConn() throws SQLException {
        if (conn == null) {
            log.info("Connection is null!");
            conn = JDBCUtil.getConnection();
        }
        return conn;
    }

    private transient Thread storageData2DBTimer = null;

    public DBRealTimeOutputBoltHelper() {
        totalFee = new ConcurrentHashMap<String, Double>();
        abnormalFee = new ConcurrentHashMap<String, Double>();
        try {
            conn = this.getConn();
            storageData2DBTimer = new DBTimer(conn);
            storageData2DBTimer.setDaemon(true);
            storageData2DBTimer.start();
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
    public void updateDataInMap(String msisdn, Long time, String channelCode, String contentId, String contentType,
                                String provinceId, String productId, String rules,
                                double realInfoFee, int orderType, String bookId) {
        String currentTime = TimeParaser.formatTimeInDay(time);
        if (orderType == 1 || orderType == 2 || orderType == 21 || orderType == 3) {
            contentType = 3 + "";
            contentId = bookId;
        } else if (orderType == 4) {
            contentType = 1 + "";
            contentId = productId;
        } else if (orderType == 5) {
            contentType = 2 + "";
            contentId = productId;
        }
        //统计正常费用。
        String totalFeeKey = currentTime + "|" + provinceId + "|" + channelCode + "|"
                + contentId + "|" + contentType;
        if (totalFee.containsKey(totalFeeKey)) {
            double currentFee = totalFee.get(totalFeeKey) + realInfoFee;
            this.totalFee.put(totalFeeKey, currentFee);
        } else {
            this.totalFee.put(totalFeeKey, realInfoFee);
        }

        //统计异常费用。
        int ruleId = DBDataWarehouseBoltHelper.getRuleNumFromString(rules);
        if (ruleId == 0) {
            return;
        }

        String abnormalFeeKey = totalFeeKey + "|" + ruleId;
        if (abnormalFee.containsKey(abnormalFeeKey)) {
            double currentAbnormalFee = abnormalFee.get(abnormalFeeKey) + realInfoFee;
            this.abnormalFee.put(abnormalFeeKey, currentAbnormalFee);
        } else {
            this.abnormalFee.put(abnormalFeeKey, realInfoFee);
        }
        //追溯: 将一小时以内统计正常的记录取出来，更新异常费用，并修改为异常。
        /**
         * 1 2 3 5 6 7 8 这些规则是向前追溯该渠道下1小时数据。
         4    追溯一天的数据
         9 10 11 追溯自然小时的数据。
         */
        long traceBackTimeLong = 0L;
        if (ruleId == 1 || ruleId == 2 || ruleId == 3 ||
                ruleId == 5 || ruleId == 6 || ruleId == 7 || ruleId == 8) {
            traceBackTimeLong = TimeParaser.OneHourAgo(time);
        } else if (ruleId == 4) {
            traceBackTimeLong = TimeParaser.OneDayAgo(time);
        } else if (ruleId == 9 || ruleId == 10 || ruleId == 11) {
            traceBackTimeLong = TimeParaser.NormalHourAgo(time);
        }

        Date traceBackTime = new Date(traceBackTimeLong);
        String checkAbnormalOrderSql =
                "SELECT \"CHANNELCODE\",\"REALFEE\" FROM " + StormConf.dataWarehouseTable +
                        " WHERE \"RECORDTIME\">= \'" + traceBackTime + "\' AND \"RULE_" + 1 + "\"=0 " +
                        "AND \"MSISDN\"=" + msisdn;
        try {
            if (conn == null) {
                conn = JDBCUtil.getConnection();
            }
            Statement stmt = conn.createStatement();
            String dateFormatSql = "alter session set nls_date_format= 'YYYY-MM-DD' ";
            stmt.execute(dateFormatSql);
            ResultSet rs = stmt.executeQuery(checkAbnormalOrderSql);
            if (StatisticsBolt.isDebug) {
                log.info("追溯查询sql" + checkAbnormalOrderSql);
            }
            //对特定用户特定渠道1小时内的异常费用进行追溯。
            while (rs.next()) {
                //Key: date|provinceId|channelCode|content|contentType|ruleID
                String channelCodeHistory = rs.getString("CHANNELCODE");
                double abnormalInfoFee = rs.getDouble("REALFEE");
                totalFeeKey = currentTime + "|" + provinceId + "|" + channelCodeHistory + "|"
                        + contentId + "|" + contentType;
                abnormalFeeKey = totalFeeKey + "|" + ruleId;
                if (abnormalFee.containsKey(abnormalFeeKey)) {
                    double currentAbnormalFee = abnormalFee.get(abnormalFeeKey) + abnormalInfoFee;
                    this.abnormalFee.put(abnormalFeeKey, currentAbnormalFee);
                } else {
                    this.abnormalFee.put(abnormalFeeKey, realInfoFee);
                }
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            log.error("追溯查询sql错误: " + checkAbnormalOrderSql);
            e.printStackTrace();
        }
        LogUtil.printLog("总费用异常费用为： " + totalFee + " == > " + abnormalFee);
        String updateOrderSql = "UPDATE " + StormConf.dataWarehouseTable + " SET \"RULE_" + ruleId + "\"=1" +
                " WHERE \"RECORDTIME\">=\'" + traceBackTime + "\' AND \"RULE_" + ruleId + "\"=0 " +
                "AND \"MSISDN\"=" + msisdn;
        //将上一个结果查询出来需要追溯的正常订单设置为异常。防止后续重复计算。
        try {
            if (conn == null) {
                conn = JDBCUtil.getConnection();
            }
            String dateFormatSql = "alter session set nls_date_format= 'YYYY-MM-DD' ";
            Statement stmt = conn.createStatement();
            if (StatisticsBolt.isDebug) {
                log.error("追溯重置sql: " + updateOrderSql);
            }
            stmt.execute(dateFormatSql);
            stmt.executeUpdate(updateOrderSql);
            stmt.execute("commit");
            stmt.close();
        } catch (SQLException e) {
            log.error("追溯重置sql错误" + updateOrderSql);
            e.printStackTrace();
        }
    }
}