package com.order.db.DBHelper;

import com.order.bolt.StatisticsBolt;
import com.order.db.JDBCUtil;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

/**
 * Created by LiMingji on 2015/6/4.
 */
public class DBRealTimeOutputBoltHelper {
    private static Logger log = Logger.getLogger(DBRealTimeOutputBoltHelper.class);
    public static final String TABLE_NAME = "ods_iread.ABN_CTID_CTTP_PARM_PRV_D";
    private transient Connection conn = null;

    //Key: date|provinceId|channelCode|context|contextType|
    private HashMap<String, Integer> abnormalFee = null;
    //Key: date|provinceId|channelCode|context|contextType|ruleID
    private HashMap<String, Integer> totalFee = null;

    private Connection getConn() throws SQLException {
        if (conn == null) {
            log.info("Connection is null!");
            conn = (new JDBCUtil()).getConnection();
        }
        return conn;
    }

    public DBRealTimeOutputBoltHelper() {
        totalFee = new HashMap<String, Integer>();
        abnormalFee = new HashMap<String, Integer>();
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
     *
     */
    public void updateData(String msisdn, Long time, String channelCode, String contextId, String contextType,
                           String provinceId, String productId, String rules,
                           int realInfoFee, int orderType, String bookId) {
        String currentTime = TimeParaser.formatTimeInDay(time);
        if (orderType == 1 || orderType == 2 || orderType == 21 || orderType == 3) {
            contextType = 3 + "";
            contextId = bookId;
        } else if (orderType == 4) {
            contextType = 1 + "";
            contextId = productId;
        } else if (orderType == 5) {

            contextType = 2 + "";
            contextId = productId;
        }
        //统计正常费用。
        String totalFeeKey = currentTime + "|" + provinceId + "|" + channelCode + "|"
                + contextId + "|" + contextType;
        if (totalFee.containsKey(totalFeeKey)) {
            int currentFee = totalFee.get(totalFeeKey) + realInfoFee;
            this.totalFee.put(totalFeeKey, currentFee);
        } else {
            this.totalFee.put(totalFeeKey, realInfoFee);
        }

        //统计异常费用。
        int ruleId = Integer.parseInt(rules);
        if (ruleId == 0) {
            return;
        } else {
            //先将本次记录登记为异常费用。
            String abnormalFeeKey = totalFeeKey + "|" + rules;
            if (abnormalFee.containsKey(abnormalFeeKey)) {
                int currentAbnormalFee = abnormalFee.get(abnormalFeeKey) + realInfoFee;
                this.abnormalFee.put(abnormalFeeKey, currentAbnormalFee);
            } else {
                this.abnormalFee.put(abnormalFeeKey, realInfoFee);
            }
            //将一小时以内统计正常的记录取出来，更新异常费用，并修改为异常。
            String oneHourAgeTime = TimeParaser.OneHourAge(time);
            String checkAbnormalOrderSql =
                    "SELECT \"channelcode\",\"realfee\" FROM " + this.TABLE_NAME +
                            " WHERE \"record_time\">="+oneHourAgeTime+" AND \"rule_"+rules+"\"=0 " +
                            "AND \"msisdn\"="+msisdn;
            try {
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(checkAbnormalOrderSql);
                if (StatisticsBolt.isDebug) {
                    log.info("追溯查询sql" + checkAbnormalOrderSql);
                }
                //对特定用户特定渠道1小时内的异常费用进行追溯。
                while (rs.next()) {
                    //Key: date|provinceId|channelCode|context|contextType|ruleID
                    String channelCodeHistory = rs.getString("channelCode");
                    int abnormalInfoFee = rs.getInt("realfee");
                    totalFeeKey = currentTime + "|" + provinceId + "|" + channelCodeHistory + "|"
                            + contextId + "|" + contextType;
                    abnormalFeeKey = totalFeeKey + "|" + rules;
                    if (abnormalFee.containsKey(abnormalFeeKey)) {
                        int currentAbnormalFee = abnormalFee.get(abnormalFeeKey) + abnormalInfoFee;
                        this.abnormalFee.put(abnormalFeeKey, currentAbnormalFee);
                    } else {
                        this.abnormalFee.put(abnormalFeeKey, realInfoFee);
                    }
                }
            } catch (SQLException e) {
                log.error("追溯查询sql错误: " + checkAbnormalOrderSql);
                e.printStackTrace();
            }
            String updateOrderSql = "UPDATE "+this.TABLE_NAME+" SET \"rule_"+rules+"\"=1"+
                    " WHERE \"record_time\">="+oneHourAgeTime+" AND \"rule_"+rules+"\"=0 " +
                    "AND \"msisdn\"="+msisdn;
            //将上一个结果查询出来需要追溯的正常订单设置为异常。防止后续重复计算。
            try {
                Statement stmt = conn.createStatement();
                if (StatisticsBolt.isDebug) {
                    log.error("追溯重置sql: " + updateOrderSql);
                }
                stmt.executeUpdate(updateOrderSql);
            } catch (SQLException e) {
                log.error("追溯重置sql错误" + updateOrderSql);
                e.printStackTrace();
            }
        }
    }
}