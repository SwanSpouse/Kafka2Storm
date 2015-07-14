package com.order.db.RedisBoltDBHelper;

/**
 * Created by LiMingji on 15/7/13.
 */

import com.order.constant.Rules;
import com.order.db.DBHelper.DBTimer;
import com.order.db.JDBCUtil;
import com.order.db.RedisBoltDBHelper.DBRedisHelper.DBTotalFeeRedisHelper;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by LiMingji on 2015/6/4.
 */
public class DBRealTimeOutputBoltRedisHelper implements Serializable {
    private static final long serialVersionUID = 1L;

    private DBTotalFeeRedisHelper redisHelper = null;

    private static Logger log = Logger.getLogger(DBRealTimeOutputBoltRedisHelper.class);
    private transient Connection conn = null;

    //Key: date|provinceId|channelCode|content|contextType|
    public ConcurrentHashMap<String, Double> abnormalFee = null;
    //Key: date|provinceId|channelCode|content|contextType|ruleID
    public ConcurrentHashMap<String, Double> totalFee = null;
    // 定时清除
    private transient DBTimer storageData2DBTimer = null;
    private transient Object LOCK = null;
    public Object getLOCK() {
        return LOCK;
    }
    public void setLOCK(Object lOCK) {
        LOCK = lOCK;
    }


    private Connection getConn() throws SQLException {
        if (conn == null) {
            log.info("Connection is null!");
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
        }
        return conn;
    }


    public DBRealTimeOutputBoltRedisHelper(DBTotalFeeRedisHelper redisHelper) {
        if (storageData2DBTimer == null) {
            storageData2DBTimer = new DBTimer(this);
            storageData2DBTimer.setDaemon(true);
            storageData2DBTimer.start();
        }
        if (LOCK == null)
            LOCK = new Object();
        synchronized (LOCK) {
            totalFee = new ConcurrentHashMap<String, Double>();
            abnormalFee = new ConcurrentHashMap<String, Double>();
        }

        //从redis获取总费用和异常费用。
        this.redisHelper = redisHelper;

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
    public void insert2Cache(Long time, String provinceId, String channelCode, String contentId,
                             String contentType, String rules, double realInfoFee) {
        if (LOCK == null)
            LOCK = new Object();
        synchronized (LOCK) {
            if (storageData2DBTimer == null) {
                storageData2DBTimer = new DBTimer(this);
                storageData2DBTimer.setDaemon(true);
                storageData2DBTimer.start();
            }
            String currentTime = TimeParaser.formatTimeInDay(time);

            // 总费用key值
            String totalFeeKey = currentTime + "|" + provinceId + "|"
                    + channelCode + "|" + contentId + "|" + contentType;

            totalFee.put(totalFeeKey, redisHelper.getTotalFeeFromRedis(time, provinceId, channelCode,
                    contentId, contentType, realInfoFee));

            int ruleId = getRuleNumFromString(rules);
            String abnFeeKey = totalFeeKey + "|" + ruleId;
            abnormalFee.put(abnFeeKey, redisHelper.getAbnFeeFromRedis(time, provinceId, channelCode,
                                                         contentId, contentType, realInfoFee, ruleId+""));
        }
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
}

