package com.order.db.DBHelper;

import com.order.constant.Rules;
import com.order.db.JDBCUtil;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
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
    public ConcurrentHashMap<String, Double> abnormalFee = null;
    //Key: date|provinceId|channelCode|content|contextType|ruleID
    public ConcurrentHashMap<String, Double> totalFee = null;
    // 每次重启时，先从数据库中查询所有总费用到内存中。此bolt中需要哪些在将其复制到totalfee中
    public ConcurrentHashMap<String, Double> totalFeeInDB = null;
    // 定时清除
    //private DBTimer storageData2DBTimer = null;
    private transient Thread storageData2DBTimer = null;
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

	public DBRealTimeOutputBoltHelper() {
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
			totalFeeInDB = new ConcurrentHashMap<String, Double>();
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
    public void updateData(String msisdn, Long time, String channelCode, String provinceId,
                           String rules, double realInfoFee, String contentId, String contentType) {
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
			int ruleId = getRuleNumFromString(rules);

			// 获取总费用旧值
			double oldTotalFee = 0;
			if (totalFee.containsKey(totalFeeKey)) {
				oldTotalFee = totalFee.get(totalFeeKey);
			} else {
				if (totalFeeInDB.containsKey(totalFeeKey)) {
					oldTotalFee = totalFeeInDB.get(totalFeeKey);
				}
				totalFee.put(totalFeeKey, oldTotalFee);
			}

			// 统计总费用
			if (ruleId == 0) {
				double curTotalFee = 0;
				curTotalFee = oldTotalFee + realInfoFee;
				totalFee.put(totalFeeKey, curTotalFee);
				// log.info(this.toString());
				return;
			}

			// 统计异常费用
			double currentAbnormalFee;
			String abnormalFeeKey = totalFeeKey + "|" + String.valueOf(ruleId);
			if (abnormalFee.containsKey(abnormalFeeKey)) {
				currentAbnormalFee = abnormalFee.get(abnormalFeeKey)
						+ realInfoFee;
				abnormalFee.put(abnormalFeeKey, currentAbnormalFee);
			} else {
				currentAbnormalFee = realInfoFee;
				abnormalFee.put(abnormalFeeKey, currentAbnormalFee);
			}

			// 为防止异常费用大于总费用做异常处理，当异常费用大于总费用时，讲总费用相应增大
			// if (currentAbnormalFee > oldTotalFee){
			// totalFee.put(totalFeeKey, currentAbnormalFee);
			// }

			return;
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
