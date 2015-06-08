package com.order.db.DBHelper;

import com.order.db.JDBCUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * Created by LiMingji on 2015/6/4.
 */
public class DBRealTimeOutputBoltHelper {
    private static Logger log = Logger.getLogger(DBRealTimeOutputBoltHelper.class);
    private transient Connection conn = null;

    //Key: data|channelCode|context|contextType|ruleID
    private HashMap<String, Integer> totalFee = null;
    private HashMap<String, Integer> abnormalFee = null;

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
            conn = (new JDBCUtil()).getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * orderType = 21 为自定义类型。
     */
    public void updateData(String time, String channelCode, String contextId, String contextType,
                           String provinceId, String productId, String rules,
                           int realInfoFee, int orderType, String bookId) {

        if (orderType == 1 || orderType == 2 || orderType == 21 || orderType == 3) {
            contextType = 3+"";
            contextId = bookId;
        }else if (orderType == 4) {
            contextType = 1 + "";
            contextId = productId;
        }else if (orderType == 5) {
            contextType = 2 + "";
            contextId = productId;
        }
        String totalFeeKey = time + "|" + channelCode + "|" + contextId + "|" + contextType;
        String abnormalFeeKey = totalFeeKey + "|" + rules;
        if (totalFee.containsKey(totalFeeKey)) {
            int currentFee = totalFee.get(totalFeeKey) + realInfoFee;
            this.totalFee.put(totalFeeKey, currentFee);
        } else {
            this.totalFee.put(totalFeeKey, realInfoFee);
        }

        if (abnormalFee.containsKey(abnormalFeeKey)) {
            int currentAbnormalFee = abnormalFee.get(abnormalFeeKey) + realInfoFee;
            this.abnormalFee.put(abnormalFeeKey, currentAbnormalFee);
        } else {
            this.abnormalFee.put(abnormalFeeKey, realInfoFee);
        }
    }
}