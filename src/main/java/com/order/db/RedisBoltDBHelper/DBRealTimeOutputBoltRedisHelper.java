package com.order.db.RedisBoltDBHelper;

/**
 * Created by LiMingji on 15/7/13.
 */

import com.order.bolt.Bean.RealTimeOutputDBItem;
import com.order.constant.Rules;
import com.order.db.DBHelper.DBTimer;
import com.order.db.JDBCUtil;
import com.order.db.RedisBoltDBHelper.DBRedisHelper.DBTotalFeeRedisHelper;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by LiMingji on 2015/6/4.
 */
public class DBRealTimeOutputBoltRedisHelper implements Serializable {
    private static final long serialVersionUID = 1L;

    private DBTotalFeeRedisHelper redisHelper = null;

    private static Logger log = Logger.getLogger(DBRealTimeOutputBoltRedisHelper.class);
    private transient Connection conn = null;

    //Key: date|provinceId|channelCode|content|contextType|
    public ConcurrentLinkedQueue<RealTimeOutputDBItem> dbItems;
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

        //从redis获取总费用和异常费用。
        this.redisHelper = redisHelper;
        dbItems = new ConcurrentLinkedQueue<RealTimeOutputDBItem>();
        try {
            conn = this.getConn();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *  将数据插入缓冲区。这里面的rules就是数字1 2 3 4这样的。
     */
    public void insert2Cache(Long time, String provinceId, String channelCode, String contentId,
                             String contentType, String rules, double realInfoFee) {
        if (LOCK == null)
            LOCK = new Object();

        if (storageData2DBTimer == null) {
            storageData2DBTimer = new DBTimer(this);
            storageData2DBTimer.setDaemon(true);
            storageData2DBTimer.start();
        }
        String currentTime = TimeParaser.formatTimeInDay(time);

        //一个Item对应数据库里的一条记录。
        RealTimeOutputDBItem item = new RealTimeOutputDBItem();
        item.setRecordTime(currentTime).setProvinceId(provinceId).setChannelCode(channelCode)
                .setContentId(contentId).setContentType(contentType).setRule(rules).setRealInfoFee(realInfoFee);

        dbItems.add(item);
    }
}

