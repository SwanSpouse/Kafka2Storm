package com.order.db.DBHelper;

import com.order.Redis.RedisClient;
import com.order.bolt.Redis.RealTimeOutputDBItem;
import com.order.constant.Constant;
import com.order.db.JDBCUtil;
import com.order.db.RedisBoltDBHelper.DBRealTimeOutputBoltRedisHelper;
import com.order.db.RedisBoltDBHelper.DBRedisHelper.DBTotalFeeRedisHelper;
import com.order.util.StormConf;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by LiMingji on 2015/6/9.
 */
public class DBTimer extends Thread {
    private static Logger log = Logger.getLogger(DBTimer.class);

    private Connection conn = null;
    private DBRealTimeOutputBoltRedisHelper helper = null;
    private DBTotalFeeRedisHelper totalFeeRedisHelper = null;

    // 新增两个MAP用于入库时的内存复制
    private ConcurrentLinkedQueue<RealTimeOutputDBItem> dbItemsTmp;

    //入库间隔为1分钟
    private static final long updateInterval = Constant.ONE_MINUTE * 1000L;
    
    public DBTimer(DBRealTimeOutputBoltRedisHelper helper) {
        this.helper = helper;
        totalFeeRedisHelper = new DBTotalFeeRedisHelper();
    }
    
	@Override
	public void run() {
		super.run();
        try {
            Thread.sleep((new Random())
                    .nextInt(Constant.ONE_MINUTE * 10 * 1000)); // test
            while (true) {
                Thread.sleep(updateInterval); // test
                log.info("===将RealTime缓存中的数据更新到数据库中===");
                // 将map中增量异常费用和增量总费用数据更新到数据库中
                if (helper.getLOCK() == null)
                    helper.setLOCK(new Object());
                synchronized (helper.getLOCK()) {
                    // 将内存中的数据复制过来后，清空原Map使excute正常访问
                    dbItemsTmp.clear();
                    dbItemsTmp.addAll(helper.dbItems);
                    helper.dbItems.clear();
                }
                this.updateCachedData2DB();
                dbItemsTmp.clear();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void updateCachedData2DB() throws SQLException {
        Iterator<RealTimeOutputDBItem> it = dbItemsTmp.iterator();
        while (it.hasNext()) {
            RealTimeOutputDBItem item = it.next();
            String currentTime = item.getRecordTime();
            String provinceId = item.getProvinceId();
            String channelCode = item.getChannelCode();
            String contentId = item.getContentId();
            String contentType = item.getContentType();
            String ruleId = item.getRule();
            Double realInfoFee = item.getRealInfoFee();

            if (totalFeeRedisHelper == null) {
                totalFeeRedisHelper = new DBTotalFeeRedisHelper();
            }
            String totalFeeKey = currentTime + "|" + provinceId + "|"
                    + channelCode + "|" + contentId + "|" + contentType;

            String abnFeeKey = totalFeeKey + "|" + ruleId;

            double totalFee = totalFeeRedisHelper.getTotalFeeFromRedis(totalFeeKey, realInfoFee);
            double abnFee = totalFeeRedisHelper.getAbnFeeFromRedis(abnFeeKey);

            if (updateFee(currentTime, provinceId, contentId, contentType, channelCode, ruleId, abnFee, totalFee) <= 0) {
                insertAbnormalFee(currentTime, provinceId, contentId, contentType, channelCode, ruleId, abnFee, totalFee);
            }
        }
    }
    
    private void insertAbnormalFee(String recordDay,String provinceId, String contentId, String contentType,
                            String channelCode, String ruleId, double abnormalFee, double totalFee) throws SQLException {
        if (DBStatisticBoltHelper.parameterId2ChannelIds == null || DBStatisticBoltHelper.parameterId2ChannelIds.isEmpty()) {
            try {
                DBStatisticBoltHelper.getData();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        String chl1 = null;
        String chl2 = null;
        String chl3 = null;

        if (!DBStatisticBoltHelper.parameterId2ChannelIds.containsKey(channelCode.toUpperCase())) {
            chl1 = chl2 = chl3 = "NULL";
        } else {
            String[] chls = DBStatisticBoltHelper.parameterId2ChannelIds.get(channelCode.toUpperCase()).split("\\|");
            chl1 = chls[0];
            chl1 = chl1 == null ? "NULL" : chl1.toUpperCase();
            chl2 = chls[1];
            chl2 = chl2 == null ? "NULL" : chl2.toUpperCase();
            chl3 = chls[2];
            chl3 = chl3 == null ? "NULL" : chl3.toUpperCase();
        }

        String insertDataSql = "INSERT INTO " + StormConf.realTimeOutputTable +
                "( RECORD_DAY,PROVINCE_ID,CHL1,CHL2,CHL3," +
                "  CONTENT_ID,SALE_PARM,ODR_ABN_FEE,ODR_FEE," +
                "  ABN_RAT,CONTENT_TYPE,RULE_ID) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
        double abnormalFeeRate ;
        if (totalFee != 0) {
            abnormalFeeRate = abnormalFee / totalFee;
        } else {
            abnormalFeeRate = 0;
        }
        PreparedStatement prepStmt = null;
        try {
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            prepStmt = conn.prepareStatement(insertDataSql);
            prepStmt.setString(1, recordDay);
            prepStmt.setString(2, provinceId);
            prepStmt.setString(3, chl1);
            prepStmt.setString(4, chl2);
            prepStmt.setString(5, chl3);
            prepStmt.setString(6, contentId);
            prepStmt.setString(7, channelCode);
            prepStmt.setDouble(8, abnormalFee);
            prepStmt.setDouble(9, totalFee);
            prepStmt.setDouble(10, abnormalFeeRate);
            prepStmt.setString(11, contentType);
            prepStmt.setInt(12, Integer.parseInt(ruleId));
            prepStmt.execute();
            prepStmt.execute("commit");
            //log.info("数据插入成功:" + insertDataSql);
        } catch (SQLException e) {
            log.error("插入sql错误:" + insertDataSql);
            e.printStackTrace();
        } finally {
            if (prepStmt != null) {
                prepStmt.close();
            }
            if (conn != null) {
                conn.close();
                conn = null;
            }
        }
    }

    private int updateFee(String date, String provinceId, String contentID, String contentType,
                            String channelCode, String ruleId, double abnormalFee, double totalFee) throws SQLException {
        PreparedStatement prepStmt = null;
        String updateSql = null;
        try {
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            updateSql = " UPDATE " + StormConf.realTimeOutputTable +
                    " SET ODR_ABN_FEE=?, " +
                    " SET ODR_FEE=?,"+
                    " ABN_RAT=?" +
                    " WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?" +
                    " AND SALE_PARM=? AND CONTENT_TYPE=? AND RULE_ID=? ";
            prepStmt = conn.prepareStatement(updateSql);
            prepStmt.setDouble(1, abnormalFee);
            prepStmt.setDouble(2, totalFee);
            prepStmt.setDouble(3, abnormalFee / totalFee);
            prepStmt.setString(4, date);
            prepStmt.setString(5, provinceId);
            prepStmt.setString(6, contentID);
            prepStmt.setString(7, channelCode);
            prepStmt.setString(8, contentType);
            prepStmt.setString(9, ruleId);
            int count = prepStmt.executeUpdate();
            prepStmt.execute("commit");
            return count;
            //log.info("DBTimer 更新数据成功" + updateSql);
        } catch (SQLException e) {
            log.error("更新sql错误" + updateSql);
            e.printStackTrace();
        }finally {
            if (prepStmt != null) {
                prepStmt.close();
            }
            if (conn != null) {
                conn.close();
                conn = null;
            }
        }
        return 0;
    }
}
