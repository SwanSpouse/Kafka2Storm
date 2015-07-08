package com.order.db.DBHelper;

import com.order.constant.Constant;
import com.order.db.JDBCUtil;
import com.order.util.LogUtil;
import com.order.util.StormConf;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * Created by LiMingji on 2015/6/9.
 */
public class DBTimer extends Thread {
    private static Logger log = Logger.getLogger(DBTimer.class);

    private Connection conn = null;
    private DBRealTimeOutputBoltHelper helper = null;
    private long lastClearDayDataTime = 0;
    private final long dayMillis = 24*60*60*1000;

    public DBTimer(DBRealTimeOutputBoltHelper helper) {
        this.helper = helper;
    }

    @Override
    public void run() {
        super.run();
        try {
        	int num = 0;
        	Thread.sleep((new Random()).nextInt(Constant.ONE_MINUTE * 5 * 1000));  //test
        	long nowtime = System.currentTimeMillis();
        	lastClearDayDataTime = nowtime - nowtime%dayMillis;
            while (true) {
                Thread.sleep(Constant.ONE_MINUTE * 5 * 1000L);  // test
                log.info("===将RealTime缓存中的数据更新到数据库中===");
                //每分钟将map中异常费用数据更新到数据库中。
                this.updateDB();
                //每N分钟将总费用更新到库中
                if (++num >= 3) {
                	this.updateTotalDB();
                	num = 0;
                }
                //if (TimeParaser.isTimeToClearData(System.currentTimeMillis())) {
                if (System.currentTimeMillis() >= (lastClearDayDataTime+dayMillis)) {
                	this.updateDB();
                    this.updateTotalDB();
                    helper.totalFee.clear();
                    helper.totalFeeInDB.clear();
                    lastClearDayDataTime += dayMillis;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException e) {
			e.printStackTrace();
		}
    }
 
    public void updateDB() throws SQLException {
    	// 将异常订购费用更新到库里，并清除内存
        log.info("====DBTimer开始更新异常费用===");
        int insertCnt = 0;
        int updateCnt = 0;
        for (String key : helper.abnormalFee.keySet()) {
            String[] keys = key.split("\\|");
            if (keys.length < 6) {
                log.error("字段错误: " + key);
                continue;
            }
            String date = keys[0];
            String provinceId = keys[1];
            String channelCode = keys[2];
            String contentID = keys[3];
            String contentType = keys[4];
            String ruleID = keys[5];
            String totalFeeKey = date + "|" + provinceId + "|" + channelCode + "|"
                    + contentID + "|" + contentType;
            if (!helper.totalFee.containsKey(totalFeeKey)) {
                LogUtil.printLog("费用列表异常：" + helper.totalFee + " : "
                        + helper.abnormalFee + " : " + totalFeeKey +
                        " " + Arrays.asList(keys));
                continue;
            }
            double fee = helper.totalFee.get(totalFeeKey);
            if (fee == 0) continue;  // 总费用为0,则无需入库
            String abnormalFeeKey = totalFeeKey + "|" + ruleID;
            double abnFee = helper.abnormalFee.get(abnormalFeeKey);
            try {
                if (checkExists(date, provinceId, contentID, contentType, channelCode, ruleID)) {
                   this.updateAbnormalFee(date, provinceId, contentID, contentType, channelCode, ruleID, abnFee, fee);
                   updateCnt++;
                } else {
                    this.insertAbnormalFee(date, provinceId, contentID, contentType, channelCode, ruleID, abnFee, fee);
                    insertCnt++;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        log.info("===DBTimer更新异常统计信息,总共" + String.valueOf(helper.abnormalFee.size())
        		+ "条，其中update" + String.valueOf(updateCnt) + "条， insert " + String.valueOf(insertCnt) + "条");
        helper.abnormalFee.clear();
    }
    
    public void updateTotalDB() throws SQLException {
        log.info("====updateTotalDB开始更新总费用===");
        int insertCnt = 0;
        int updateCnt = 0;
        // 将总费用更新到库中（ruleid为0）,不清空内存
        for (String key : helper.totalFee.keySet()) {
            String[] keys = key.split("\\|");
            if (keys.length != 5) {
                log.error("totalfee的key值字段个数错误: " + key);
                continue;
            }
            String date = keys[0];
            String provinceId = keys[1];
            String channelCode = keys[2];
            String contentID = keys[3];
            String contentType = keys[4];
            String ruleID = "0";
            double fee = helper.totalFee.get(key);
            if (fee == 0) continue;  //总费用为0不入库
            //log.info("Insert Key : " + key);
            if (checkExists(date, provinceId, contentID, contentType, channelCode, ruleID)) {
                //this.updateAbnormalFee(date, provinceId, contentID, contentType, channelCode, ruleID, 0, fee);
                this.updateTotalFee(date, provinceId, contentID, contentType, channelCode, fee);
                updateCnt++;
            } else {
                this.insertAbnormalFee(date, provinceId, contentID, contentType, channelCode, ruleID, 0, fee);
                insertCnt++;
            }
        }
        log.info("===updateTotalDB更新" + String.valueOf(helper.totalFee.size()) + "条总费用统计信息到数据库中===");
        log.info("===updateTotalDB更新异常统计信息,总共" + String.valueOf(helper.totalFee.size())
        		+ "条，其中update" + String.valueOf(updateCnt) + "条， insert " + String.valueOf(insertCnt) + "条");
     }

    private boolean checkExists(String date, String provinceId, String contentID, String contentType,
                                String channelCode, String ruleId) throws SQLException {
        String checkExistsSql = "SELECT COUNT(*) recordTimes FROM "+ StormConf.realTimeOutputTable
                + " WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?" +
                     " AND SALE_PARM=? AND CONTENT_TYPE=? AND RULE_ID=?";
        ResultSet rs = null;
        PreparedStatement prepStmt = null;
        try {
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            prepStmt = conn.prepareStatement(checkExistsSql);
            prepStmt.setString(1, date);
            prepStmt.setString(2, provinceId);
            prepStmt.setString(3, contentID);
            prepStmt.setString(4, channelCode);
            prepStmt.setString(5, contentType);
            prepStmt.setString(6, ruleId);

            rs = prepStmt.executeQuery();
            rs.next();
            int count = rs.getInt("recordTimes");
            rs.close();
            prepStmt.close();
            //log.info("DBTimer 检查数据是否存在 " + String.valueOf(count));
            return count != 0;
        } catch (SQLException e) {
            log.error("查询sql错误" + checkExistsSql);
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
        return false;
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
            chl1 = chl2 = chl3 = "null";
        } else {
            String[] chls = DBStatisticBoltHelper.parameterId2ChannelIds.get(channelCode.toUpperCase()).split("\\|");
            chl1 = chls[0];
            chl2 = chls[1];
            chl3 = chls[2];
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

    private void updateAbnormalFee(String date, String provinceId, String contentID, String contentType,
                            String channelCode, String ruleId, double abnormalFee, double totalFee) throws SQLException {
        PreparedStatement prepStmt = null;
        String updateSql = null;
        try {
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            updateSql = " UPDATE " + StormConf.realTimeOutputTable +
                    " SET ODR_ABN_FEE=ODR_ABN_FEE+?, ODR_FEE=?, " +
                    " ABN_RAT=(ODR_ABN_FEE+?)/?" +
                    " WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?" +
                    " AND SALE_PARM=? AND CONTENT_TYPE=? AND RULE_ID=? ";
            prepStmt = conn.prepareStatement(updateSql);
            prepStmt.setDouble(1, abnormalFee);
            prepStmt.setDouble(2, totalFee);
            prepStmt.setDouble(3, abnormalFee);
            prepStmt.setDouble(4, totalFee);
            prepStmt.setString(5, date);
            prepStmt.setString(6, provinceId);
            prepStmt.setString(7, contentID);
            prepStmt.setString(8, channelCode);
            prepStmt.setString(9, contentType);
            prepStmt.setString(10, ruleId);
            prepStmt.executeUpdate();
            prepStmt.execute("commit");
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
    }
    
    private void updateTotalFee(String date, String provinceId, String contentID, String contentType,
                            String channelCode, double totalFee) throws SQLException {
        PreparedStatement prepStmt = null;
        String updateSql = null;
        try {
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            updateSql = " UPDATE " + StormConf.realTimeOutputTable +
                    " SET ODR_FEE=?, " +
                    " ABN_RAT=ODR_ABN_FEE/?" +
                    " WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?" +
                    " AND SALE_PARM=? AND CONTENT_TYPE=? ";
            prepStmt = conn.prepareStatement(updateSql);
            prepStmt.setDouble(1, totalFee);
            prepStmt.setDouble(2, totalFee);
            prepStmt.setString(3, date);
            prepStmt.setString(4, provinceId);
            prepStmt.setString(5, contentID);
            prepStmt.setString(6, channelCode);
            prepStmt.setString(7, contentType);
            prepStmt.executeUpdate();
            prepStmt.execute("commit");
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
    }
    
    public String toString() {
        String result = "\n size of totalFee is " + String.valueOf(helper.totalFee.size()) + "\n";
        // 遍历
        Iterator<Map.Entry<String, Double>> it = helper.totalFee.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Double> entry = it.next();
            result += entry.getKey() + " " + String.valueOf(entry.getValue()) + "\n";
        }

        result += "\n size of ABFee is " + String.valueOf(helper.abnormalFee.size()) + "\n";
        Iterator<Map.Entry<String, Double>> it2 = helper.abnormalFee.entrySet().iterator();
        while (it2.hasNext()) {
            Map.Entry<String, Double> entry2 = it2.next();
            result += entry2.getKey() + " " + String.valueOf(entry2.getValue()) + "\n";
        }

        return result;
    }
}
