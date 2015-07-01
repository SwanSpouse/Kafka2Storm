package com.order.db.DBHelper;

import com.order.bolt.StatisticsBolt;
import com.order.constant.Constant;
import com.order.db.DBConstant;
import com.order.util.LogUtil;
import com.order.util.StormConf;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by LiMingji on 2015/6/9.
 */
public class DBTimer extends Thread {
    private static Logger log = Logger.getLogger(DBTimer.class);

    private Connection conn = null;

    public DBTimer(Connection conn) {
        this.conn = conn;
    }

    @Override
    public void run() {
        super.run();
        try {
            while (true) {
                Thread.sleep(Constant.ONE_MINUTE * 1000L);
                LogUtil.printLog("===将map中的数据更新到数据库中===");
                //将map中的数据更新到数据库中。
                this.updateDB();
                if (TimeParaser.isTimeToClearData(System.currentTimeMillis())) {
                    DBRealTimeOutputBoltHelper.totalFee.clear();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void updateDB() {
        for (String key : DBRealTimeOutputBoltHelper.abnormalFee.keySet()) {
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
            if (!DBRealTimeOutputBoltHelper.totalFee.containsKey(totalFeeKey)) {
                LogUtil.printLog("费用列表异常：" + DBRealTimeOutputBoltHelper.totalFee + " : "
                        + DBRealTimeOutputBoltHelper.abnormalFee + " : " + totalFeeKey +
                        " " + Arrays.asList(keys));
                continue;
            }
            double fee = DBRealTimeOutputBoltHelper.totalFee.get(totalFeeKey);
            String abnormalFeeKey = totalFeeKey + "|" + ruleID;
            double abnFee = DBRealTimeOutputBoltHelper.abnormalFee.get(abnormalFeeKey);
            try {
                if (checkExists(date, provinceId, contentID, contentType, channelCode, ruleID)) {
                    this.updateDate(date, provinceId, contentID, contentType, channelCode, ruleID, abnFee, fee);
                } else {
                    this.insertData(date, provinceId, contentID, contentType, channelCode, ruleID, abnFee, fee);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        DBRealTimeOutputBoltHelper.abnormalFee.clear();
    }

    private boolean checkExists(String date, String provinceId, String contentID, String contentType,
                                String channelCode, String ruleId) throws SQLException {
        String checkExistsSql = "SELECT COUNT(*) recordTimes FROM "+ StormConf.realTimeOutputTable
                + " WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?" +
                     " AND SALE_PARM=? AND CONTENT_TYPE=? AND RULE_ID=?";
        ResultSet rs = null;
        PreparedStatement prepStmt = null;
        try {
            if (conn == null) {
                conn = DriverManager.getConnection(DBConstant.DBURL, DBConstant.DBUSER, DBConstant.DBPASSWORD);
                conn.setAutoCommit(false);
            }
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
            LogUtil.printLog("DBTimer 检查数据是否存在");
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
            }
        }
        return false;
    }

    private void insertData(String recordDay,String provinceId, String contentId, String contentType,
                            String channelCode, String ruleId, double abnormalFee, double totalFee) throws SQLException {
        if (DBStatisticBoltHelper.parameterId2ChannelIds == null) {
            DBStatisticBoltHelper.getData();
        }
        if (!DBStatisticBoltHelper.parameterId2ChannelIds.containsKey(channelCode)) {
            log.error("营销参数维表更新错误:" + new Date() + "==>" + channelCode);
            return;
        }
        if (Integer.parseInt(ruleId) == 0) {
            return;
        }
        String[] chls = DBStatisticBoltHelper.parameterId2ChannelIds.get(channelCode).split("\\|");
        String chl1 = chls[0];
        String chl2 = chls[1];
        String chl3 = chls[2];
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
            if (conn == null) {
                conn = DriverManager.getConnection(DBConstant.DBURL, DBConstant.DBUSER, DBConstant.DBPASSWORD);
                conn.setAutoCommit(false);
            }
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
            if (StatisticsBolt.isDebug) {
                log.info("数据插入成功" + insertDataSql);
            }
        } catch (SQLException e) {
            log.error("插入sql错误" + insertDataSql);
            e.printStackTrace();
        } finally {
            if (prepStmt != null) {
                prepStmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    private void updateDate(String date, String provinceId, String contentID, String contentType,
                            String channelCode, String ruleId, double abnormalFee, double totalFee) throws SQLException {
        String checkExistsSql = "SELECT ODR_ABN_FEE,ODR_FEE FROM " + StormConf.realTimeOutputTable
                + " WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?"+
                " AND SALE_PARM=? AND CONTENT_TYPE=? AND RULE_ID=?";

        PreparedStatement prepStmt = null;
        try {
            if (conn == null) {
                conn = DriverManager.getConnection(DBConstant.DBURL, DBConstant.DBUSER, DBConstant.DBPASSWORD);
                conn.setAutoCommit(false);
            }
            prepStmt = conn.prepareStatement(checkExistsSql);
            prepStmt.setString(1, date);
            prepStmt.setString(2, provinceId);
            prepStmt.setString(3, contentID);
            prepStmt.setString(4, channelCode);
            prepStmt.setString(5, contentType);
            prepStmt.setString(6, ruleId);

            double abnormalFeeOld = 0;
            double orderFeeOld = 0;
            ResultSet rs = prepStmt.executeQuery();
            while (rs.next()) {
                abnormalFeeOld = rs.getDouble("ODR_ABN_FEE");
                orderFeeOld = rs.getDouble("ODR_FEE");
            }
            rs.close();
            double abnormalFeeNew = abnormalFeeOld + abnormalFee;
            double orderFeeNew = orderFeeOld + totalFee;
            double rateNew = abnormalFeeNew / orderFeeNew;

            String updateSql = " UPDATE " + StormConf.realTimeOutputTable
                    +" SET ODR_ABN_FEE=\'"+abnormalFeeNew+"\', ODR_FEE=\'"+orderFeeNew+"\',"+
                    "ABN_RAT=\'"+rateNew+
                    " \' WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?" +
                    " AND SALE_PARM=? AND CONTENT_TYPE=? AND RULE_ID=?";

            prepStmt = conn.prepareStatement(updateSql);
            prepStmt.setString(1, date);
            prepStmt.setString(2, provinceId);
            prepStmt.setString(3, contentID);
            prepStmt.setString(4, channelCode);
            prepStmt.setString(5, contentType);
            prepStmt.setString(6, ruleId);
            prepStmt.executeUpdate();
            prepStmt.execute("commit");
            LogUtil.printLog("DBTimer 更新数据成功");
        } catch (SQLException e) {
            log.error("查询sql错误" + checkExistsSql);
            e.printStackTrace();
        }finally {
            if (prepStmt != null) {
                prepStmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}
