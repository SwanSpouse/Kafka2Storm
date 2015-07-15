package com.order.db.DBHelper;

import com.order.constant.Constant;
import com.order.db.JDBCUtil;
import com.order.util.LogUtil;
import com.order.util.OrderRecord;
import com.order.util.StormConf;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by LiMingji on 2015/6/9.
 */
public class DBTimer extends Thread {
    private static Logger log = Logger.getLogger(DBTimer.class);

    private Connection conn = null;
    private DBRealTimeOutputBoltHelper helper = null;
    private long lastClearDayDataTime = 0;
    private long lastClearMinDataTime = 0;
    private final long dayMillis = 24*60*60*1000;
    private final long minMillis = 15 * 60 * 1000;
    //private static transient Object LOCK = null;
    
    // 新增两个MAP用于入库时的内存复制
    public ConcurrentHashMap<String, Double> abnormalFeeTmp = null;
    public ConcurrentHashMap<String, Double> totalFeeTmp = null;

    
    public DBTimer(DBRealTimeOutputBoltHelper helper) {
        this.helper = helper;
    	long nowtime = System.currentTimeMillis();
    	lastClearDayDataTime = nowtime - nowtime%dayMillis;
    	lastClearMinDataTime = nowtime;
    	// 初始化两个Map
    	abnormalFeeTmp = new ConcurrentHashMap<String, Double>();
    	totalFeeTmp = new ConcurrentHashMap<String, Double>();
    	// 如果营销参数与各级渠道对应关系为空，曾先读取
        if (DBStatisticBoltHelper.parameterId2ChannelIds == null || DBStatisticBoltHelper.parameterId2ChannelIds.isEmpty()) {
            try {
                DBStatisticBoltHelper.getData();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // 先不采用在excute中清理的方式
	public void checkClear() {
//		long nowTime = System.currentTimeMillis();
//		if (nowTime < lastClearMinDataTime + minMillis) {
//			return;
//		}
//		lastClearMinDataTime = nowTime;
//		
//		try {
//			log.info("===将RealTime缓存中的数据更新到数据库中===");
//			// 将map中增量异常费用和增量总费用数据更新到数据库中
//			this.updateAllTotalFeeToDB();
//			this.updateAllAbnormalFeeToDB();
//			helper.totalFee.clear();
//			helper.abnormalFee.clear();
//			
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
	}
    
	@Override
	public void run() {
		super.run();
		try {
			Thread.sleep((new Random()).nextInt(Constant.ONE_MINUTE * 10 * 1000)); // test
			while (true) {
				Thread.sleep(Constant.ONE_MINUTE * 10 * 1000L); // test
				log.info("===将RealTime缓存中的数据更新到数据库中===");
				// 将map中增量异常费用和增量总费用数据更新到数据库中
				if (helper.getLOCK() == null)
					helper.setLOCK(new Object());
				synchronized (helper.getLOCK()) {
					// 将内存中的数据复制过来后，清空原Map使excute正常访问
					abnormalFeeTmp.clear();
					abnormalFeeTmp.putAll(helper.abnormalFee);
					helper.abnormalFee.clear();
					totalFeeTmp.clear();
					totalFeeTmp.putAll(helper.totalFee);
					helper.totalFee.clear();
				}

				long startTime = System.currentTimeMillis();
				int totalCount = this.insertAllTotalFeeToDB();
				long allTime = System.currentTimeMillis();
				int abnormalCount = this.insertAllAbnormalFeeToDB();
				long abnormalTime = System.currentTimeMillis();
				log.info("====DBTimer ClearToDB, totalFee insert "
						+ totalCount + " records cost " + (allTime - startTime)
						+ " ms, abnormalFee insert " + abnormalCount
						+ " records cost " + (abnormalTime - allTime) + " ms====");
				
				/*long startTime = System.currentTimeMillis();
				int totalCount = this.updateAllTotalFeeToDB();
				long allTime = System.currentTimeMillis();
				int abnormalCount = this.updateAllAbnormalFeeToDB();
				long abnormalTime = System.currentTimeMillis();
				log.info("====DBTimer ClearToDB, totalFee operate "
						+ totalCount + " records cost " + (allTime - startTime)
						+ " ms, abnormalFee operate " + abnormalCount
						+ " records cost " + (abnormalTime - allTime) + " ms====");*/
				abnormalFeeTmp.clear();
				totalFeeTmp.clear();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
 
	public int insertAllAbnormalFeeToDB() throws SQLException {
		log.info("====insertAllAbnormalFeeToDB开始插入增量异常费用===");
		PreparedStatement pst = null;
		try {
			long startTime = System.currentTimeMillis();
			String sql = "INSERT INTO " + StormConf.realTimeOutputTable +
	                "( RECORD_DAY,PROVINCE_ID,CHL1,CHL2,CHL3," +
	                "  CONTENT_ID,SALE_PARM,ODR_ABN_FEE,ODR_FEE," +
	                "  ABN_RAT,CONTENT_TYPE,RULE_ID,LOAD_TIME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
			conn = JDBCUtil.connUtil.getConnection();
			conn.setAutoCommit(false);
			pst = conn.prepareStatement(sql);
			
			String nowTimeString = TimeParaser.formatTimeInSeconds(System.currentTimeMillis());
	        for (String key : abnormalFeeTmp.keySet()) {
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
	            // 获取增量异常费用
	            double abnFee = abnormalFeeTmp.get(key);
	            // 增量异常为0,则continue
	            if (abnFee < 1) {
	            	continue;
	            }

	            String chl1 = null;
	            String chl2 = null;
	            String chl3 = null;
	            if (!DBStatisticBoltHelper.parameterId2ChannelIds.containsKey(channelCode)) {
	                chl1 = chl2 = chl3 = "NULL";
	            } else {
	                String[] chls = DBStatisticBoltHelper.parameterId2ChannelIds.get(channelCode).split("\\|");
	                chl1 = chls[0];
	                chl1 = chl1 == null ? "NULL" : chl1;
	                chl2 = chls[1];
	                chl2 = chl2 == null ? "NULL" : chl2;
	                chl3 = chls[2];
	                chl3 = chl3 == null ? "NULL" : chl3;
	            }
	            
	            pst.setString(1, date);
	            pst.setString(2, provinceId);
	            pst.setString(3, chl1);
	            pst.setString(4, chl2);
	            pst.setString(5, chl3);
	            pst.setString(6, contentID);
	            pst.setString(7, channelCode);
	            pst.setDouble(8, abnFee);
	            pst.setDouble(9, 0.0);
	            pst.setDouble(10, 0.0);
	            pst.setString(11, contentType);
	            pst.setInt(12, Integer.parseInt(ruleID));
	            pst.setString(13, nowTimeString);
				pst.addBatch();
			}
			pst.executeBatch();
			conn.commit();
			long endTime = System.currentTimeMillis();
			log.info("====The patch insert " + String.valueOf(abnormalFeeTmp.size())
					+ " abnormal fee taked time ：" + (endTime - startTime)
					+ "ms");
			return abnormalFeeTmp.size();
		} catch (SQLException e) {
			e.printStackTrace();
			log.error("insert data to DB is failed.");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pst != null) {
				pst.close();
			}
			if (conn != null) {
				conn.close();
				conn = null;
			}
		}
		return 0;
     }
	
	public int insertAllTotalFeeToDB() throws SQLException {
		log.info("====insertAllTotalFeeToDB开始插入增量总费用===");
		PreparedStatement pst = null;
		try {
			long startTime = System.currentTimeMillis();
			String sql = "INSERT INTO " + StormConf.realTimeOutputTable +
	                "( RECORD_DAY,PROVINCE_ID,CHL1,CHL2,CHL3," +
	                "  CONTENT_ID,SALE_PARM,ODR_ABN_FEE,ODR_FEE," +
	                "  ABN_RAT,CONTENT_TYPE,RULE_ID,LOAD_TIME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
			conn = JDBCUtil.connUtil.getConnection();
			conn.setAutoCommit(false);
			pst = conn.prepareStatement(sql);

			String nowTimeString = TimeParaser.formatTimeInSeconds(System.currentTimeMillis());
	        for (String key : totalFeeTmp.keySet()) {
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
	            double fee = totalFeeTmp.get(key);
	            //总费用为0不入库
	            if (fee < 1) {
	            	continue;
	            }

	            String chl1 = null;
	            String chl2 = null;
	            String chl3 = null;
	            if (!DBStatisticBoltHelper.parameterId2ChannelIds.containsKey(channelCode)) {
	                chl1 = chl2 = chl3 = "NULL";
	            } else {
	                String[] chls = DBStatisticBoltHelper.parameterId2ChannelIds.get(channelCode).split("\\|");
	                chl1 = chls[0];
	                chl1 = chl1 == null ? "NULL" : chl1;
	                chl2 = chls[1];
	                chl2 = chl2 == null ? "NULL" : chl2;
	                chl3 = chls[2];
	                chl3 = chl3 == null ? "NULL" : chl3;
	            }
	            
	            pst.setString(1, date);
	            pst.setString(2, provinceId);
	            pst.setString(3, chl1);
	            pst.setString(4, chl2);
	            pst.setString(5, chl3);
	            pst.setString(6, contentID);
	            pst.setString(7, channelCode);
	            pst.setDouble(8, 0.0);
	            pst.setDouble(9, fee);
	            pst.setDouble(10, 0.0);
	            pst.setString(11, contentType);
	            pst.setInt(12, 0);
	            pst.setString(13, nowTimeString);
				pst.addBatch();
			}
			pst.executeBatch();
			conn.commit();
			long endTime = System.currentTimeMillis();
			log.info("====The patch insert " + String.valueOf(totalFeeTmp.size())
					+ " total fee taked time ：" + (endTime - startTime)
					+ "ms");
			return totalFeeTmp.size();
		} catch (SQLException e) {
			e.printStackTrace();
			log.error("insert data to DB is failed.");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pst != null) {
				pst.close();
			}
			if (conn != null) {
				conn.close();
				conn = null;
			}
		}
		return 0;
     }
	
    public int updateAllAbnormalFeeToDB() throws SQLException {
    	// 将异常订购费用更新到库里，并清除内存
        log.info("====DBTimer开始更新增量异常费用===");
        int insertCnt = 0;
        int updateCnt = 0;
        for (String key : abnormalFeeTmp.keySet()) {
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
            if (!totalFeeTmp.containsKey(totalFeeKey)) {
                continue;
            }
            double fee = totalFeeTmp.get(totalFeeKey);
            // 总费用等于0, 则忽略此异常费用
            if (fee < 1) continue;

            //String abnormalFeeKey = totalFeeKey + "|" + ruleID;
            double abnFee = abnormalFeeTmp.get(key);
            // 增量总费用小于增量异常费用,则赋值
            if (fee < abnFee) {
            	abnFee = fee;
            }
            
            try {
            	// 若update条数小于等于0，说明原记录不存在，进行插入
            	if (this.updateAbnormalFee(date, provinceId, contentID, contentType, channelCode, ruleID, abnFee, fee) <= 0) {
                    this.insertAbnormalFee(date, provinceId, contentID, contentType, channelCode, ruleID, abnFee, fee);
                    insertCnt++;
            	} else {
            		updateCnt++;
				}
            	
            	// 原处理逻辑
                //if (checkExists(date, provinceId, contentID, contentType, channelCode, ruleID)) {
                //   this.updateAbnormalFee(date, provinceId, contentID, contentType, channelCode, ruleID, abnFee, fee);
                //   updateCnt++;
                //} else {
                //    this.insertAbnormalFee(date, provinceId, contentID, contentType, channelCode, ruleID, abnFee, fee);
                //    insertCnt++;
                //}
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        log.info("===DBTimer更新异常统计信息,总共" + String.valueOf(abnormalFeeTmp.size())
        		+ "条，其中update" + String.valueOf(updateCnt) + "条， insert " + String.valueOf(insertCnt) + "条");
        abnormalFeeTmp.clear();
        return insertCnt+updateCnt;
    }
    
    public int updateAllTotalFeeToDB() throws SQLException {
        log.info("====updateTotalDB开始更新增量总费用===");
        int insertCnt = 0;
        int updateCnt = 0;
        // 将总费用更新到库中（ruleid为0）,不清空内存
        for (String key : totalFeeTmp.keySet()) {
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
            double fee = totalFeeTmp.get(key);
            //总费用为0不入库
            if (fee < 1) {
            	continue;
            }
            
            // 如果总费用记录不存在，则首先插入
            if (!checkExists(date, provinceId, contentID, contentType, channelCode, ruleID)) {
                if (!this.insertAbnormalFee(date, provinceId, contentID, contentType, channelCode, ruleID, 0, 0)) {
                	totalFeeTmp.put(key, (double) 0);
                	continue;
                }
                insertCnt++;
            }
            // 插入后更新所有该Key的总费用记录和异常费用记录中的总费用
            if(!this.updateTotalFee(date, provinceId, contentID, contentType, channelCode, fee)) {
            	totalFeeTmp.put(key, (double) 0);
            }
            updateCnt++;
            
            // 更新完毕后，将增量总费用置零
            //helper.totalFee.put(key, (double) 0);
            
            // 之前逻辑注释
            //if (checkExists(date, provinceId, contentID, contentType, channelCode, ruleID)) {
            //    this.updateTotalFee(date, provinceId, contentID, contentType, channelCode, fee);
            //    updateCnt++;
            //} else {
            //    this.insertAbnormalFee(date, provinceId, contentID, contentType, channelCode, ruleID, 0, fee);
            //    insertCnt++;
            //}
        }
        log.info("===updateTotalDB更新" + String.valueOf(totalFeeTmp.size()) + "条总费用统计信息到数据库中===");
        log.info("===updateTotalDB更新异常统计信息,总共" + String.valueOf(totalFeeTmp.size())
        		+ "条，其中update" + String.valueOf(updateCnt) + "条， insert " + String.valueOf(insertCnt) + "条");
        return updateCnt;
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

    private boolean insertAbnormalFee(String recordDay,String provinceId, String contentId, String contentType,
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
            return true;
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
        return false;
    }

    private int updateAbnormalFee(String date, String provinceId, String contentID, String contentType,
                            String channelCode, String ruleId, double abnormalFee, double totalFee) throws SQLException {
        PreparedStatement prepStmt = null;
        String updateSql = null;
        try {
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            updateSql = " UPDATE " + StormConf.realTimeOutputTable +
                    " SET ODR_ABN_FEE=ODR_ABN_FEE+?, " +
                    " ABN_RAT=(ODR_ABN_FEE+?)/ODR_FEE" +
                    " WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?" +
                    " AND SALE_PARM=? AND CONTENT_TYPE=? AND RULE_ID=? ";
            prepStmt = conn.prepareStatement(updateSql);
            prepStmt.setDouble(1, abnormalFee);
            prepStmt.setDouble(2, abnormalFee);
            prepStmt.setString(3, date);
            prepStmt.setString(4, provinceId);
            prepStmt.setString(5, contentID);
            prepStmt.setString(6, channelCode);
            prepStmt.setString(7, contentType);
            prepStmt.setString(8, ruleId);
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
    
    private boolean updateTotalFee(String date, String provinceId, String contentID, String contentType,
                            String channelCode, double totalFee) throws SQLException {
        PreparedStatement prepStmt = null;
        String updateSql = null;
        try {
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            updateSql = " UPDATE " + StormConf.realTimeOutputTable +
                    " SET ODR_FEE=ODR_FEE+?, " +
                    " ABN_RAT=ODR_ABN_FEE/(ODR_FEE+?)" +
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
            return true;
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
        return false;
    }
    
	public void setAllTotalFeeToZero() throws SQLException {
		log.info("====开始设定所有总费用为0===");
		for (String key : totalFeeTmp.keySet()) {
			totalFeeTmp.put(key, (double) 0);
		}
	}

    public String toString() {
        String result = "\n size of totalFee is " + String.valueOf(totalFeeTmp.size()) + "\n";
        // 遍历
        Iterator<Map.Entry<String, Double>> it = totalFeeTmp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Double> entry = it.next();
            result += entry.getKey() + " " + String.valueOf(entry.getValue()) + "\n";
        }

        result += "\n size of ABFee is " + String.valueOf(abnormalFeeTmp.size()) + "\n";
        Iterator<Map.Entry<String, Double>> it2 = abnormalFeeTmp.entrySet().iterator();
        while (it2.hasNext()) {
            Map.Entry<String, Double> entry2 = it2.next();
            result += entry2.getKey() + " " + String.valueOf(entry2.getValue()) + "\n";
        }

        return result;
    }
}
