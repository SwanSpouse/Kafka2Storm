package com.order.db.DBHelper;

import com.order.constant.Constant;
import com.order.db.JDBCUtil;
import com.order.util.LogUtil;
import com.order.util.OrderRecord;
import com.order.util.StormConf;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.sql.*;
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

    // 新增两个MAP用于入库时的内存复制
    public ConcurrentHashMap<String, Double> abnormalFeeTmp = null;
    public ConcurrentHashMap<String, Double> totalFeeTmp = null;

    
    public DBTimer(DBRealTimeOutputBoltHelper helper) {
        this.helper = helper;
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

	@Override
	public void run() {
		super.run();
		try {
			Thread.sleep((new Random()).nextInt(Constant.ONE_MINUTE * 5 * 1000)); // test
			while (true) {
				Thread.sleep(Constant.ONE_MINUTE * 5 * 1000L); // RealTime 实时统计表每5分钟更新一次。
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

                String chl1;
                String chl2;
                String chl3;
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
