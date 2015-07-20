package com.order.db.DBHelper;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import com.order.db.JDBCUtil;
import org.apache.log4j.Logger;

import com.order.constant.Rules;
import com.order.util.OrderRecord;
import com.order.util.StormConf;
import com.order.util.TimeParaser;

/**
 * 用于实现仓库输出表的数据缓存、put、get、回溯查询和定期入库操作。
 * * 输出表结构:
 * CREATE TABLE AAS.IREAD_ORDER_ABN_RULE
 * (
 * RECORDTIME   DATE,
 * MSISDN        VARCHAR2(32),
 * SESSIONID     VARCHAR2(40),
 * CHANNELCODE   VARCHAR2(40),
 * BOOKID        VARCHAR2(19),
 * PRODUCTID     VARCHAR2(32),
 * REALFEE       NUMBER,
 * RULE_1        VARCHAR2(2),
 * RULE_2        VARCHAR2(2),
 * RULE_3        VARCHAR2(2),
 * RULE_4        VARCHAR2(2),
 * RULE_5        VARCHAR2(2),
 * RULE_6        VARCHAR2(2),
 * RULE_7        VARCHAR2(2),
 * RULE_8        VARCHAR2(2),
 * RULE_9        VARCHAR2(2),
 * RULE_10       VARCHAR2(2),
 * RULE_11       VARCHAR2(2),
 * RULE_12       VARCHAR2(2)
 * )
 * Created by Guo Hongbo on 2015/6/15.
 */

public class DBDataWarehouseCacheHelper implements Serializable {
	private static final long serialVersionUID = 1L;
    private static Logger log = Logger.getLogger(DBDataWarehouseCacheHelper.class);
	private transient Connection conn = null;

	private transient Thread cleaner = null; // 清理线程
    private transient Object LOCK = null;

	private final int clearTimer = 1 * 5 * 60; // 每5分钟秒清理一次
	private final long historyTimer = 1 * 60 * 60; // 每次清理60分钟前的所有订购，并入库
	private long dropnum = 0;
	
	// 用户订购记录
	private ConcurrentHashMap<String, ArrayList<OrderRecord>> orderMap;

	private Connection getConn() throws SQLException {
		if (conn == null) {
			log.info("Connection is null!");
			conn = JDBCUtil.connUtil.getConnection();
			conn.setAutoCommit(false);
		}
		return conn;
	}

	public DBDataWarehouseCacheHelper() {
		/* 连接数据库 */
		try {
			conn = this.getConn();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (LOCK == null)
			LOCK = new Object();
		synchronized (LOCK) {
			/* 定时清理内存中的订购记录到数据库中 */
			orderMap = new ConcurrentHashMap<String, ArrayList<OrderRecord>>();
		}
	}

	/* 插入新的订购记录 */
	public int insertData(String msisdn, String sessionId, String channelCode,
			Long recordTime, String bookID, String productID,
			double realInfoFee, String provinceId, int orderType) {
		if (cleaner == null) {
			cleaner = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						Thread.sleep((new Random()).nextInt(clearTimer * 1000));
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					while (true) {
						try {
							// 每隔一个一段时间清理一次。
							cleaner.sleep(clearTimer * 1000);
							log.info("Begin Clean DataWareHouse cache ...");
							cleanAndToDB();
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			});
			cleaner.setDaemon(true);
			cleaner.start();
		}
		OrderRecord order = new OrderRecord();
		order.setRecordTime(recordTime);
		order.setMsisdn(msisdn);
		order.setSessionId(sessionId);
		order.setChannelCode(channelCode);
		order.setBookID(bookID);
		order.setProductID(productID);
		order.setRealfee(realInfoFee);
		order.setProvinceId(provinceId);
		order.setOrderType(orderType);
		for (int i = 1; i < 13; i++) {
			order.getRules().put(i, 1);
		}

		// 加锁
		if (LOCK == null)
			LOCK = new Object();
		synchronized (LOCK) {
			// 查找如果该订购在内存中，返回0
			if (orderMap.containsKey(msisdn)) {
				// 由于有完全相同的订购消息，所以不判断原来是否存在该订购，直接存入
				// Iterator<OrderRecord> itOrder =
				// orderMap.get(msisdn).iterator();
				// while (itOrder.hasNext()) {
				// OrderRecord oneRecord = itOrder.next();
				// if (oneRecord.equals(order)) {
				// return 0;
				// }
				// }
				orderMap.get(msisdn).add(order);
				return 1;
			} else {
				ArrayList<OrderRecord> list = new ArrayList<OrderRecord>();
				list.add(order);
				orderMap.put(msisdn, list);
				return 1;
			}
			// log.info("insert result: " + this.toString());
		}
	}

	/* 更新订购记录某一个规则的异常状态 */
	public int updateData(String msisdn, String sessionId, String channelCode,
			Long recordTime, String bookID, String productID,
			double realInfoFee, String provinceId, int orderType, String rule) {
		if (cleaner == null) {
			cleaner = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						Thread.sleep((new Random()).nextInt(clearTimer * 1000));
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					while (true) {
						try {
							// 每隔一个一段时间清理一次。
							cleaner.sleep(clearTimer * 1000);
							cleanAndToDB();
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			});
			cleaner.setDaemon(true);
			cleaner.start();
		}
		OrderRecord order = new OrderRecord();
		order.setRecordTime(recordTime);
		order.setMsisdn(msisdn);
		order.setSessionId(sessionId);
		order.setChannelCode(channelCode);
		order.setBookID(bookID);
		order.setProductID(productID);
		order.setRealfee(realInfoFee);
		order.setProvinceId(provinceId);
		order.setOrderType(orderType);

		// 加锁
		if (LOCK == null)
			LOCK = new Object();
		synchronized (LOCK) {
			if (orderMap.containsKey(msisdn)) {
				Iterator<OrderRecord> itOrder = orderMap.get(msisdn).iterator();
				while (itOrder.hasNext()) {
					OrderRecord oneRecord = itOrder.next();
					if (oneRecord.equals(order)) {
						int ruleId = getRuleNumFromString(rule);
						if (oneRecord.getRules().get(ruleId) != 0) {
							oneRecord.getRules().put(
									getRuleNumFromString(rule), 0);
							return 1;
						} else {
							return 0;
						}
					}
				}
			}
			// log.info("update result: " + this.toString());
			return -1;
		}
	}

	/* 回溯前一段时间的订购，返回之前判断为正常的订购 */
    public ArrayList<OrderRecord> traceBackOrders(String msisdn, String channelCode, Long traceBackTime, int ruleID) {
		ArrayList<OrderRecord> relist = new ArrayList<OrderRecord>();
		// 加锁
		if (LOCK == null)
			LOCK = new Object();
		synchronized (LOCK) {

			if (!orderMap.containsKey(msisdn)) {
				return relist;
			}

			Iterator<OrderRecord> itOrder = orderMap.get(msisdn).iterator();
			while (itOrder.hasNext()) {
				OrderRecord oneRecord = itOrder.next();
				// 如果channelCode不为空，则需要判断channelCode
				if (channelCode != null && !channelCode.trim().equals("")
						&& !oneRecord.getChannelCode().equals(channelCode)) {
					continue;
				}
				// 如果traceBackTime不为空，则需要订购时间大于等于traceBackTime
				if (traceBackTime != null
						&& oneRecord.getRecordTime() < traceBackTime) {
					continue;
				}
				// 如果ruleID为1-12，则获取之前为判断之前为正常的订购，并将状态改为异常
				if (ruleID >= 1 && ruleID <= 12
						&& oneRecord.getRules().get(ruleID) == 1) {
					oneRecord.getRules().put(ruleID, 0);
					relist.add(oneRecord);
				}
			}
			// log.info("trackback result: " + this.toString());
			return relist;
		}
	}

	public void cleanAndToDB() throws Exception {
		// 加锁
		if (LOCK == null)
			LOCK = new Object();
		synchronized (LOCK) {
			log.info("====Begin cleanAndToDB ");
			long currentTime = System.currentTimeMillis();
			currentTime = currentTime - 1000 * historyTimer;

			log.info("====Begin cleanAndToDB before "
					+ TimeParaser.formatTimeInSeconds(currentTime));
			// 要入库的订购记录列表
			ArrayList<OrderRecord> insertList = new ArrayList<OrderRecord>();

			// 遍历
			Iterator<Map.Entry<String, ArrayList<OrderRecord>>> itMsisdn = orderMap
					.entrySet().iterator();
			while (itMsisdn.hasNext()) {
				Map.Entry<String, ArrayList<OrderRecord>> entry = itMsisdn
						.next();
				// 获取一个用户的订购列表
				ArrayList<OrderRecord> orderList = entry.getValue();
				Iterator<OrderRecord> itOrder = orderList.iterator();
				while (itOrder.hasNext()) {
					// 获取该用户某次订购
					OrderRecord oneRecord = itOrder.next();
					// 如果订购时间小于阀值
					if (oneRecord.getRecordTime() < currentTime) {
						// 要入库的订购记录列表
						insertList.add(oneRecord);
						count("drop");
						// 删除此条订购记录
						itOrder.remove();
					}
				}
				// 如果用户的订购列表为空，则删除该用户
				if (orderList.size() == 0) {
					itMsisdn.remove();
				}
			}
			// 将删除的订购记录批量入库
			insertOrdersToDB(insertList);
			log.info("====cleanAndToDB result size: "
					+ String.valueOf(orderMap.size()));
		}
	}

	// 批量入库
	private void insertOrdersToDB(ArrayList<OrderRecord> orderList)
			throws Exception {
		PreparedStatement pst = null;
		try {
			long startTime = System.currentTimeMillis();
			String sql = "insert into " + StormConf.dataWarehouseTable
					+ " VALUES (?,?,?,?,?,?,?," + "?,?,?,?,?,?,?,?,?,?,?,?,?)";
			conn = JDBCUtil.connUtil.getConnection();
			conn.setAutoCommit(false);
			pst = conn.prepareStatement(sql);
			Iterator<OrderRecord> itOrder = orderList.iterator();
			while (itOrder.hasNext()) {
				OrderRecord oneRecord = itOrder.next();
				pst.setDate(1, new Date(oneRecord.getRecordTime()));
				pst.setString(2, oneRecord.getMsisdn());
				pst.setString(3, oneRecord.getSessionId());
				pst.setString(4, oneRecord.getChannelCode());
				pst.setString(5, oneRecord.getBookID());
				pst.setString(6, oneRecord.getProductID());
				pst.setDouble(7, oneRecord.getRealfee());
				for (int i = 8; i < 20; i++) {
					pst.setString(i, oneRecord.getRules().get(i - 7).toString());
				}
				pst.setString(20, TimeParaser.formatTimeInSeconds(oneRecord.getRecordTime()));
				pst.addBatch();
				// log.info("AddBatch: " + oneRecord.toString());
			}
			pst.executeBatch();
			conn.commit();
			long endTime = System.currentTimeMillis();
			log.info("====The patch insert " + String.valueOf(orderList.size())
					+ " order record taked time ：" + (endTime - startTime)
					+ "ms");
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
	}
	
    public void count(String colume) {
    	if (colume.equals("drop")) {
    		dropnum++;
	    	if (dropnum >= 1000) {
	    		DBOrderCount.updateDbSum("DataWarehouseBolt", "drop", 1000);
	    		dropnum=0;
	    	}
	    }
    }

	public String toString() {
		String result = "\n size of order Map is "
				+ String.valueOf(orderMap.size()) + "\n";
		// 遍历
		Iterator<Map.Entry<String, ArrayList<OrderRecord>>> itMsisdn = orderMap
				.entrySet().iterator();
		while (itMsisdn.hasNext()) {
			Map.Entry<String, ArrayList<OrderRecord>> entry = itMsisdn.next();
			// 获取一个用户的订购列表
			ArrayList<OrderRecord> orderList = entry.getValue();
			Iterator<OrderRecord> itOrder = orderList.iterator();
			while (itOrder.hasNext()) {
				// 获取该用户某次订购
				OrderRecord oneRecord = itOrder.next();
				result += oneRecord.toString() + "\n";
			}
		}
		return result;
	}

	/* 获取异常规则对应的数字编号 */
	public int getRuleNumFromString(String rule) {
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