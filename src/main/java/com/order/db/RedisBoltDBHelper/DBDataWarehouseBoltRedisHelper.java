package com.order.db.RedisBoltDBHelper;

import com.order.constant.Rules;
import com.order.db.JDBCUtil;
import com.order.util.OrderRecord;
import com.order.util.StormConf;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

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

public class DBDataWarehouseBoltRedisHelper implements Serializable {
	private static final long serialVersionUID = 1L;
    private static Logger log = Logger.getLogger(DBDataWarehouseBoltRedisHelper.class);
	private transient Connection conn = null;

	private transient Thread cleaner = null; // 清理线程
    private transient Object LOCK = null;

	private final int clearTimer = 1 * 5 * 60; // 每5分钟秒清理一次

    // 用户订购记录
    private ArrayList<OrderRecord> orderList;

	private Connection getConn() throws SQLException {
		if (conn == null) {
			log.info("Connection is null!");
			conn = JDBCUtil.connUtil.getConnection();
			conn.setAutoCommit(false);
		}
		return conn;
	}

	public DBDataWarehouseBoltRedisHelper() {
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
            orderList = new ArrayList<OrderRecord>();
		}
	}

    /* 插入新的订购记录 */
    public void insertDataToCache(Long recordTime, String msisdn, String sessionId, String channelCode,
                           String bookID, String productID, double realInfoFee, String rules) {
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
        for (int i = 1; i < 13; i++) {
            order.getRules().put(i, 1);
        }
        modifyRules(order, rules);
        orderList.add(order);
    }

    private void modifyRules(OrderRecord orderRecord, String rules) {
        String[] ruleArr = rules.split("\\|");
        for (int i = 0; i < ruleArr.length; i++) {
            if (ruleArr[i].trim().equals("")) {
                continue;
            }
            orderRecord.getRules().put(this.getRuleNumFromString(ruleArr[i]), 0);
        }
    }

	public void cleanAndToDB() throws Exception {
		if (LOCK == null) {
            LOCK = new Object();
        }
		synchronized (LOCK) {
			// 要入库的订购记录列表
			ArrayList<OrderRecord> insertList = new ArrayList<OrderRecord>();
            insertList.addAll(orderList);
            orderList.clear();
        }
        insertOrdersToDB(orderList);
        orderList.clear();
    }

	// 批量入库
	private void insertOrdersToDB(ArrayList<OrderRecord> orderList)
			throws Exception {
		PreparedStatement pst = null;
		try {
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
			}
			pst.executeBatch();
			conn.commit();
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