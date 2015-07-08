package com.order.db.DBHelper;

import org.apache.log4j.Logger;

import com.order.db.JDBCUtil;
import com.order.util.TimeParaser;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by GuoHongbo on 2015/07/08.
 */
public class DBOrderCount {
    private static Logger log =Logger.getLogger(DBOrderCount.class);
    
    public static void updateDbSum(String name, String column, long count) {
    	String date = TimeParaser.formatTimeInDay(System.currentTimeMillis());
        if (checkExists(date, name)) {
            updateRecord(date, name, column, count);
         } else {
        	 insertRecord(date, name);
        	 updateRecord(date, name, column, count);
         }
    }
    
    public static boolean checkExists(String date, String name) {
        String checkExistsSql = "SELECT COUNT(*) counts FROM AAS.ORDER_COUNT"
                + " WHERE DATE=? AND NAME=?";
        ResultSet rs = null;
        PreparedStatement prepStmt = null;
        Connection conn = null;
        try {
        	conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            prepStmt = conn.prepareStatement(checkExistsSql);
            prepStmt.setString(1, date);
            prepStmt.setString(2, name);
            rs = prepStmt.executeQuery();
            rs.next();
            int count = rs.getInt("counts");
            rs.close();
            prepStmt.close();
            return count != 0;
        } catch (SQLException e) {
            log.error("查询sql错误" + checkExistsSql);
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
            }
            if (prepStmt != null) {
                try {
					prepStmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
            }
            if (conn != null) {
                try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
                conn = null;
            }
        }
        return false;
    }
    
    
	private static void insertRecord(String date, String name) {
		String insertDataSql = "INSERT INTO AAS.ORDER_COUNT"
				+ "( DATE,NAME,recv,drop,send)"
				+ "VALUES(?,?,0,0,0)";
		Connection conn = null;
		PreparedStatement prepStmt = null;
		try {
			conn = JDBCUtil.connUtil.getConnection();
			conn.setAutoCommit(false);
			prepStmt = conn.prepareStatement(insertDataSql);
			prepStmt.setString(1, date);
			prepStmt.setString(2, name);
			prepStmt.execute();
			prepStmt.execute("commit");
		} catch (SQLException e) {
			log.error("插入sql错误:" + insertDataSql);
			e.printStackTrace();
		} finally {
			if (prepStmt != null) {
				try {
					prepStmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				conn = null;
			}
		}
	}
    
    
    public static void updateRecord(String date, String name, String column, long count) {
        PreparedStatement prepStmt = null;
        String updateSql = null;
		Connection conn = null;
        try {
            conn = JDBCUtil.connUtil.getConnection();
            conn.setAutoCommit(false);
            updateSql = " UPDATE AAS.ORDER_COUNT " +
                    " SET ?=?+? WHERE DATE=? AND NAME=? ";
            prepStmt = conn.prepareStatement(updateSql);
            prepStmt.setString(1, column);
            prepStmt.setString(2, column);
            prepStmt.setLong(3, count);
            prepStmt.setString(4, date);
            prepStmt.setString(5, name);
            prepStmt.executeUpdate();
            prepStmt.execute("commit");
        } catch (SQLException e) {
            log.error("更新sql错误" + updateSql);
            e.printStackTrace();
        }finally {
            if (prepStmt != null) {
                try {
					prepStmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
            }
            if (conn != null) {
                try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
                conn = null;
            }
        }    	
    }
}
