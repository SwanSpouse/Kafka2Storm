package com.order.db;

import com.order.util.StormConf;

import java.sql.*;

public class DBTest {

    /**
     * 如何在bolt中使用Oracle的JDBC.
     */
    public static void main(String[] args) throws SQLException {

        Date traceBackTime = new Date(1433494523823L);
        Connection conn = null;
        conn = DriverManager.getConnection(DBConstant.DBURL, DBConstant.DBUSER, DBConstant.DBPASSWORD);
        conn.setAutoCommit(false);
        String updateOrderSql = "UPDATE " + StormConf.dataWarehouseTable + " SET \"RULE_" + 1 + "\"=1" +
                " WHERE \"RECORDTIME\">=\'" + traceBackTime + "\' AND \"RULE_" + 1 + "\"=0 " +
                "AND \"MSISDN\"=" + "80086409468";
        //将上一个结果查询出来需要追溯的正常订单设置为异常。防止后续重复计算。
        try {
            if (conn == null) {
                conn = DriverManager.getConnection(DBConstant.DBURL, DBConstant.DBUSER, DBConstant.DBPASSWORD);
                conn.setAutoCommit(false);
            }
            String dateFormatSql = "alter session set nls_date_format= 'YYYY-MM-DD' ";
            Statement stmt = conn.createStatement();
            stmt.execute(dateFormatSql);
            stmt.executeUpdate(updateOrderSql);
            stmt.execute("commit");
            stmt.close();
        } catch (SQLException e) {
            System.out.println("追溯重置sql错误" + updateOrderSql);
            e.printStackTrace();
        }
    }
}
