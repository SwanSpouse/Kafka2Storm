package com.order.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DBTest {

    /**
     * 一个非常标准的连接Oracle数据库的示例代码
     */
    public static void testOracle() {
        Connection con = null;// 创建一个数据库连接
        PreparedStatement pre = null;// 创建预编译语句对象，一般都是用这个而不用Statement
        ResultSet result = null;// 创建一个结果集对象
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");// 加载Oracle驱动程序
            String url = "jdbc:oracle:" + "thin:@10.1.69.173:1521:orclbi";// 127.0.0.1是本机地址，XE是精简版Oracle的默认数据库名
            String user = "ods_iread";// 用户名,系统默认的账户名
            String password = "cx_123456";// 你安装时选设置的密码
            con = DriverManager.getConnection(url, user, password);// 获取连接
            System.out.println("连接成功！");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (result != null) result.close();
                if (pre != null) pre.close();
                if (con != null) con.close();
                System.out.println("数据库连接已关闭！");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 如何在bolt中使用Oracle的JDBC.
     */
    public static void main(String[] args) {
        DB db = new DB();
        db.getData();
    }
}
