package com.order.db;

import com.order.db.DBHelper.DBRealTimeOutputBoltHelper;
import com.order.util.StormConf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBTest {

    /**
     * 如何在bolt中使用Oracle的JDBC.
     */
    public static void main(String[] args) {
        Connection conn;
        int abnormalFeeNew = 886;
        int orderFeeNew = 886;
        double rateNew = 0.886;
        String updateSql = " UPDATE " + StormConf.realTimeOutputTable
                +" SET ODR_ABN_FEE=\'"+abnormalFeeNew+"\', ODR_FEE=\'"+orderFeeNew+"\',"+
                "ABN_RAT=\'"+rateNew+
                " \' WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?" +
                " AND SALE_PARM=? AND CONTENT_TYPE=? AND RULE_ID=?";
        try {
            conn = (new JDBCUtil()).getConnection();
            PreparedStatement prepStmt = conn.prepareStatement(updateSql);
            prepStmt.setString(1, "20150610");
            prepStmt.setString(2, "liao");
            prepStmt.setString(3, "id");
            prepStmt.setString(4, "code");
            prepStmt.setString(5, "1");
            prepStmt.setString(6, "10");

            prepStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}













