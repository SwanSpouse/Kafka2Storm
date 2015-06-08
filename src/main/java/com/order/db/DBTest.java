package com.order.db;

import com.order.db.DBHelper.DBDataWarehouseBoltHelper;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DBTest {
    /**
     * 如何在bolt中使用Oracle的JDBC.
     */
    public static void main(String[] args) {
        Connection conn;
        try {
            conn = (new JDBCUtil()).getConnection();
            Statement stmt = conn.createStatement();
            String checkAbnormalOrderSql =
                    "SELECT \"msisdn\",\"realfee\" FROM " + DBDataWarehouseBoltHelper.TABLE_NAME +
                            " WHERE \"record_time\"<="+"20150608205535"+" AND \"rule_"+"1"+"\"=0 " +
                            "AND \"msisdn\"="+"18001214581";
            String updateOrderSql = "UPDATE "+DBDataWarehouseBoltHelper.TABLE_NAME+" SET \"rule_"+"1\""+"=1"+
                    " WHERE \"record_time\">="+"20150608205535"+" AND \"rule_"+"1"+"\"=0 " +
                    "AND \"msisdn\"="+"18001214581";

            stmt.executeUpdate(updateOrderSql);
//            ResultSet resultSet = stmt.executeQuery(updateOrderSql);
//            while (resultSet.next()) {
//                String msisdn = resultSet.getString("msisdn");
//                int infoFee = resultSet.getInt("realfee");
//                System.out.println("msisdn: " + msisdn + " infoFee:" + infoFee);
//            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
