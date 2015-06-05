package com.order.db;

import com.order.constant.Rules;
import com.order.db.DBHelper.DBRealTimeOutputBoltHelper;

public class DBTest {
    /**
     * 如何在bolt中使用Oracle的JDBC.
     */
    public static void main(String[] args) {
        DBRealTimeOutputBoltHelper dbhelper = new DBRealTimeOutputBoltHelper();
        dbhelper.upDateData("13500876661", "1", "1", "", 0, Rules.ONE.name());
    }
}
