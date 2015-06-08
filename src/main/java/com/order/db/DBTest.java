package com.order.db;

import com.order.constant.Rules;
import com.order.db.DBHelper.DBDataWarehouseBoltHelper;

public class DBTest {
    /**
     * 如何在bolt中使用Oracle的JDBC.
     */
    public static void main(String[] args) {
        DBDataWarehouseBoltHelper dbhelper = new DBDataWarehouseBoltHelper();
        dbhelper.upDateData("13500876661", "1", "1", "", 0, Rules.ONE.name());
    }
}
