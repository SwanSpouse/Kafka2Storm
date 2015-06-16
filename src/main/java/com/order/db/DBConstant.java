package com.order.db;

public class DBConstant {
    //北京测试
	public static final String DBDRIVER = "oracle.jdbc.driver.OracleDriver";
//    public static final String DBURL = "jdbc:oracle:thin:@10.1.70.31:1521:ebrac1";
    public static final String DBURL = "jdbc:oracle:thin:@10.1.69.173:1521:orclbi";
    public static final String DBUSER = "ods_iread";
    public static final String DBPASSWORD = "cx_123456";

    //杭州现网
    /*
    public static final String DBURL = "jdbc:oracle:thin:@192.168.5.146:1521:order";
    public static final String DBUSER = "ods_iread";
    public static final String DBPASSWORD = "ods123qaz";
    */

    //    public static final String DBURL = "jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.3.183)(PORT = 1521))(ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.3.185)(PORT = 1521))(LOAD_BALANCE = yes)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = service_ora)))";
    // the lab's oracle site and password is as follows.
	/*
	 * public static final String DBDRIVER = "oracle.jdbc.driver.OracleDriver";
	 * public static final String DBURL =
	 * "jdbc:oracle:thin:@10.1.69.173:1521:ORCLBI"; public static final String
	 * DBUSER = "huangq"; public static final String DBPASSWORD = "123456";
	 */
}