package com.order.constant;

import com.order.util.StormConf;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Created by LiMingji on 2015/5/21.
 */
public final class Constant {
    private static Logger log = Logger.getLogger(Constant.class);

    private static final String filePath = "rulesParams.properties";
    private static Map<String, String> conf = StormConf.readPropery(filePath);

    //OrderType中对应的信息
    public static final int ORDERTYPE_FREE = 0;
    public static final int ORDERTYPE_BOOK = 1;
    public static final int ORDERTYPE_CHAPTER = 2;
    public static final int ORDERTYPE_WORD = 3;
    public static final int ORDERTYPE_PROMOTION = 4;
    public static final int ORDERTYPE_VOLUME = 5;

    //3分钟。用于统计规则5中的订购次数
    public final static int THREEO_MINUTES = 3 * 60;
    //5分钟。用于定时清空sessionInfo ipInfo terminalInfo的数据
    public final static int FIVE_MINUTES = 5 * 60;
    //65分钟，用于定时清空bookreadpv的数据
    public final static int SIXTYFIVE_MINUTES = 65 * 60;

    //规则1、2pv 变化阈值
    public final static int READPV_THREASHOLD = Integer.parseInt(conf.get("READPV_THREASHOLD"));
    //规则3 扣费二级渠道 变化阈值
    public final static int CHANNEL_THRESHOLD = Integer.parseInt(conf.get("CHANNEL_THRESHOLD"));
    //规则4 日渠道ID按本扣费 变化阈值
    public final static int ORDER_FEE_THRESHOLD = Integer.parseInt(conf.get("ORDER_FEE_THRESHOLD"));
    //规则5 包月订购次数 变化阈值
    public final static int ORDER_BY_MONTH_THRESHOLD = Integer.parseInt(conf.get("ORDER_BY_MONTH_THRESHOLD"));
    //规则6 图书订购或批量订购 变化阈值
    public final static int ORDER_TIMES_THRESHOLD = Integer.parseInt(conf.get("ORDER_TIMES_THRESHOLD"));
    //规则7 连载图书订购章数 变化阈值
    public final static int ORDER_CHAPTER_TIMES_THRESHOLD = Integer.parseInt(conf.get("ORDER_CHAPTER_TIMES_THRESHOLD"));
    //规则8 session 变化阈值
    public final static int SESSION_CHANGE_THRESHOLD = Integer.parseInt(conf.get("SESSION_CHANGE_THRESHOLD"));
    //规则9 IP变化阈值
    public final static int IP_CHANGE_THRESHOLD = Integer.parseInt(conf.get("IP_CHANGE_THRESHOLD"));
    //规则10 UA信息变化阈值
    public final static int UA_CHANGE_THRESHOLD = Integer.parseInt(conf.get("UA_CHANGE_THRESHOLD"));

    //测试
    public static void main(String[] args) {
        conf = StormConf.readPropery(filePath);
        System.out.println(conf);
        System.out.println(SESSION_CHANGE_THRESHOLD);
        System.out.println(IP_CHANGE_THRESHOLD);
        System.out.println(UA_CHANGE_THRESHOLD);

    }
}
