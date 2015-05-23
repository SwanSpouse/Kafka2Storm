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

    //5分钟。用于定时清空sessionInfo ipInfo terminalInfo的数据
    public final static int FIVE_MINUTES = 5 * 60;

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
