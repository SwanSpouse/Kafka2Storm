package com.order.constant;

/**
 * Created by LiMingji on 2015/5/21.
 */
public final class Constant {

    //5分钟。用于定时清空sessionInfo ipInfo terminalInfo的数据
    public final static int FIVE_MINUTES = 5 * 60;

    //规则8 session 变化阈值
    public final static int SESSION_CHANGE_THRESHOLD = 3;

    //规则9 IP变化阈值
    public final static int IP_CHANGE_THRESHOLD = 3;

    //规则10 UA信息变化阈值
    public final static int UA_CHANGE_THRESHOLD = 2;
}
