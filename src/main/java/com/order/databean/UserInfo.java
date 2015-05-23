package com.order.databean;

import com.order.constant.Constant;
import com.order.util.TimeCacheStructures.RealTimeCacheList;

/**
 * UserInfo 用于存储用户信息并对规则8 9 10 进行检测
 *
 * Created by LiMingji on 2015/5/21.
 */
public class UserInfo {

    //规则8、9、10对应的检测位
    private final static int SESSION_CHECK_BIT = 0;
    private final static int IP_CHECK_BIT = 1;
    private final static int UA_CHECK_BIT = 2;

    //用户ID
    private String msisdn;
    private long lastUpdateTime;

    //统计用户session信息。
    private RealTimeCacheList<String> seesionInfos = new RealTimeCacheList<String>(Constant.FIVE_MINUTES);

    //统计用户ip信息。
    private RealTimeCacheList<String> ipInfos = new RealTimeCacheList<String>(Constant.FIVE_MINUTES);

    //统计用户终端信息。
    private RealTimeCacheList<String> terminalInfos = new RealTimeCacheList<String>(Constant.FIVE_MINUTES);

    //构造新用户。
    public UserInfo(String misidn, long currentTime, String sessionInfo, String ipInfo, String terminalInfo) {
        this.msisdn = misidn;
        this.lastUpdateTime = currentTime;
        this.seesionInfos.put(sessionInfo);
        this.ipInfos.put(ipInfo);
        this.terminalInfos.put(terminalInfo);
    }

    //更新已存在用户的信息
    public void upDateUserInfo(long currentTime, String sessionInfo, String ipInfo, String terminalInfo) {
        this.lastUpdateTime = currentTime;
        if (sessionInfo != null && !sessionInfo.trim().equals("")) {
            this.seesionInfos.put(sessionInfo);
        }
        if (ipInfo != null && !ipInfo.trim().equals("")) {
            this.ipInfos.put(ipInfo);
        }
        if (terminalInfo != null && !terminalInfo.trim().equals("")) {
            this.terminalInfos.put(terminalInfo);
        }
    }

    //检测规则8、9、10是否符合规则。如果符合规则，则返回true，反之返回false
    public boolean[] isObeyRules() {
        boolean[] checkMarkBit = new boolean[3];
        if (seesionInfos.size() >= Constant.SESSION_CHANGE_THRESHOLD) {
            checkMarkBit[SESSION_CHECK_BIT] = false;
        } else {
            checkMarkBit[SESSION_CHECK_BIT] = true;
        }

        if (ipInfos.size() >= Constant.IP_CHANGE_THRESHOLD) {
            checkMarkBit[IP_CHECK_BIT] = false;
        } else {
            checkMarkBit[IP_CHECK_BIT] = true;
        }

        if (terminalInfos.size() >= Constant.UA_CHANGE_THRESHOLD) {
            checkMarkBit[UA_CHECK_BIT] = false;
        } else {
            checkMarkBit[UA_CHECK_BIT] = true;
        }
        return checkMarkBit;
    }
}
