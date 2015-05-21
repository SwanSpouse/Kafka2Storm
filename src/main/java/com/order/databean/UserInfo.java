package com.order.databean;

import com.order.constant.Constant;
import com.order.util.TimeCacheList;

/**
 * Created by LiMingji on 2015/5/21.
 */
public class UserInfo {

    //8、9、10规则对应的检测位
    private final static int RULE_8 = 0;
    private final static int RULE_9 = 1;
    private final static int RULE_10 = 2;

    private String msisdn;
    private long lastUpdateTime;

    //统计用户session信息。
    private TimeCacheList<String> seesionInfos = new TimeCacheList<String>(Constant.FIVE_MINUTES);

    //统计用户ip信息。
    private TimeCacheList<String> ipInfos = new TimeCacheList<String>(Constant.FIVE_MINUTES);

    //统计用户终端信息。
    private TimeCacheList<String> terminalInfos = new TimeCacheList<String>(Constant.FIVE_MINUTES);

    public UserInfo(String misidn, long currentTime, String sessionInfo, String ipInfo, String terminalInfo) {
        this.msisdn = misidn;
        this.lastUpdateTime = currentTime;
        this.seesionInfos.put(sessionInfo);
        this.ipInfos.put(ipInfo);
        this.terminalInfos.put(terminalInfo);
    }

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
            checkMarkBit[RULE_8] = false;
        } else {
            checkMarkBit[RULE_8] = true;
        }

        if (ipInfos.size() >= Constant.IP_CHANGE_THRESHOLD) {
            checkMarkBit[RULE_9] = false;
        } else {
            checkMarkBit[RULE_9] = true;
        }

        if (terminalInfos.size() >= Constant.UA_CHANGE_THRESHOLD) {
            checkMarkBit[RULE_10] = false;
        } else {
            checkMarkBit[RULE_10] = true;
        }
        return checkMarkBit;
    }
}
