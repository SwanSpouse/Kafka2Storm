package com.order.databean;

import com.order.constant.Constant;
import com.order.databean.TimeCacheStructures.RealTimeCacheList;
import com.order.util.LogUtil;

/**
 * UserInfo 用于存储用户信息并对规则8 9 10 进行检测
 *
 * Created by LiMingji on 2015/5/21.
 */
public class UserInfo {

    //规则9、10、11对应的检测位
    public final static int SESSION_CHECK_BIT = 0;
    public final static int IP_CHECK_BIT = 1;
    public final static int UA_CHECK_BIT = 2;

    //是否为异常用户
    private boolean isNormalUser = true;

    //用户ID
    private String msisdnId;
    private long lastUpdateTime;

    //统计用户session信息。
    private RealTimeCacheList<String> seesionInfos = new RealTimeCacheList<String>(Constant.FIVE_MINUTES);

    //统计用户ip信息。
    private RealTimeCacheList<String> ipInfos = new RealTimeCacheList<String>(Constant.FIVE_MINUTES);

    //统计用户终端信息。
    private RealTimeCacheList<String> terminalInfos = new RealTimeCacheList<String>(Constant.FIVE_MINUTES);

    @Override
    public String toString() {
        String context = "";
        context += "session信息: " + seesionInfos.toString() + "\n";
        context += "ip信息 : " + ipInfos.toString() + "\n";
        context += "ua信息 : " + terminalInfos.toString() + "\n";
        return context;
    }

    //构造新用户。
    public UserInfo(String msisdnId, long currentTime, String sessionInfo, String ipInfo, String terminalInfo) {
        this.msisdnId = msisdnId;
        this.lastUpdateTime = currentTime;
        this.seesionInfos.put(sessionInfo, lastUpdateTime);
        this.ipInfos.put(ipInfo, lastUpdateTime);
        this.terminalInfos.put(terminalInfo, lastUpdateTime);
    }

    //更新已存在用户的信息
    public void upDateUserInfo(long currentTime, String sessionInfo, String ipInfo, String terminalInfo) {
        this.lastUpdateTime = currentTime;
        if (sessionInfo != null && !sessionInfo.trim().equals("")) {
            this.seesionInfos.put(sessionInfo, lastUpdateTime);
        }
        if (ipInfo != null && !ipInfo.trim().equals("")) {
            this.ipInfos.put(ipInfo, lastUpdateTime);
        }
        if (terminalInfo != null && !terminalInfo.trim().equals("")) {
            this.terminalInfos.put(terminalInfo, lastUpdateTime);
        }
    }

    /**
     * 检测规则9、10、11是否符合规则。如果符合规则，则返回true，反之返回false
     * 规则9：一个用户一小时内订购session>=3。
     * 规则10：一小时内用户订购IP地址变化 变化次数>=3次。
     * 规则11：一小时内用户订购UA信息发生变化次数>=2次。
     * @return
     */
    public boolean[] isObeyRules() {
        boolean[] checkMarkBit = new boolean[3];
        if (seesionInfos.size() >= Constant.SESSION_CHANGE_THRESHOLD) {
            LogUtil.printLog(this,"rule9", false);
            checkMarkBit[SESSION_CHECK_BIT] = false;
        } else {
            LogUtil.printLog(this,"rule9", true);
            checkMarkBit[SESSION_CHECK_BIT] = true;
        }

        if (ipInfos.size() >= Constant.IP_CHANGE_THRESHOLD) {
            LogUtil.printLog(this,"rule10", false);
            checkMarkBit[IP_CHECK_BIT] = false;
        } else {
            LogUtil.printLog(this,"rule10", true);
            checkMarkBit[IP_CHECK_BIT] = true;
        }

        if (terminalInfos.size() >= Constant.UA_CHANGE_THRESHOLD) {
            LogUtil.printLog(this,"rule11", false);
            checkMarkBit[UA_CHECK_BIT] = false;
        } else {
            LogUtil.printLog(this, "rule11", true);
            checkMarkBit[UA_CHECK_BIT] = true;
        }
        return checkMarkBit;
    }

    //将用户设置为异常用户
    public void setAbnormalUser(boolean isNormalUser) {
        this.isNormalUser = isNormalUser;
    }

    //判断用户是否为异常用户
    public boolean isNormalUser() {
        return isNormalUser;
    }

    //三个容器内均无有效数据且不是异常用户。则用户超时。
    public boolean isTimeout() {
        return seesionInfos.size() == 0 && ipInfos.size() == 0
                && terminalInfos.size() == 0 && isNormalUser == true;
    }
}
