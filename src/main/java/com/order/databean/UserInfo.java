package com.order.databean;

import com.order.constant.Constant;
import com.order.databean.TimeCacheStructures.CachedList;

import java.io.Serializable;

/**
 * UserInfo 用于存储用户信息并对规则9 10 11 进行检测
 *
 * Created by LiMingji on 2015/5/21.
 */
public class UserInfo implements Serializable{

    private static final long serialVersionUID = 1L;
    //规则9、10、11对应的检测位
    public final static int SESSION_CHECK_BIT = 0;
    public final static int IP_CHECK_BIT = 1;
    public final static int UA_CHECK_BIT = 2;

    //用户ID
    private String msisdnId;
    private long lastUpdateTime;

    /**
     * 这里把过期的时间配置成1小时10分钟是因为订单缓存了5分钟。为防止删除1小时过期数据删除了部分订单前一小时数据。
     * 把过期时间配置成1小时10分钟。取size的时候是根据时间窗口取的。和这个过期时间没有影响。
     */

    //统计用户session信息。
    private CachedList<String> sessionInfos = new CachedList<String>(Constant.ONE_HOUR + Constant.FIVE_MINUTES * 2);

    //统计用户ip信息。
    private CachedList<String> ipInfos = new CachedList<String>(Constant.ONE_HOUR + Constant.FIVE_MINUTES * 2);

    //统计用户终端信息。
    private CachedList<String> terminalInfos = new CachedList<String>(Constant.ONE_HOUR + Constant.FIVE_MINUTES * 2);

    @Override
    public String toString() {
        String context = "";
        context += "session信息: " + sessionInfos.toString() + "\n";
        context += "ip信息 : " + ipInfos.toString() + "\n";
        context += "ua信息 : " + terminalInfos.toString() + "\n";
        return context;
    }

    //构造新用户。
    public UserInfo(String msisdnId, long currentTime, String sessionInfo, String ipInfo, String terminalInfo) {
        this.msisdnId = msisdnId;
        this.lastUpdateTime = currentTime;
        if (sessionInfo != null && !sessionInfo.equals("") && !sessionInfo.equals("NULL")) {
            this.sessionInfos.put(sessionInfo.trim(), lastUpdateTime);
        }
        if (ipInfo != null && !ipInfo.equals("")) {
            this.ipInfos.put(ipInfo.trim(), lastUpdateTime);
        }
        if (terminalInfo != null && !terminalInfo.equals("")) {
            this.terminalInfos.put(terminalInfo.trim(), lastUpdateTime);
        }
    }

    //更新已存在用户的信息
    public void upDateUserInfo(long currentTime, String sessionInfo, String ipInfo, String terminalInfo) {
        this.lastUpdateTime = currentTime;
        if (sessionInfo != null && !sessionInfo.equals("") && !sessionInfo.equals("NULL")) {
            this.sessionInfos.put(sessionInfo.trim(), lastUpdateTime);
        }
        if (ipInfo != null && !ipInfo.trim().equals("")) {
            this.ipInfos.put(ipInfo.trim(), lastUpdateTime);
        }
        if (terminalInfo != null && !terminalInfo.trim().equals("")) {
            this.terminalInfos.put(terminalInfo.trim(), lastUpdateTime);
        }
    }

    /**
     * 清理UserInfo内的过期数据
     * 如果清理后为空。则此UserInfo可以被清理。
     */
    public boolean clear() {
        //size()自带清理功能
        return sessionInfos.size(lastUpdateTime, -1) == 0 &&
                terminalInfos.size(lastUpdateTime, -1) == 0 &&
                ipInfos.size(lastUpdateTime, -1) == 0;
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
        if (sessionInfos.size(lastUpdateTime, Constant.ONE_HOUR) >= 3) {
            checkMarkBit[SESSION_CHECK_BIT] = false;
        } else {
            checkMarkBit[SESSION_CHECK_BIT] = true;
        }

        if (ipInfos.size(lastUpdateTime, Constant.ONE_HOUR) >= 3) {
            checkMarkBit[IP_CHECK_BIT] = false;
        } else {
            checkMarkBit[IP_CHECK_BIT] = true;
        }

        if (terminalInfos.size(lastUpdateTime, Constant.ONE_HOUR) >= 2) {
            checkMarkBit[UA_CHECK_BIT] = false;
        } else {
            checkMarkBit[UA_CHECK_BIT] = true;
        }
        return checkMarkBit;
    }
}
