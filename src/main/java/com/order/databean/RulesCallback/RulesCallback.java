package com.order.databean.RulesCallback;

/**
 * 自定义延迟检测。并对异常数据做处理。
 *
 * Created by LiMingji on 2015/5/23.
 */
public interface RulesCallback {
    public void hanleData(String msisdnId, String sessionId, Long currentTime,
                          int realInfoFee, int channelId, int promotionId, int rules, boolean isObey);
}
