package com.order.databean.RulesCallback;


import com.order.constant.Rules;

/**
 * 自定义延迟检测。并对异常数据做处理。
 *
 * Created by LiMingji on 2015/5/23.
 */

public interface RulesCallback {

    /**
     * @param msisdnId    用户msisdnId
     * @param sessionId   订单或浏览记录所在的SeesionId
     * @param currentTime 默认为订单产生时间，如果此时间为空，则用消息到达系统的时间来代替。
     * @param realInfoFee 真实信息费
     * @param channelId   渠道Id
     * @param promotionId 营销参数
     * @param rules       对应处理的规则。
     */
    public void hanleData(String msisdnId, String sessionId, Long currentTime,
                          int realInfoFee, int channelId, int promotionId, Rules rules);
}
