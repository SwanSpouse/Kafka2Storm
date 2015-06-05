package com.order.databean.RulesCallback;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Values;
import com.order.constant.Rules;
import com.order.util.StreamId;

/**
 * 发射订单数据的函数
 *
 * Created by LiMingji on 2015/5/27.
 */
public class EmitDatas implements RulesCallback {

    BasicOutputCollector collector = null;

    public EmitDatas(BasicOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void hanleData(String msisdnId, String sessionId, Long currentTime, int realInfoFee,
                          String channelId, String productId, String rules,
                          int provinceId, int orderType, String bookId) {
        collector.emit(StreamId.DATASTREAM.name(), new Values(msisdnId, sessionId, currentTime,
                realInfoFee, channelId, productId, rules, provinceId, orderType, bookId));
    }
}
