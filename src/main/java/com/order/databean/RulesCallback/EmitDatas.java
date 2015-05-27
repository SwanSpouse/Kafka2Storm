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

    /*
     *声明的数据流格式。
     *declarer.declareStream(StreamId.DATASTREAM.name(),
     *   new Fields(FName.MSISDN.name(), FName.SESSIONID.name(), FName.RECORDTIME.name(),
     *              FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PROMOTIONID.name(),
     *               FName.PROVINCEID.name()));
    * */
    @Override
    public void hanleData(String msisdnId, String sessionId, Long currentTime, int realInfoFee,
                          int channelId, int promotionId, Rules rules, String provinceId) {
        collector.emit(StreamId.DATASTREAM.name(), new Values(msisdnId, sessionId, currentTime,
                realInfoFee, channelId, promotionId, rules, provinceId));
    }
}
